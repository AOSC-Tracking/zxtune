package app.zxtune.playback.service

import android.content.Context
import android.net.Uri
import app.zxtune.Logger
import app.zxtune.fs.provider.Schema
import app.zxtune.fs.provider.VfsProviderClient
import app.zxtune.playback.EmptyQueue
import app.zxtune.playback.Queue
import app.zxtune.playlist.PlaylistQuery
import app.zxtune.utils.ifNotNulls
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.flatMapLatest

class DispatcherQueue(private val ctx: Context) : Queue {
    private val loader = Loader()
    private val _playlistQueue by lazy {
        PlaylistQueue(ctx, loader)
    }
    private val _vfsProvider by lazy {
        VfsProviderClient(ctx)
    }
    private var _shuffledCache = false
    private val _currentQueue = MutableStateFlow<Queue>(EmptyQueue)
    private var currentQueue
        get() = _currentQueue.value
        set(value) {
            value.shuffled = _shuffledCache
            _currentQueue.value = value
        }

    override suspend fun activate(uri: Uri) = findQueue(uri)?.let {
        LOG.d { "Activate queue $it for $uri" }
        currentQueue = it
        it.activate(uri)
    } ?: Unit

    override suspend fun next() = currentQueue.next()

    override suspend fun prev() = currentQueue.prev()

    @OptIn(ExperimentalCoroutinesApi::class)
    override val items
        get() = _currentQueue.flatMapLatest { it.items }

    override var shuffled
        get() = currentQueue.shuffled
        set(value) {
            _shuffledCache = value
            currentQueue.shuffled = value
        }

    override fun release() = currentQueue.release()

    private suspend fun findQueue(uri: Uri): Queue? = if (PlaylistQuery.isPlaylistUri(uri)) {
        _playlistQueue
    } else {
        var played: Uri? = null
        var parent: Uri? = null
        var dirWithFeed: Uri? = null
        _vfsProvider.resolve(uri, object : VfsProviderClient.ListingCallback {
            override fun onProgress(status: Schema.Status.Progress) = Unit
            override fun onDir(dir: Schema.Content.Dir) {
                parent = parent ?: dir.uri
                dirWithFeed = dirWithFeed ?: if (dir.hasFeed) dir.uri else null
            }

            override fun onFile(file: Schema.Content.File) {
                if (parent == null && dirWithFeed == null) {
                    played = played ?: file.uri
                }
            }
        })
        dirWithFeed?.let { feedUri ->
            if (feedUri == (currentQueue as? FeedQueue)?.context) {
                LOG.d { "Reuse feed queue for $parent" }
                currentQueue
            } else {
                FeedQueue(_vfsProvider, feedUri, loader)
            }
        } ?: ifNotNulls(played, parent) { _, parent ->
            if (parent == (currentQueue as? FolderQueue)?.context) {
                LOG.d { "Reuse folder queue for $parent" }
                currentQueue
            } else {
                FolderQueue(_vfsProvider, parent, loader)
            }
        }
    }

    private companion object {
        private val LOG = Logger(DispatcherQueue::class.java.name)
    }
}