package app.zxtune.playback.service

import android.content.Context
import android.net.Uri
import app.zxtune.Logger
import app.zxtune.playback.PlayableItem
import app.zxtune.playback.Queue
import app.zxtune.playlist.PlaylistContent
import app.zxtune.playlist.PlaylistQuery
import app.zxtune.playlist.ProviderClient
import app.zxtune.ui.playlist.Entry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
internal class PlaylistQueue(ctx: Context, private val loader: Loader) : Queue {
    private val scope = CoroutineScope(Dispatchers.IO)
    private val client = ProviderClient.Companion.create(ctx)
    private val currentPosition = AtomicReference<Cursor?>(null)
    private val currentStream = MutableSharedFlow<ReceiveChannel<PlayableItem>>(
        replay = 1, extraBufferCapacity = 0, onBufferOverflow = BufferOverflow.DROP_OLDEST
    )

    private class Cursor(
        var version: Int, var index: Int, var entry: Entry
    ) {
        fun synchronizeIndex(state: PlaylistContent) {
            index = state.indexOfFirst { it.id == entry.id }
        }
    }

    private val state = object {
        init {
            client.observeContent().onEach { contentsChanged() }.launchIn(scope)
        }

        // TODO: think about only ids storing with metadata requesting
        private var _contentCache: PlaylistContent? = null
            set(value) {
                field = value
                if (value != null) {
                    ++_contentVersion
                }
            }
        private var _contentVersion = 0
        var shuffled = false
            set(value) {
                _contentCache?.let {
                    if (field != value) {
                        if (value) {
                            it.shuffle()
                        } else {
                            contentsChanged()
                        }
                    }
                }
                field = value
            }

        private suspend fun getContent(cursor: Cursor) = getContent()?.also {
            if (cursor.version != _contentVersion) {
                cursor.synchronizeIndex(it)
                cursor.version = _contentVersion
            }
        }

        private suspend fun getContent() = _contentCache ?: client.queryContent()?.also { state ->
            if (shuffled) {
                state.shuffle()
            }
            LOG.d { "Updated content ${state.size}/$shuffled" }
            _contentCache = state
        }

        private fun contentsChanged() {
            LOG.d { "Invalidated content" }
            _contentCache = null
        }

        private fun makeCursor(idx: Int) = with(requireNotNull(_contentCache)) {
            if (idx in 0..<size) {
                Cursor(_contentVersion, idx, get(idx))
            } else {
                null
            }
        }

        suspend fun navigate(id: Long) = getContent()?.run {
            makeCursor(indexOfFirst { it.id == id })
        }

        suspend fun advance(cursor: Cursor, delta : Int) = getContent(cursor)?.run {
            makeCursor(cursor.index + delta)
        }
    }

    override val items: Flow<ReceiveChannel<PlayableItem>>
        get() = currentStream

    override suspend fun activate(uri: Uri) {
        val id = requireNotNull(PlaylistQuery.idOf(uri))
        state.navigate(id)?.let {
            activate(it)
        }
    }

    private suspend fun activate(cursor: Cursor) {
        LOG.d { "Activate $cursor" }
        currentPosition.store(null)
        currentStream.emit(channelFrom(cursor))
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private fun channelFrom(first: Cursor) = scope.produce {
        LOG.d { "Start sequence from ${first.entry.id}" }
        var current = first
        while (true) {
            loader.load(current.entry)?.let {
                send(it)
                currentPosition.store(current)
            }
            current = state.advance(current, +1) ?: break
        }
    }

    override suspend fun next() = advance(+1)

    override suspend fun prev() = advance(-1)

    private suspend fun advance(delta: Int) = currentPosition.load()?.let { current ->
        state.advance(current, delta)?.let {
            activate(it)
        }
    } ?: Unit

    override var shuffled by state::shuffled

    override fun release() = scope.cancel()

    private companion object {
        private val LOG = Logger(PlaylistQueue::class.java.name)
    }
}