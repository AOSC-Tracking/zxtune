package app.zxtune.playback.service

import android.content.Context
import android.net.Uri
import app.zxtune.Logger
import app.zxtune.playback.PlayableItem
import app.zxtune.playback.Queue
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
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi

internal typealias Storage = ShuffledList<Entry>
internal typealias Cursor = ShuffledList.Cursor<Entry>

@OptIn(ExperimentalAtomicApi::class)
internal class PlaylistQueue(ctx: Context, private val loader: Loader) : Queue {
    private val scope = CoroutineScope(Dispatchers.IO)
    private val client = ProviderClient.create(ctx)
    private val currentPosition = AtomicReference<Cursor?>(null)
    private val currentStream = MutableSharedFlow<ReceiveChannel<PlayableItem>>(
        replay = 1, extraBufferCapacity = 0, onBufferOverflow = BufferOverflow.DROP_OLDEST
    )

    private val state = object {
        private val lock = Mutex()
        private val content = Storage { l, r ->
            l.id == r.id
        }
        private var contentChanged = true
        var shuffled by content::shuffled

        init {
            client.observeContent().onEach {
                LOG.d { "Invalidated content" }
                contentChanged = true
            }.launchIn(scope)
        }

        private suspend fun <T> withContent(block: Storage.() -> T): T = lock.withLock {
            synchronizeContent()
            return content.block()
        }

        private suspend fun synchronizeContent() {
            if (contentChanged) {
                client.queryContent()?.let {
                    LOG.d { "Updated content ${it.size}" }
                    content.update(it)
                    contentChanged = false
                }
            }
        }

        suspend fun navigate(id: Long) = withContent {
            find { it.id == id }
        }

        suspend fun advance(cursor: Cursor, delta: Int) = withContent {
            advanceCursor(cursor, delta)
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
        LOG.d { "Start sequence from ${first.data.id}" }
        var current = first
        while (true) {
            loader.load(current.data)?.let {
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