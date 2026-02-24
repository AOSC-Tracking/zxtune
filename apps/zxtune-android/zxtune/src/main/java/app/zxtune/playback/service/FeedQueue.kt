package app.zxtune.playback.service

import android.net.Uri
import app.zxtune.Logger
import app.zxtune.core.Identifier
import app.zxtune.fs.provider.VfsProviderClient
import app.zxtune.playback.PlayableItem
import app.zxtune.playback.Queue
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
internal class FeedQueue(client: VfsProviderClient, val context: Uri, loader: Loader) : Queue {
    private val scope = CoroutineScope(Dispatchers.IO + CoroutineName("FeedQueue"))
    private val currentPosition = AtomicReference<Cursor?>(null)
    private val currentStream = MutableSharedFlow<ReceiveChannel<PlayableItem>>(
        replay = 1, extraBufferCapacity = 0, onBufferOverflow = BufferOverflow.DROP_OLDEST
    )

    @JvmInline
    private value class Cursor(val value: Int)

    private val state = object {
        private val history = ArrayList<Identifier>()

        fun reset(initial: Uri?) {
            history.clear()
            initial?.let {
                history.add(Identifier(it))
            }
        }

        @OptIn(ExperimentalCoroutinesApi::class)
        fun channelFrom(start: Cursor) = scope.produce {
            var cur = start.value
            while (cur < history.size) {
                val entry = history[cur]
                loader.load(entry)?.let {
                    LOG.d { "History item $entry}" }
                    send(it)
                    currentPosition.store(Cursor(cur))
                }
                ++cur
            }
            client.feed(context).collect { file ->
                loader.detect(file.uri).collect {
                    val index = history.size
                    history.add(it.dataId)
                    if (index >= cur) {
                        LOG.d { "Feed item ${it.dataId}" }
                        send(it)
                        currentPosition.store(Cursor(index))
                    } else {
                        LOG.d { "Skip archived ${it.dataId}" }
                        it.module.release()
                    }
                }
            }
        }

        fun nextOf(cursor: Cursor) = Cursor(cursor.value + 1)

        fun prevOf(cursor: Cursor) = if (cursor.value > 0) {
            Cursor(cursor.value - 1)
        } else null
    }

    override val items: Flow<ReceiveChannel<PlayableItem>>
        get() = currentStream

    override suspend fun activate(uri: Uri) {
        state.reset(uri.takeIf { it != context })
        activate(Cursor(0))
    }

    private suspend fun activate(pos: Cursor) {
        LOG.d { "Activate $pos" }
        currentPosition.store(null)
        currentStream.emit(state.channelFrom(pos))
    }

    override suspend fun next() = currentPosition.load()?.let { current ->
        activate(state.nextOf(current))
    } ?: Unit

    override suspend fun prev() = currentPosition.load()?.let { current ->
        state.prevOf(current)?.let {
            activate(it)
        }
    } ?: Unit

    override var shuffled
        get() = false
        set(_) {}

    override fun release() = Unit

    private companion object {
        private val LOG = Logger(FeedQueue::class.java.name)
    }
}
