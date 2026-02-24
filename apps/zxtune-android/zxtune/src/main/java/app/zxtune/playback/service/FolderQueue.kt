package app.zxtune.playback.service

import android.net.Uri
import app.zxtune.Logger
import app.zxtune.core.Identifier
import app.zxtune.fs.provider.Schema
import app.zxtune.fs.provider.VfsProviderClient
import app.zxtune.playback.PlayableItem
import app.zxtune.playback.Queue
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
internal class FolderQueue(
    client: VfsProviderClient, val context: Uri, private val loader: Loader
) : Queue {
    private val scope = CoroutineScope(Dispatchers.IO + CoroutineName("FolderQueue"))
    private val currentPosition = AtomicReference<Cursor?>(null)
    private val currentStream = MutableSharedFlow<ReceiveChannel<PlayableItem>>(
        replay = 1, extraBufferCapacity = 0, onBufferOverflow = BufferOverflow.DROP_OLDEST
    )

    private sealed interface Entry {
        object Ignored : Entry

        class Unknown(val uri: Uri, val resolvedTo: Deferred<Entry?>) : Entry

        @JvmInline
        value class Track(val id: Identifier) : Entry

        // TODO: think about playback history pre-fill from indexed archives.
        // Use SearchEngine with empty query to get all the files in the same order they
        // arrive while detection
        class Archive(val scanner: ArchiveScanner) : Entry
    }

    private inner class ArchiveScanner(private val uri: Uri) {
        val history = ArrayList<Identifier>()
        private val items = Channel<PlayableItem>(onUndeliveredElement = {
            it.module.release()
        }).apply {
            scope.launch {
                loader.detect(uri).collect(this@apply::send)
                close()
            }
        }

        @OptIn(DelicateCoroutinesApi::class)
        val detectionFinished
            get() = items.isClosedForReceive

        fun has(idx: Int) = idx >= 0 && (idx < history.size || !detectionFinished) // highly likely

        suspend fun total(): Int {
            if (!detectionFinished) {
                consumeFrom(history.size) { _, item ->
                    item.module.release()
                }
            }
            return history.size
        }

        // TODO: share from FeedQueue
        suspend fun consumeFrom(start: Int, block: suspend (Int, PlayableItem) -> Unit) {
            if (start == 0 && history.isNotEmpty()) {
                return loader.detect(uri).collectIndexed(block)
            }
            for (cur in start..<history.size) {
                loader.load(history[cur])?.let { item ->
                    block(cur, item)
                }
            }
            while (true) {
                items.receiveCatching().getOrNull()?.let {
                    val index = history.size
                    history.add(it.dataId)
                    if (index >= start) {
                        block(index, it)
                    } else {
                        it.module.release()
                    }
                } ?: break
            }
        }
    }

    private class Cursor(val fileIdx: Int, val trackIdx: Int = 0) {
        fun forTrack(idx: Int) = Cursor(fileIdx, idx)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private val state = object {
        private val lock = Mutex()
        private lateinit var _content: ArrayList<Entry>

        private suspend fun createContent() = ArrayList<Entry>().apply {
            client.list(context, object : VfsProviderClient.ListingCallback {
                override fun onProgress(status: Schema.Status.Progress) = Unit
                override fun onDir(dir: Schema.Content.Dir) = Unit
                override fun onFile(file: Schema.Content.File) =
                    file.toEntry().takeIf { it !== Entry.Ignored }?.let {
                        add(it)
                        Unit
                    } ?: Unit
            })
        }

        private suspend fun <T> withContent(block: ArrayList<Entry>.() -> T): T = lock.withLock {
            if (!this::_content.isInitialized) {
                _content = createContent()
            }
            return _content.block()
        }

        private fun Schema.Content.File.toEntry() = when (type) {
            Schema.Content.File.Type.UNKNOWN -> Entry.Unknown(uri, indexation(uri))
            Schema.Content.File.Type.UNSUPPORTED -> Entry.Ignored
            Schema.Content.File.Type.TRACK -> Entry.Track(Identifier(uri))
            Schema.Content.File.Type.REMOTE -> Entry.Unknown(uri, indexation(uri))
            Schema.Content.File.Type.ARCHIVE -> Entry.Archive(ArchiveScanner(uri))
        }

        suspend fun navigate(uri: Uri) = withContent {
            // Known limitation: when playback is stopped and resumed while playing some track
            // inside archive, next session's context will be that track parent dir, not an
            // original directory.
            indexOfFirst { uri == (it as? Entry.Track)?.id?.fullLocation }.takeIf { it != -1 }
                ?.let {
                    Cursor(it)
                }
        }

        @OptIn(ExperimentalCoroutinesApi::class)
        fun channelFrom(start: Cursor) = scope.produce {
            var cur = start
            while (true) {
                val toPlay = fetch(cur) ?: break
                playEntry(cur, toPlay)
                cur = Cursor(cur.fileIdx + 1)
            }
        }

        private suspend fun fetch(cur: Cursor) = withContent {
            getOrNull(cur.fileIdx)
        }

        private suspend fun resolveUnknown(cur: Cursor, entry: Entry) = withContent {
            val old = get(cur.fileIdx)
            if (old is Entry.Unknown) {
                LOG.d { "Resolve unknown at ${old.uri} to $entry" }
                set(cur.fileIdx, entry)
            }
        }

        private fun indexation(uri: Uri): Deferred<Entry?> =
            scope.async(start = CoroutineStart.LAZY) {
                LOG.d { "Indexing $uri" }
                var result: Schema.Content.File? = null
                client.resolve(uri, object : VfsProviderClient.ListingCallback {
                    override fun onProgress(status: Schema.Status.Progress) {
                        LOG.d { "Indexing $uri: $status" }
                    }

                    override fun onDir(dir: Schema.Content.Dir) = TODO("Should not be called")

                    override fun onFile(file: Schema.Content.File) {
                        result = result ?: file
                    }
                })
                result?.toEntry()
            }

        private suspend fun ProducerScope<PlayableItem>.playEntry(cur: Cursor, entry: Entry): Unit =
            when (entry) {
                is Entry.Track -> playTrack(cur, entry)
                is Entry.Archive -> playArchive(cur, entry)
                is Entry.Unknown -> if (entry.resolvedTo.isCompleted) {
                    entry.resolvedTo.await()?.takeUnless { it is Entry.Unknown }?.let {
                        resolveUnknown(cur, it)
                        playEntry(cur, it)
                    } ?: resolveUnknown(cur, Entry.Ignored)
                } else {
                    playUri(cur, entry.uri)
                }

                else -> Unit
            }

        private suspend fun ProducerScope<PlayableItem>.playTrack(cur: Cursor, entry: Entry.Track) =
            loader.load(entry.id)?.let {
                playItem(cur, it)
            } ?: Unit

        private suspend fun ProducerScope<PlayableItem>.playArchive(
            cur: Cursor, entry: Entry.Archive
        ) = entry.scanner.consumeFrom(cur.trackIdx) { idx, item ->
            playItem(cur.forTrack(idx), item)
        }

        private suspend fun ProducerScope<PlayableItem>.playUri(start: Cursor, uri: Uri) =
            with(ArchiveScanner(uri)) {
                LOG.d { "Play at $uri starting from $start" }
                consumeFrom(start.trackIdx) { idx, item ->
                    playItem(start.forTrack(idx), item)
                }
                require(detectionFinished) { "Should be called on fully indexed archive" }
                when (history.size) {
                    0 -> Entry.Ignored
                    1 -> Entry.Track(history.first())
                    else -> Entry.Archive(this@with)
                }.let {
                    resolveUnknown(start, it)
                }
            }

        private suspend fun ProducerScope<PlayableItem>.playItem(
            cursor: Cursor, item: PlayableItem
        ) {
            send(item)
            currentPosition.store(cursor)
        }

        suspend fun advance(start: Cursor, delta: Int): Cursor? {
            var cur = start
            while (true) {
                val initialStep = cur == start
                when (val element = fetch(cur)) {
                    null -> return null
                    is Entry.Track -> if (!initialStep) {
                        return cur
                    }

                    is Entry.Ignored -> Unit
                    is Entry.Unknown -> {
                        element.resolvedTo.await()
                            ?.takeUnless { it is Entry.Unknown }?.let {
                                resolveUnknown(cur, it)
                            } ?: resolveUnknown(cur, Entry.Ignored)
                        continue
                    }

                    is Entry.Archive -> {
                        val next = when {
                            initialStep -> cur.forTrack(cur.trackIdx + delta)
                            delta < 0 -> cur.forTrack(element.scanner.total() + delta)
                            else -> cur.forTrack(cur.trackIdx + delta)
                        }
                        next.takeIf { element.scanner.has(it.trackIdx) }?.let {
                            return it
                        }
                    }
                }
                cur = Cursor(cur.fileIdx + delta)
            }
        }
    }

    override val items: Flow<ReceiveChannel<PlayableItem>>
        get() = currentStream

    override suspend fun activate(uri: Uri) {
        state.navigate(uri)?.let {
            activate(it)
        }
    }

    private suspend fun activate(cursor: Cursor) {
        LOG.d { "Activate $cursor" }
        currentPosition.store(null)
        currentStream.emit(state.channelFrom(cursor))
    }

    override suspend fun next() = advance(+1)

    override suspend fun prev() = advance(-1)

    private suspend fun advance(delta: Int) = currentPosition.load()?.let { current ->
        state.advance(current, delta)?.let {
            activate(it)
        }
    } ?: Unit

    override var shuffled: Boolean
        get() = TODO("Not yet implemented")
        set(value) {}

    override fun release() = scope.cancel()

    private companion object {
        private val LOG = Logger(FolderQueue::class.java.name)
    }
}
