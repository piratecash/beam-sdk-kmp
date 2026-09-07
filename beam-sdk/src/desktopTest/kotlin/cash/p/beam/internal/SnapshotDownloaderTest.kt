package cash.p.beam.internal

import cash.p.beam.BeamFailure
import cash.p.beam.BeamNetwork
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.utils.io.ByteChannel
import io.ktor.utils.io.ByteReadChannel
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.Comparator
import java.util.Properties
import kotlin.io.path.deleteIfExists
import kotlin.io.path.readBytes
import kotlin.io.path.writeBytes
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

class SnapshotDownloaderTest {
    @Test
    fun download_validPinnedSnapshot_promotesCompleteFile() = runBlocking {
        val directory = Files.createTempDirectory("beam-snapshot-test-")
        val client = HttpClient(MockEngine) {
            engine {
                addHandler {
                    respond(
                        content = ByteReadChannel("beam".encodeToByteArray()),
                        status = HttpStatusCode.OK,
                        headers = headersOf(
                            HttpHeaders.ContentLength to listOf("4"),
                            HttpHeaders.ETag to listOf("\"snapshot-v1\""),
                        ),
                    )
                }
            }
        }
        try {
            val result = SnapshotDownloader(client).download(
                network = BeamNetwork.Mainnet,
                storagePath = directory.toString(),
                requestedUrl = null,
                expectedSha256 = "ae4b867cf2eeb128ceab8c7df148df2eacfe2be35dbd40856a77bfc74f882236",
                onProgress = { _, _ -> },
            )
            assertEquals(4, result.bytes)
            assertContentEquals("beam".encodeToByteArray(), result.path.readBytes())
            assertFalse(directory.resolve(".beam-recovery/recovery.bin.part").toFile().exists())
        } finally {
            client.close()
            directory.deleteTree()
        }
    }

    @Test
    fun download_strongEtagResume_requiresMatching206Range() = runBlocking {
        val directory = Files.createTempDirectory("beam-snapshot-resume-")
        val recovery = directory.resolve(".beam-recovery")
        Files.createDirectories(recovery)
        recovery.resolve("recovery.bin.part").writeBytes("be".encodeToByteArray())
        Properties().apply {
            setProperty("url", MAINNET_URL)
            setProperty("etag", "\"snapshot-v1\"")
        }.let { properties ->
            FileOutputStream(recovery.resolve("recovery.resume.properties").toFile()).use { output ->
                properties.store(output, null)
            }
        }
        val client = HttpClient(MockEngine) {
            engine {
                addHandler { request ->
                    assertEquals("bytes=2-", request.headers[HttpHeaders.Range])
                    assertEquals("\"snapshot-v1\"", request.headers[HttpHeaders.IfRange])
                    respond(
                        content = ByteReadChannel("am".encodeToByteArray()),
                        status = HttpStatusCode.PartialContent,
                        headers = headersOf(
                            HttpHeaders.ContentLength to listOf("2"),
                            HttpHeaders.ContentRange to listOf("bytes 2-3/4"),
                            HttpHeaders.ETag to listOf("\"snapshot-v1\""),
                        ),
                    )
                }
            }
        }
        try {
            val result = SnapshotDownloader(client).download(
                BeamNetwork.Mainnet,
                directory.toString(),
                MAINNET_URL,
                null,
            ) { _, _ -> }
            assertContentEquals("beam".encodeToByteArray(), result.path.readBytes())
        } finally {
            client.close()
            directory.deleteTree()
        }
    }

    @Test
    fun download_hashMismatch_removesUntrustedPayload() = runBlocking {
        val directory = Files.createTempDirectory("beam-snapshot-hash-")
        val client = HttpClient(MockEngine) {
            engine {
                addHandler {
                    respond(
                        ByteReadChannel("beam".encodeToByteArray()),
                        HttpStatusCode.OK,
                        headersOf(HttpHeaders.ContentLength to listOf("4")),
                    )
                }
            }
        }
        try {
            assertFailsWith<BeamFailure.Download> {
                SnapshotDownloader(client).download(
                    BeamNetwork.Mainnet,
                    directory.toString(),
                    null,
                    "0".repeat(64),
                ) { _, _ -> }
            }
            assertFalse(directory.resolve(".beam-recovery/recovery.bin").toFile().exists())
            assertFalse(directory.resolve(".beam-recovery/recovery.bin.part").toFile().exists())
        } finally {
            client.close()
            directory.deleteTree()
        }
    }

    @Test
    fun download_nonAllowlistedHost_isRejectedBeforeNetwork() = runBlocking {
        val directory = Files.createTempDirectory("beam-snapshot-url-")
        val client = HttpClient(MockEngine) { engine { addHandler { error("must not be requested") } } }
        try {
            assertFailsWith<IllegalArgumentException> {
                SnapshotDownloader(client).download(
                    BeamNetwork.Mainnet,
                    directory.toString(),
                    "https://example.com/recovery.bin",
                    null,
                ) { _, _ -> }
            }
            Unit
        } finally {
            client.close()
            directory.deleteTree()
        }
    }

    @Test
    fun download_oversizedStreamingResponse_rejectsBeforeReadingBody() = runBlocking {
        val directory = Files.createTempDirectory("beam-snapshot-stream-limit-")
        val bodyThatNeverCompletes = ByteChannel()
        val client = HttpClient(MockEngine) {
            engine {
                addHandler {
                    respond(
                        content = bodyThatNeverCompletes,
                        status = HttpStatusCode.OK,
                        headers = headersOf(
                            HttpHeaders.ContentLength to listOf((2L * 1024L * 1024L * 1024L + 1L).toString()),
                        ),
                    )
                }
            }
        }
        try {
            withTimeout(1_000) {
                assertFailsWith<BeamFailure.Download> {
                    SnapshotDownloader(client).download(
                        BeamNetwork.Mainnet,
                        directory.toString(),
                        null,
                        null,
                    ) { _, _ -> }
                }
            }
            Unit
        } finally {
            client.close()
            directory.deleteTree()
        }
    }

    @Test
    fun download_insufficientDiskSpace_rejectsBeforeReadingBody() = runBlocking {
        val directory = Files.createTempDirectory("beam-snapshot-disk-space-")
        val bodyThatNeverCompletes = ByteChannel()
        val client = HttpClient(MockEngine) {
            engine {
                addHandler {
                    respond(
                        content = bodyThatNeverCompletes,
                        status = HttpStatusCode.OK,
                        headers = headersOf(HttpHeaders.ContentLength to listOf("4")),
                    )
                }
            }
        }
        try {
            withTimeout(1_000) {
                assertFailsWith<BeamFailure.Storage> {
                    SnapshotDownloader(client, usableSpace = { 0L }).download(
                        BeamNetwork.Mainnet,
                        directory.toString(),
                        null,
                        null,
                    ) { _, _ -> }
                }
            }
            Unit
        } finally {
            client.close()
            bodyThatNeverCompletes.cancel(null)
            directory.deleteTree()
        }
    }

    @Test
    fun download_redirectToNonAllowlistedHost_isRejectedBeforeSecondRequest() = runBlocking {
        val directory = Files.createTempDirectory("beam-snapshot-redirect-")
        var requestCount = 0
        val client = HttpClient(MockEngine) {
            followRedirects = false
            engine {
                addHandler {
                    requestCount++
                    respond(
                        content = ByteReadChannel.Empty,
                        status = HttpStatusCode.Found,
                        headers = headersOf(HttpHeaders.Location to listOf("https://example.com/recovery.bin")),
                    )
                }
            }
        }
        try {
            val failure = assertFailsWith<BeamFailure.Download> {
                SnapshotDownloader(client).download(
                    BeamNetwork.Mainnet,
                    directory.toString(),
                    MAINNET_URL,
                    null,
                ) { _, _ -> }
            }
            assertTrue(failure.message.orEmpty().contains("allowlisted"))
            assertEquals(1, requestCount)
        } finally {
            client.close()
            directory.deleteTree()
        }
    }

    private companion object {
        const val MAINNET_URL = "https://mobile-restore.beam.mw/mainnet/mainnet_recovery.bin"
    }
}

private fun Path.deleteTree() {
    Files.walk(this).use { paths ->
        paths.sorted(Comparator.reverseOrder()).forEach(Path::deleteIfExists)
    }
}
