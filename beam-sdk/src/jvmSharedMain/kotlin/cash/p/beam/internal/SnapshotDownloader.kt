package cash.p.beam.internal

import cash.p.beam.BeamFailure
import cash.p.beam.BeamNetwork
import io.ktor.client.HttpClient
import io.ktor.client.request.header
import io.ktor.client.request.prepareGet
import io.ktor.client.statement.bodyAsChannel
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.Url
import io.ktor.utils.io.readAvailable
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.FileOutputStream
import java.net.URI
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.util.Properties
import kotlin.io.path.exists
import kotlin.io.path.fileSize
import kotlinx.coroutines.CancellationException

internal data class SnapshotDownload(
    val path: Path,
    val bytes: Long,
    val sha256: String,
)

internal fun completedSnapshotPath(storagePath: String): Path =
    Paths.get(storagePath).resolve(".beam-recovery").resolve("recovery.bin")

internal class SnapshotDownloader(
    private val client: HttpClient,
    private val usableSpace: (Path) -> Long = { directory -> directory.toFile().usableSpace },
) {
    suspend fun download(
        network: BeamNetwork,
        storagePath: String,
        requestedUrl: String?,
        expectedSha256: String?,
        onProgress: (downloaded: Long, total: Long?) -> Unit,
    ): SnapshotDownload {
        val url = Url(requestedUrl ?: defaultUrl(network))
        validateUrl(url)
        val expectedHash = expectedSha256?.lowercase()?.also {
            require(SHA256_PATTERN.matches(it)) { "expectedSha256 must contain 64 hexadecimal characters" }
        }
        val directory = Paths.get(storagePath).resolve(RECOVERY_DIRECTORY)
        Files.createDirectories(directory)
        val partial = directory.resolve(PARTIAL_FILE)
        val complete = directory.resolve(COMPLETE_FILE)
        val metadataPath = directory.resolve(METADATA_FILE)
        Files.deleteIfExists(complete)

        var metadata = ResumeMetadata.read(metadataPath)
        var offset = partial.takeIf(Path::exists)?.fileSize() ?: 0L
        val resumeUrl = metadata?.url
        val resumeEtag = metadata?.etag
        if (resumeUrl != url.toString() || !resumeEtag.isStrongEtag() || offset <= 0L) {
            Files.deleteIfExists(partial)
            Files.deleteIfExists(metadataPath)
            metadata = null
            offset = 0L
        }

        try {
            var requestUrl = url
            var redirectCount = 0
            var restartedFromZero = false
            while (true) {
                validateUrl(requestUrl)
                val result = client.prepareGet(requestUrl) {
                    if (offset > 0L) {
                        header(HttpHeaders.Range, "bytes=$offset-")
                        header(HttpHeaders.IfRange, metadata?.etag.orEmpty())
                    }
                }.execute { response ->
                    if (response.status.value in 300..399) {
                        val location = response.headers[HttpHeaders.Location]
                            ?: throw BeamFailure.Download("Snapshot redirect omitted the Location header")
                        val redirected = resolveRedirect(response.call.request.url, location)
                        validateUrl(redirected)
                        return@execute DownloadResult.Redirect(redirected)
                    }
                    val responseEtag = response.headers[HttpHeaders.ETag]
                    val append = offset > 0L && response.status == HttpStatusCode.PartialContent &&
                        responseEtag == metadata?.etag &&
                        contentRangeStart(response.headers[HttpHeaders.ContentRange]) == offset
                    if (offset > 0L && !append) {
                        Files.deleteIfExists(partial)
                        Files.deleteIfExists(metadataPath)
                        metadata = null
                        offset = 0L
                        DownloadResult.RestartFromZero
                    } else {
                        if (response.status != HttpStatusCode.OK && !append) {
                            throw BeamFailure.Download("Snapshot server returned HTTP ${response.status.value}")
                        }

                        val responseLength = response.headers[HttpHeaders.ContentLength]?.toLongOrNull()
                        val declaredTotal = when {
                            append -> contentRangeTotal(response.headers[HttpHeaders.ContentRange])
                            responseLength != null -> responseLength
                            else -> null
                        }
                        validateSize(offset, responseLength, declaredTotal)
                        preflightDisk(directory, responseLength)
                        if (responseEtag.isStrongEtag()) {
                            ResumeMetadata(url.toString(), responseEtag!!).write(metadataPath)
                            metadata = ResumeMetadata(url.toString(), responseEtag)
                        } else {
                            Files.deleteIfExists(metadataPath)
                            metadata = null
                        }

                        var downloaded = offset
                        FileOutputStream(partial.toFile(), append).use { output ->
                            val channel = response.bodyAsChannel()
                            val buffer = ByteArray(BUFFER_SIZE)
                            while (!channel.isClosedForRead) {
                                val count = channel.readAvailable(buffer)
                                if (count == -1) break
                                if (count == 0) continue
                                downloaded += count
                                if (downloaded > MAX_SNAPSHOT_BYTES) {
                                    throw BeamFailure.Download("Snapshot exceeds the 2 GiB safety limit")
                                }
                                output.write(buffer, 0, count)
                                onProgress(downloaded, declaredTotal)
                            }
                            output.fd.sync()
                        }
                        if (declaredTotal != null && downloaded != declaredTotal) {
                            throw BeamFailure.Download(
                                "Snapshot is truncated: received $downloaded of $declaredTotal bytes",
                            )
                        }
                        DownloadResult.Complete
                    }
                }
                when (result) {
                    DownloadResult.Complete -> break
                    DownloadResult.RestartFromZero -> {
                        if (restartedFromZero) {
                            throw BeamFailure.Download("Snapshot server rejected a safe ranged resume")
                        }
                        restartedFromZero = true
                        requestUrl = url
                        redirectCount = 0
                    }
                    is DownloadResult.Redirect -> {
                        if (redirectCount >= MAX_REDIRECTS) {
                            throw BeamFailure.Download("Snapshot redirect limit exceeded")
                        }
                        redirectCount++
                        requestUrl = result.url
                    }
                }
            }

            val actualHash = sha256(partial)
            if (expectedHash != null && actualHash != expectedHash) {
                Files.deleteIfExists(partial)
                Files.deleteIfExists(metadataPath)
                throw BeamFailure.Download("Snapshot SHA-256 does not match the trusted value")
            }
            moveAtomically(partial, complete)
            Files.deleteIfExists(metadataPath)
            return SnapshotDownload(complete, complete.fileSize(), actualHash)
        } catch (cancelled: CancellationException) {
            if (!metadata?.etag.isStrongEtag()) Files.deleteIfExists(partial)
            throw cancelled
        } catch (failure: BeamFailure) {
            if (!metadata?.etag.isStrongEtag()) Files.deleteIfExists(partial)
            throw failure
        } catch (error: Throwable) {
            if (!metadata?.etag.isStrongEtag()) Files.deleteIfExists(partial)
            throw BeamFailure.Download(error.message ?: "Snapshot download failed", error)
        }
    }

    private fun validateUrl(url: Url) {
        require(url.protocol.name == "https") { "Beam snapshot URL must use HTTPS" }
        require(url.host.lowercase() in ALLOWED_HOSTS) { "Beam snapshot host is not allowlisted" }
        require(url.user.isNullOrEmpty() && url.password.isNullOrEmpty()) {
            "Beam snapshot URL must not contain credentials"
        }
        require(url.port == 443) { "Beam snapshot URL must use the standard HTTPS port" }
        require(url.fragment.isEmpty()) { "Beam snapshot URL must not contain a fragment" }
    }

    private fun resolveRedirect(current: Url, location: String): Url =
        Url(URI(current.toString()).resolve(location).toString())

    private fun validateSize(offset: Long, responseLength: Long?, total: Long?) {
        require(offset in 0..MAX_SNAPSHOT_BYTES) { "Invalid snapshot resume offset" }
        if (responseLength != null) require(responseLength >= 0L) { "Invalid snapshot content length" }
        if (total != null) require(total in 1..MAX_SNAPSHOT_BYTES) { "Invalid snapshot total size" }
        if (responseLength != null && offset + responseLength > MAX_SNAPSHOT_BYTES) {
            throw BeamFailure.Download("Snapshot exceeds the 2 GiB safety limit")
        }
    }

    private fun preflightDisk(directory: Path, remainingBytes: Long?) {
        if (remainingBytes == null) return
        // Android's java.nio provider rejects Files.getFileStore() even for app-private paths.
        // java.io.File delegates to the platform's statfs implementation on both Android and desktop.
        val usable = usableSpace(directory)
        if (usable < remainingBytes + DISK_SAFETY_BYTES) {
            throw BeamFailure.Storage("Not enough free space for Beam snapshot recovery")
        }
    }

    private fun sha256(path: Path): String {
        val digest = MessageDigest.getInstance("SHA-256")
        BufferedInputStream(FileInputStream(path.toFile())).use { input ->
            val buffer = ByteArray(BUFFER_SIZE)
            while (true) {
                val count = input.read(buffer)
                if (count == -1) break
                digest.update(buffer, 0, count)
            }
        }
        return digest.digest().joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }
    }

    private fun moveAtomically(source: Path, target: Path) {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        } catch (_: AtomicMoveNotSupportedException) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING)
        }
    }

    private data class ResumeMetadata(val url: String, val etag: String) {
        fun write(path: Path) {
            val properties = Properties().apply {
                setProperty("url", url)
                setProperty("etag", etag)
            }
            FileOutputStream(path.toFile()).use { output ->
                properties.store(output, null)
                output.fd.sync()
            }
        }

        companion object {
            fun read(path: Path): ResumeMetadata? = runCatching {
                if (!path.exists()) return null
                val properties = Properties()
                FileInputStream(path.toFile()).use(properties::load)
                ResumeMetadata(
                    url = requireNotNull(properties.getProperty("url")),
                    etag = requireNotNull(properties.getProperty("etag")),
                )
            }.getOrNull()
        }
    }

    private sealed interface DownloadResult {
        data object Complete : DownloadResult
        data object RestartFromZero : DownloadResult
        data class Redirect(val url: Url) : DownloadResult
    }

    private companion object {
        const val MAINNET_URL = "https://mobile-restore.beam.mw/mainnet/mainnet_recovery.bin"
        const val TESTNET_URL = "https://mobile-restore.beam.mw/testnet/testnet_recovery.bin"
        const val RECOVERY_DIRECTORY = ".beam-recovery"
        const val PARTIAL_FILE = "recovery.bin.part"
        const val COMPLETE_FILE = "recovery.bin"
        const val METADATA_FILE = "recovery.resume.properties"
        const val BUFFER_SIZE = 128 * 1024
        const val MAX_SNAPSHOT_BYTES = 2L * 1024L * 1024L * 1024L
        const val DISK_SAFETY_BYTES = 64L * 1024L * 1024L
        const val MAX_REDIRECTS = 5
        val SHA256_PATTERN = Regex("[0-9a-f]{64}")
        val ALLOWED_HOSTS = setOf("mobile-restore.beam.mw", "s3.eu-central-1.amazonaws.com")

        fun defaultUrl(network: BeamNetwork): String = when (network) {
            BeamNetwork.Mainnet -> MAINNET_URL
            BeamNetwork.Testnet -> TESTNET_URL
        }

        fun String?.isStrongEtag(): Boolean =
            this != null && isNotBlank() && !startsWith("W/", ignoreCase = true)

        fun contentRangeStart(value: String?): Long? =
            value?.substringAfter("bytes ", "")?.substringBefore('-')?.toLongOrNull()

        fun contentRangeTotal(value: String?): Long? =
            value?.substringAfter('/', "")?.takeUnless { it == "*" }?.toLongOrNull()
    }
}

internal expect fun platformSnapshotHttpClient(): HttpClient
