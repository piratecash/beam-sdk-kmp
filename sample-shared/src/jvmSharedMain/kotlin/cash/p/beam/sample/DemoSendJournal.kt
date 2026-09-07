package cash.p.beam.sample

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.util.Base64

internal actual object DemoSendJournal {
    actual fun load(journalRoot: String, legacyNetwork: cash.p.beam.BeamNetwork): DemoPendingSend? {
        val target = target(journalRoot)
        if (!Files.exists(target)) return null
        require(Files.isRegularFile(target)) { "Invalid Beam send journal" }
        val contents = String(Files.readAllBytes(target), StandardCharsets.US_ASCII).trim()
        require(contents.isNotEmpty()) { "Invalid Beam send journal" }
        val lines = contents.lines()
        if (lines.first() != FORMAT_VERSION) {
            return DemoPendingSend(
                owner = ActiveWalletIdentity(legacyNetwork, journalRoot),
                operationId = contents,
            )
        }
        require(lines.size == 4) { "Invalid Beam send journal" }
        val network = cash.p.beam.BeamNetwork.entries.firstOrNull { it.name == lines[1] }
            ?: error("Invalid Beam send journal network")
        val ownerPath = String(Base64.getUrlDecoder().decode(lines[2]), StandardCharsets.UTF_8)
        require(ownerPath.isNotBlank() && lines[3].isNotBlank()) { "Invalid Beam send journal" }
        return DemoPendingSend(
            owner = ActiveWalletIdentity(network, ownerPath),
            operationId = lines[3],
        )
    }

    @Synchronized
    actual fun claim(
        journalRoot: String,
        owner: ActiveWalletIdentity,
        operationId: String,
    ): Boolean = withExclusiveLock(journalRoot) {
        val journal = target(journalRoot)
        if (Files.exists(journal)) return@withExclusiveLock false
        val bytes = encode(owner, operationId)
        try {
            if (isWindows()) {
                // Windows cannot open a directory as a FileChannel. CREATE_NEW plus force(true)
                // durably publishes this file through the same channel before native prepare can
                // begin. A crash during the write leaves a corrupt file, which load() fails closed.
                writeAndForce(journal, bytes, StandardOpenOption.CREATE_NEW)
            } else {
                val temporary = journal.resolveSibling("${journal.fileName}.tmp")
                writeAndForce(
                    temporary,
                    bytes,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                )
                try {
                    Files.move(
                        temporary,
                        journal,
                        StandardCopyOption.ATOMIC_MOVE,
                    )
                } catch (_: AtomicMoveNotSupportedException) {
                    Files.move(temporary, journal)
                }
                forceDirectory(journal.parent)
            }
            true
        } catch (error: Throwable) {
            Files.deleteIfExists(journal.resolveSibling("${journal.fileName}.tmp"))
            Files.deleteIfExists(journal)
            throw error
        }
    }

    @Synchronized
    actual fun clear(
        journalRoot: String,
        owner: ActiveWalletIdentity,
        operationId: String,
    ): Unit = withExclusiveLock(journalRoot) {
        val current = load(journalRoot, owner.network) ?: return@withExclusiveLock
        if (current.owner == owner && current.operationId == operationId) {
            Files.deleteIfExists(target(journalRoot))
            // A lost Windows deletion can only retain a stale guard and block another send.
            if (!isWindows()) forceDirectory(Paths.get(journalRoot))
        }
    }

    private fun target(journalRoot: String): Path = Paths.get(journalRoot).resolve(".demo-active-send")

    private fun lockTarget(journalRoot: String): Path =
        Paths.get(journalRoot).resolve(".demo-active-send.lock")

    private fun encode(owner: ActiveWalletIdentity, operationId: String): ByteArray {
        val encodedOwner = Base64.getUrlEncoder().withoutPadding()
            .encodeToString(owner.storagePath.toByteArray(StandardCharsets.UTF_8))
        return listOf(FORMAT_VERSION, owner.network.name, encodedOwner, operationId)
            .joinToString("\n")
            .toByteArray(StandardCharsets.US_ASCII)
    }

    private fun writeAndForce(
        path: Path,
        bytes: ByteArray,
        vararg options: StandardOpenOption,
    ) {
        val openOptions = mutableSetOf<java.nio.file.OpenOption>(StandardOpenOption.WRITE)
        openOptions.addAll(options)
        FileChannel.open(path, openOptions).use { channel ->
            val buffer = ByteBuffer.wrap(bytes)
            while (buffer.hasRemaining()) channel.write(buffer)
            channel.force(true)
        }
    }

    private inline fun <T> withExclusiveLock(journalRoot: String, block: () -> T): T {
        val lock = lockTarget(journalRoot)
        Files.createDirectories(lock.parent)
        return FileChannel.open(
            lock,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
        ).use { channel ->
            channel.lock().use { block() }
        }
    }

    private fun forceDirectory(directory: Path) {
        FileChannel.open(directory, StandardOpenOption.READ).use { channel ->
            channel.force(true)
        }
    }

    private fun isWindows(): Boolean =
        System.getProperty("os.name").orEmpty().startsWith("Windows", ignoreCase = true)

    private const val FORMAT_VERSION = "beam-demo-send-v2"
}

internal actual object DemoWalletStorage {
    actual fun walletExists(storagePath: String): Boolean =
        Files.isRegularFile(Paths.get(storagePath).resolve("wallet.db"))

    actual fun recoveryPath(storagePath: String, recoveryId: String): String =
        Paths.get(storagePath).resolve("recovery").resolve(recoveryId).toString()
}
