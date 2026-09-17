package cash.p.beam.internal

import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.isDirectory
import kotlin.io.path.name

/**
 * Beam core owns a single process-wide logger, so each scenario below lives in its own class and
 * the `desktopLoggingTest` Gradle task gives each class its own JVM. Running any of them inside
 * the shared `desktopTest` JVM would assert against whichever session happened to open first.
 */
internal object CoreLogging {
    /** Mirrors `BeamLogLevel`: None=0, Error=1, Info=2, Debug=3. */
    const val NONE = 0
    const val INFO = 2
    const val DEBUG = 3

    fun newStorage(tag: String): Path = Files.createTempDirectory("beam-sdk-logging-$tag-")

    /** Opens a session purely to bind the logger, then closes it. */
    fun openAndClose(storage: Path, logLevel: Int) {
        NativeLibraryLoader.load()
        val seed = ByteArray(64) { index -> (0x11 + index).toByte() }
        val key = ByteArray(32) { index -> (0x71 + index).toByte() }
        var handle = 0L
        try {
            handle = BeamNative.create(
                storagePath = storage.toString(),
                databaseKey = key,
                seed = seed,
                network = 1,
                restoreType = -1,
                restoreValue = "",
                logLevel = logLevel,
                requireRecoveryQuorum = false,
            )
        } finally {
            if (handle != 0L) BeamNative.close(handle)
        }
    }

    fun logFiles(storage: Path): List<Path> {
        val directory = storage.resolve("logs")
        if (!directory.isDirectory()) return emptyList()
        Files.list(directory).use { stream ->
            return stream.filter { it.name.startsWith("beam_") && it.name.endsWith(".log") }
                .toList()
        }
    }
}
