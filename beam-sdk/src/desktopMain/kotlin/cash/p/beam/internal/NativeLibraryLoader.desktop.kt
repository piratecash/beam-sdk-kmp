package cash.p.beam.internal

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.PosixFilePermission
import java.util.Locale

internal actual object NativeLibraryLoader {
    @Volatile
    private var loaded = false

    actual fun load() {
        if (loaded) return
        synchronized(this) {
            if (loaded) return
            val explicitPath = System.getProperty("beam.sdk.native.path")
            val library = explicitPath?.let(Path::of) ?: extractPackagedLibrary()
            require(Files.isRegularFile(library)) { "Beam native library does not exist: $library" }
            System.load(library.toAbsolutePath().toString())
            loaded = true
        }
    }

    private fun extractPackagedLibrary(): Path {
        val fileName = System.mapLibraryName("beam_sdk_kmp")
        val resourcePath = "/native/$hostTriple/$fileName"
        val input = NativeLibraryLoader::class.java.getResourceAsStream(resourcePath)
            ?: error(
                "Beam native library is not packaged for $hostTriple. " +
                    "Set -Dbeam.sdk.native.path=/absolute/path/to/$fileName for a local build.",
            )
        val directory = Files.createTempDirectory("beam-sdk-native-")
        val target = directory.resolve(fileName)
        input.use { Files.copy(it, target, StandardCopyOption.REPLACE_EXISTING) }
        setOwnerOnlyPermissions(directory, target)
        directory.toFile().deleteOnExit()
        target.toFile().deleteOnExit()
        return target
    }

    private fun setOwnerOnlyPermissions(directory: Path, library: Path) {
        runCatching {
            Files.setPosixFilePermissions(
                directory,
                setOf(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE,
                ),
            )
            Files.setPosixFilePermissions(
                library,
                setOf(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE),
            )
        }
    }

    private val hostTriple: String
        get() {
            val os = System.getProperty("os.name").lowercase(Locale.ROOT)
            val arch = when (System.getProperty("os.arch").lowercase(Locale.ROOT)) {
                "amd64", "x86_64" -> "x86_64"
                "aarch64", "arm64" -> "aarch64"
                else -> error("Unsupported Beam desktop architecture")
            }
            return when {
                os.startsWith("mac") -> "$arch-apple-darwin"
                os.startsWith("windows") -> "$arch-pc-windows-msvc"
                os.startsWith("linux") -> "$arch-unknown-linux-gnu"
                else -> error("Unsupported Beam desktop operating system")
            }
        }
}
