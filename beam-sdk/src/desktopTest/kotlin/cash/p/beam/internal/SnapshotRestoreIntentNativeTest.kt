package cash.p.beam.internal

import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.deleteIfExists
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject

class SnapshotRestoreIntentNativeTest {
    @Test
    fun snapshotIntent_isCreatedAtomicallyAndSurvivesReopenBeforeDownload() {
        NativeLibraryLoader.load()
        val directory = Files.createTempDirectory("beam-sdk-snapshot-intent-")
        val seed = ByteArray(64) { index -> (0x51 + index).toByte() }
        val key = ByteArray(32) { index -> (0x31 + index).toByte() }
        val snapshotPath = directory.resolve(".beam-recovery/recovery.bin").toAbsolutePath().toString()
        val intent = buildJsonObject {
            put("snapshotUrl", JsonPrimitive(SNAPSHOT_URL))
            put("expectedSha256", JsonPrimitive(SNAPSHOT_HASH))
            put("path", JsonPrimitive(snapshotPath))
        }.toString()
        val expectedPersistedIntent = buildJsonObject {
            put("snapshotUrl", JsonPrimitive(SNAPSHOT_URL))
            put("expectedSha256", JsonPrimitive(SNAPSHOT_HASH))
        }
        var handle = 0L

        try {
            handle = BeamNative.create(
                storagePath = directory.toString(),
                databaseKey = key,
                seed = seed,
                network = 1,
                restoreType = 2,
                restoreValue = intent,
                logLevel = 0,
            )
            assertNotEquals(0L, handle)
            assertEquals(expectedPersistedIntent, Json.parseToJsonElement(BeamNative.snapshotRestoreIntent(handle)))
            BeamNative.close(handle)
            handle = 0L

            handle = BeamNative.open(directory.toString(), key, network = 1, logLevel = 0)
            assertNotEquals(0L, handle)
            assertEquals(expectedPersistedIntent, Json.parseToJsonElement(BeamNative.snapshotRestoreIntent(handle)))
        } finally {
            if (handle != 0L) runCatching { BeamNative.close(handle) }
            seed.fill(0)
            key.fill(0)
            directory.deleteRecursively()
        }
    }

    private companion object {
        const val SNAPSHOT_URL = "https://mobile-restore.beam.mw/testnet/testnet_recovery.bin"
        const val SNAPSHOT_HASH = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    }
}

private fun Path.deleteRecursively() {
    Files.walk(this).use { paths ->
        paths.sorted(Comparator.reverseOrder()).forEach(Path::deleteIfExists)
    }
}
