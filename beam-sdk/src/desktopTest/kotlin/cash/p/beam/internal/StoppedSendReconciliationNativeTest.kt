package cash.p.beam.internal

import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.deleteIfExists
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assume.assumeNoException

class StoppedSendReconciliationNativeTest {
    @Test
    fun newWalletBirthday_rejectsStaleTipButAcceptsNormalBlockLag() {
        NativeLibraryLoader.load()
        assertTrue(newWalletTipFresh(1_000L, 1_000L))
        assertTrue(newWalletTipFresh(1_166L, 1_000L))
        assertTrue(newWalletTipFresh(1_000L, 1_166L))
        assertFalse(newWalletTipFresh(1_601L, 1_000L))
    }

    @Test
    fun interruptedNewWallet_reopenDisablesLegacyBodyScanBeforeBirthdayResolution() {
        NativeLibraryLoader.load()
        val directory = Files.createTempDirectory("beam-sdk-interrupted-bootstrap-")
        val seed = ByteArray(64) { index -> (0x31 + index).toByte() }
        val key = ByteArray(32) { index -> (0x41 + index).toByte() }
        var handle = 0L

        try {
            handle = BeamNative.create(
                storagePath = directory.toString(),
                databaseKey = key,
                seed = seed,
                network = 1,
                restoreType = -1,
                restoreValue = "",
                logLevel = 0,
            )
            seedInterruptedBootstrap(handle)
            assertTrue(BeamNative.bodyRequestsPendingForTests(handle))
            BeamNative.close(handle)
            handle = 0L

            handle = BeamNative.open(directory.toString(), key, network = 1, logLevel = 0)
            assertFalse(
                BeamNative.bodyRequestsPendingForTests(handle),
                "An unfinished wallet must sync headers before installing its birthday body cursor",
            )
            assertEquals(
                0L,
                newWalletCreatedAt(handle),
                "A legacy wallet with no creation timestamp must conservatively scan from genesis",
            )
        } finally {
            if (handle != 0L) runCatching { BeamNative.close(handle) }
            seed.fill(0)
            key.fill(0)
            directory.deleteRecursively()
        }
    }

    @Test
    fun preparedSend_canBeResolvedAndAbortedAfterStoppedReopen() {
        NativeLibraryLoader.load()
        val directory = Files.createTempDirectory("beam-sdk-stopped-send-")
        val seed = ByteArray(64) { index -> (0x21 + index).toByte() }
        val key = ByteArray(32) { index -> (0x11 + index).toByte() }
        var handle = 0L

        try {
            handle = BeamNative.create(
                storagePath = directory.toString(),
                databaseKey = key,
                seed = seed,
                network = 1,
                restoreType = -1,
                restoreValue = "",
                logLevel = 0,
            )
            assertNotEquals(0L, handle)
            val transactionId = seedPreparedSend(handle)
            BeamNative.close(handle)
            handle = 0L

            handle = BeamNative.open(directory.toString(), key, network = 1, logLevel = 0)
            assertResolution("Prepared", transactionId, BeamNative.resolveSend(handle, OPERATION_ID))
            assertTrue(BeamNative.abortPrepared(handle, OPERATION_ID))
            assertResolution("NotPrepared", null, BeamNative.resolveSend(handle, OPERATION_ID))
            assertFalse(BeamNative.abortPrepared(handle, OPERATION_ID))
        } finally {
            if (handle != 0L) runCatching { BeamNative.close(handle) }
            seed.fill(0)
            key.fill(0)
            directory.deleteRecursively()
        }
    }

    private fun assertResolution(expectedKind: String, expectedTransactionId: String?, encoded: String) {
        val value = Json.parseToJsonElement(encoded).jsonObject
        assertEquals(expectedKind, value.getValue("kind").jsonPrimitive.content)
        assertEquals(expectedTransactionId, value["transactionId"]?.jsonPrimitive?.content)
    }

    private fun seedPreparedSend(handle: Long): String = try {
        BeamNative.seedPreparedSendForTests(handle, OPERATION_ID)
    } catch (missingFixture: UnsatisfiedLinkError) {
        if (System.getenv(EXPECT_NATIVE_TEST_FIXTURES) == "1") {
            throw AssertionError("Instrumented native test fixture is missing", missingFixture)
        }
        assumeNoException(
            "Stopped-send native fixture requires BEAM_NATIVE_TESTS=1; production native is valid",
            missingFixture,
        )
        error("JUnit assumption did not abort the test")
    }

    private fun seedInterruptedBootstrap(handle: Long) = try {
        BeamNative.seedInterruptedBootstrapForTests(handle)
    } catch (missingFixture: UnsatisfiedLinkError) {
        if (System.getenv(EXPECT_NATIVE_TEST_FIXTURES) == "1") {
            throw AssertionError("Instrumented native test fixture is missing", missingFixture)
        }
        assumeNoException(
            "Interrupted-bootstrap native fixture requires BEAM_NATIVE_TESTS=1; production native is valid",
            missingFixture,
        )
    }

    private fun newWalletCreatedAt(handle: Long): Long = try {
        BeamNative.newWalletCreatedAtForTests(handle)
    } catch (missingFixture: UnsatisfiedLinkError) {
        if (System.getenv(EXPECT_NATIVE_TEST_FIXTURES) == "1") {
            throw AssertionError("Instrumented native test fixture is missing", missingFixture)
        }
        assumeNoException(
            "New-wallet migration native fixture requires BEAM_NATIVE_TESTS=1; production native is valid",
            missingFixture,
        )
        error("JUnit assumption did not abort the test")
    }

    private fun newWalletTipFresh(creationTimestamp: Long, tipTimestamp: Long): Boolean = try {
        BeamNative.newWalletTipFreshForTests(creationTimestamp, tipTimestamp)
    } catch (missingFixture: UnsatisfiedLinkError) {
        if (System.getenv(EXPECT_NATIVE_TEST_FIXTURES) == "1") {
            throw AssertionError("Instrumented native test fixture is missing", missingFixture)
        }
        assumeNoException(
            "New-wallet tip-freshness fixture requires BEAM_NATIVE_TESTS=1; production native is valid",
            missingFixture,
        )
        error("JUnit assumption did not abort the test")
    }

    private companion object {
        const val OPERATION_ID = "123e4567-e89b-42d3-a456-426614174001"
        const val EXPECT_NATIVE_TEST_FIXTURES = "BEAM_EXPECT_NATIVE_TEST_FIXTURES"
    }
}

private fun Path.deleteRecursively() {
    Files.walk(this).use { paths ->
        paths.sorted(Comparator.reverseOrder()).forEach(Path::deleteIfExists)
    }
}
