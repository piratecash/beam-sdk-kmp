package cash.p.beam.sample

import cash.p.beam.BeamAddressType
import cash.p.beam.BeamLogLevel
import cash.p.beam.BeamNetwork
import cash.p.beam.BeamSdkConfig
import cash.p.beam.BeamWalletFactory
import cash.p.beam.BeamWalletSession
import cash.p.beam.BeamWalletState
import cash.p.beam.RestoreSource
import io.horizontalsystems.hdwalletkit.Mnemonic
import java.nio.file.Files
import java.nio.file.Path
import java.util.Comparator
import kotlin.io.path.deleteIfExists
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assume.assumeTrue

class MainnetSnapshotRestoreLiveTest {
    @Test
    fun configuredWallet_restoresOfficialMainnetSnapshotAndReachesReady() = runBlocking {
        assumeTrue(
            "Set BEAM_LIVE_MAINNET_RESTORE=1 to run the mainnet snapshot restore test",
            System.getenv("BEAM_LIVE_MAINNET_RESTORE") == "1",
        )

        val directory = Files.createTempDirectory("beam-mainnet-snapshot-restore-")
        var session: BeamWalletSession? = null
        try {
            val seed = configuredSeed()
            val restoredSession = try {
                val databaseKey = configuredDatabaseKey()
                try {
                    BeamWalletFactory().restore(
                        config = BeamSdkConfig(
                            network = BeamNetwork.Mainnet,
                            storagePath = directory.toString(),
                            logLevel = BeamLogLevel.None,
                        ),
                        seed = seed,
                        databaseKey = databaseKey,
                        source = RestoreSource.SnapshotThenScan(),
                    )
                } finally {
                    databaseKey.fill(0)
                }
            } finally {
                seed.fill(0)
            }
            session = restoredSession

            var downloadedBytes = 0L
            val terminal = withTimeout(30 * 60 * 1_000L) {
                val terminalState = async(start = CoroutineStart.UNDISPATCHED) {
                    restoredSession.state.first { state ->
                        if (state is BeamWalletState.Restoring) {
                            downloadedBytes = maxOf(
                                downloadedBytes,
                                state.progress.downloadedBytes ?: 0L,
                            )
                        }
                        state is BeamWalletState.Ready || state is BeamWalletState.Error
                    }
                }
                try {
                    restoredSession.start()
                } catch (cancellation: CancellationException) {
                    throw cancellation
                } catch (failure: Throwable) {
                    terminalState.cancel()
                    throw AssertionError(
                        "Wallet start failed before reaching a terminal state",
                        failure,
                    )
                }
                terminalState.await()
            }

            val ready = assertIs<BeamWalletState.Ready>(terminal, "Restore did not reach Ready")
            assertTrue(restoredSession.balance.value.isAuthoritative, "Ready balance must be authoritative")
            assertTrue(downloadedBytes > 0L, "Snapshot restore must download data")

            val address = restoredSession.receiveAddress(BeamAddressType.PublicOffline)
            assertEquals(BeamAddressType.PublicOffline, address.type)
            assertEquals(BeamNetwork.Mainnet, address.network)

            val transactionCount = restoredSession.transactionPage(offset = 0, limit = 20).items.size
            println(
                "BEAM_LIVE_MAINNET_RESTORE_SUCCESS " +
                    "height=${ready.height} downloadedBytes=$downloadedBytes " +
                    "transactionCount=$transactionCount",
            )
        } finally {
            try {
                session?.close()
            } finally {
                directory.deleteLiveTestDirectory()
            }
        }
    }
}

private fun configuredSeed(): ByteArray {
    val words = DemoConfig.WORDS.trim().split(Regex("\\s+")).filter(String::isNotEmpty)
    check(words.isNotEmpty()) { "Configure words in the ignored local.properties file" }
    return runCatching { Mnemonic().toSeed(words) }
        .getOrElse { throw AssertionError("Configured mnemonic is invalid") }
        .also { seed -> check(seed.size == 64) { "Configured mnemonic did not produce a 64-byte seed" } }
}

private fun configuredDatabaseKey(): ByteArray {
    val hex = DemoConfig.DATABASE_KEY_HEX.trim()
    check(hex.length == 64 && hex.all { it.isDigit() || it.lowercaseChar() in 'a'..'f' }) {
        "Configure a 32-byte hexadecimal database key in the ignored local.properties file"
    }
    return ByteArray(32) { index -> hex.substring(index * 2, index * 2 + 2).toInt(16).toByte() }
}

private fun Path.deleteLiveTestDirectory() {
    if (!Files.exists(this)) return
    Files.walk(this).use { paths ->
        paths.sorted(Comparator.reverseOrder()).forEach(Path::deleteIfExists)
    }
}
