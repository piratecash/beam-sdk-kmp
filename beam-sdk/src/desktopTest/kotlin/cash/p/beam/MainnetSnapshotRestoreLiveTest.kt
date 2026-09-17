package cash.p.beam

import java.nio.file.Files
import java.security.SecureRandom
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.Assume.assumeTrue

/**
 * Live evidence for R1, which is not offline-testable: against real mainnet nodes, a wallet
 * restored through [RestoreSource.SnapshotThenScan] must keep advancing its body scan.
 *
 * This reproduces the measured failure exactly. On a Pixel 6 the same restore left
 * `currentHeight` frozen at 4040837 while the tip advanced 36 blocks, because every recovery
 * request was parked waiting for a second qualifying peer that never appeared. The falsifiable
 * signal is therefore movement of `currentHeight`, not reaching `Ready`: a wallet that merely
 * connects proves nothing, and a full mainnet restore is far too long to wait out here.
 *
 * The seed is generated randomly for every run. The wallet is empty by construction and no
 * real seed phrase is ever involved.
 */
class MainnetSnapshotRestoreLiveTest {
    @Test
    fun mainnetSnapshotRestore_bodyScanKeepsAdvancing() = runBlocking {
        assumeTrue(
            "Set BEAM_MAINNET_LIVE=1 to run the mainnet restore evidence test",
            System.getenv("BEAM_MAINNET_LIVE") == "1",
        )
        val budgetMs = System.getenv("BEAM_MAINNET_LIVE_BUDGET_MS")?.toLongOrNull() ?: 3_600_000L
        val requiredAdvances = System.getenv("BEAM_MAINNET_LIVE_ADVANCES")?.toIntOrNull() ?: 3

        // Kept deliberately: the Debug log under $storagePath/logs is the second half of the
        // evidence, so the directory outlives the test and its path is printed below.
        val directory = Files.createTempDirectory("beam-mainnet-live-")
        val seed = ByteArray(64).also(SecureRandom()::nextBytes)
        println("LIVE storage=$directory")

        val session = BeamWalletFactory().restore(
            config = BeamSdkConfig(
                network = BeamNetwork.Mainnet,
                storagePath = directory.toString(),
                // Exercises S3 on the same run: before this change the setting was inert.
                logLevel = BeamLogLevel.Debug,
                // The shipped default, stated explicitly because it is what is under test.
                // BEAM_MAINNET_LIVE_QUORUM=1 runs the control: with the quorum back on the same
                // restore must NOT complete, which is what makes the passing run evidence rather
                // than a wallet that happened to work.
                requireRecoveryQuorum = System.getenv("BEAM_MAINNET_LIVE_QUORUM") == "1",
            ),
            seed = seed,
            databaseKey = ByteArray(32).also(SecureRandom()::nextBytes),
            source = RestoreSource.SnapshotThenScan(),
        )

        var advances = 0
        var reachedReady = false
        var lastHeight: Long? = null
        var lastPhase: BeamRestorePhase? = null
        var failure: BeamFailure? = null
        val started = System.currentTimeMillis()

        try {
            session.start()
            withTimeoutOrNull(budgetMs) {
                // `first` and not `takeWhile`: takeWhile tests the predicate before the next
                // emission, so the run would idle waiting for one more update it does not need.
                session.state.first { state ->
                        val elapsed = (System.currentTimeMillis() - started) / 1000
                        when (state) {
                            is BeamWalletState.Restoring -> {
                                val progress = state.progress
                                if (progress.phase != lastPhase) {
                                    lastPhase = progress.phase
                                    println("LIVE ${elapsed}s phase=${progress.phase}")
                                }
                                val height = progress.currentHeight
                                if (height != null && (lastHeight == null || height > lastHeight!!)) {
                                    if (lastHeight != null) advances++
                                    lastHeight = height
                                    println("LIVE ${elapsed}s height=$height advances=$advances")
                                }
                            }
                            // Ready before the scan produced three increments is still a pass:
                            // the scan finished, which is strictly more than it needs to prove.
                            is BeamWalletState.Ready -> {
                                println("LIVE ${elapsed}s READY height=${state.height}")
                                reachedReady = true
                            }
                            is BeamWalletState.Error -> {
                                failure = state.failure
                                println("LIVE ${elapsed}s ERROR ${state.failure}")
                            }
                            else -> println("LIVE ${elapsed}s state=$state")
                        }
                        reachedReady || advances >= requiredAdvances || failure != null
                }
            }
        } finally {
            session.close()
        }

        println(
            "LIVE done ready=$reachedReady advances=$advances lastHeight=$lastHeight " +
                "failure=$failure storage=$directory",
        )
        assertTrue(failure == null, "the wallet reported $failure")
        // Either signal alone is sufficient and they are reported separately on purpose: a run
        // that reaches Ready proves the scan finished, which is strictly stronger than movement,
        // but collapsing the two into one counter hides which of them actually fired.
        assertTrue(
            reachedReady || advances >= requiredAdvances,
            "the body scan neither completed nor advanced ${requiredAdvances} times in " +
                "${budgetMs / 1000}s (advances=$advances, lastHeight=$lastHeight, " +
                "phase=$lastPhase) - this is the measured deadlock",
        )
    }
}
