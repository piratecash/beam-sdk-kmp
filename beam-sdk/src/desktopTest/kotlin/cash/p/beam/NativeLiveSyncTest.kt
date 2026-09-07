package cash.p.beam

import java.nio.file.Files
import java.nio.file.Path
import java.util.Comparator
import kotlin.io.path.deleteIfExists
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assume.assumeTrue

class NativeLiveSyncTest {
    @Test
    fun freshTestnetWallet_reachesReadyThroughOfficialNodePool() = runBlocking {
        assumeTrue("Set BEAM_LIVE_TEST=1 to run the official-node integration test", System.getenv("BEAM_LIVE_TEST") == "1")
        val directory = Files.createTempDirectory("beam-live-test-")
        val session = BeamWalletFactory().createNew(
            config = BeamSdkConfig(BeamNetwork.Testnet, directory.toString()),
            seed = ByteArray(64) { index -> (index + 1).toByte() },
            databaseKey = ByteArray(32) { index -> (index + 33).toByte() },
        )
        try {
            session.start()
            val terminal = withTimeout(180_000) {
                session.state.first { it is BeamWalletState.Ready || it is BeamWalletState.Error }
            }
            assertIs<BeamWalletState.Ready>(terminal)
            val address = session.receiveAddress()
            assertEquals(BeamNetwork.Testnet, address.network)
        } finally {
            session.close()
            directory.deleteTree()
        }
    }
}

private fun Path.deleteTree() {
    Files.walk(this).use { paths ->
        paths.sorted(Comparator.reverseOrder()).forEach(Path::deleteIfExists)
    }
}
