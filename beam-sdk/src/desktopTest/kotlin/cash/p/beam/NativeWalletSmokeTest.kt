package cash.p.beam

import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.deleteIfExists
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

class NativeWalletSmokeTest {
    @Test
    fun encryptedWallet_createReopenRejectWrongKeyAndEnforceExclusiveOwner() = runBlocking {
        val directory = Files.createTempDirectory("beam-sdk-wallet-smoke-")
        val walletDirectory = directory.resolve("кошелёк-🚀")
        val seed = ByteArray(64) { index -> (index + 1).toByte() }
        val key = ByteArray(32) { index -> (0xa0 + index).toByte() }
        val config = BeamSdkConfig(BeamNetwork.Testnet, walletDirectory.toString(), BeamLogLevel.None)

        try {
            withTimeout(30_000) {
                BeamWalletFactory().createNew(config, seed, key).useSession {
                    val address = it.receiveAddress(BeamAddressType.PublicOffline)
                    assertEquals(BeamAddressType.PublicOffline, address.type)
                    assertEquals(BeamNetwork.Testnet, address.network)
                    check(address.token.isNotBlank())
                    assertFailsWith<BeamFailure.Validation> {
                        BeamWalletFactory().openExisting(config, key)
                    }
                    assertFailsWith<BeamFailure.Validation> {
                        BeamWalletFactory().createNew(
                            config.copy(storagePath = directory.resolve("same-owner").toString()),
                            seed,
                            key,
                        )
                    }
                }
            }

            assertFailsWith<BeamFailure.Storage> {
                BeamWalletFactory().openExisting(config, ByteArray(32) { 7 })
            }

            withTimeout(30_000) {
                BeamWalletFactory().openExisting(config, key).useSession { }
            }

            val restoreConfig = config.copy(storagePath = directory.resolve("restore-height").toString())
            withTimeout(30_000) {
                BeamWalletFactory().restore(
                    restoreConfig,
                    seed,
                    key,
                    RestoreSource.Height(123),
                ).useSession { }
                BeamWalletFactory().openExisting(restoreConfig, key).useSession { }
            }
        } finally {
            seed.fill(0)
            key.fill(0)
            directory.deleteRecursively()
        }
    }
}

private suspend fun <T> BeamWalletSession.useSession(block: suspend (BeamWalletSession) -> T): T =
    try {
        block(this)
    } finally {
        close()
    }

private fun Path.deleteRecursively() {
    Files.walk(this).use { paths ->
        paths.sorted(Comparator.reverseOrder()).forEach(Path::deleteIfExists)
    }
}
