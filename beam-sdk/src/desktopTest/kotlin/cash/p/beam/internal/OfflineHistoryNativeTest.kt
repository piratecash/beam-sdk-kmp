package cash.p.beam.internal

import cash.p.beam.BeamOfflineSigningState
import cash.p.beam.BeamAddressType
import cash.p.beam.BeamOfflineSendState
import cash.p.beam.BeamOfflineSignResult
import cash.p.beam.BeamQuoteRequest
import cash.p.beam.BeamSendAmount
import cash.p.beam.BeamSendContext
import cash.p.beam.BeamSendDeliveryMode
import cash.p.beam.BeamSendResolution
import cash.p.beam.BeamTokenParser
import cash.p.beam.BeamTransactionInspector
import cash.p.beam.BeamNetwork
import cash.p.beam.BeamSdkConfig
import cash.p.beam.BeamTransactionDirection
import cash.p.beam.BeamTransactionStatus
import cash.p.beam.BeamWalletFactory
import cash.p.beam.BeamWalletState
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertContentEquals
import kotlin.test.assertIs
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Assume.assumeTrue

class OfflineHistoryNativeTest {
    @org.junit.Test(timeout = 180_000)
    fun fundedWallet_publicOfflineSendRoundTripExactAndMax() = withFixture { directory ->
        runBlocking {
            withTimeout(150_000) {
                val factory = BeamWalletFactory()
                val receiver = factory.createNew(
                    config(directory.resolve("receiver")), ByteArray(64) { 0x52 }, key(),
                )
                val token = try {
                    assertEquals(BeamWalletState.Stopped, receiver.state.value)
                    receiver.receiveAddress(BeamAddressType.PublicOffline).token
                } finally { receiver.close() }
                assertEquals(BeamAddressType.PublicOffline, BeamTokenParser.parse(token).type)
                val rules = BeamTransactionInspector.supportedRules(BeamNetwork.Testnet)
                for (amount in listOf(BeamSendAmount.Exact(1_000_000), BeamSendAmount.Max)) {
                    val path = directory.resolve(if (amount is BeamSendAmount.Exact) "exact" else "max")
                    val fundedHeight = seedFundedOfflineContext(path.toString())
                    val session = factory.openExisting(config(path), key())
                    val operationId = "00000000-0000-4000-8000-000000000001"
                    val bytes: ByteArray
                    val exported: cash.p.beam.BeamSendOperation
                    try {
                        assertEquals(BeamWalletState.Stopped, session.state.value)
                        val ready = assertIs<BeamOfflineSigningState.Ready>(withTimeout(5_000) {
                            session.offlineSigningState.first { it is BeamOfflineSigningState.Ready }
                        })
                        assertEquals(fundedHeight, ready.height)
                        assertEquals(0L, ready.shieldedCount)
                        val request = BeamQuoteRequest(token, amount, BeamSendContext.Offline(ready.contextId))
                        val quote = session.quoteSend(request)
                        assertEquals(quote, session.quoteSend(request))
                        assertTrue(session.sendOperations().isEmpty(), "Quoting must not reserve an operation")
                        assertEquals(ready.contextId, quote.contextId)
                        assertEquals(rules.signature, quote.rules)
                        assertEquals(BeamAddressType.PublicOffline, quote.receiverType)
                        assertEquals(1, quote.ordinaryInputs)
                        assertEquals(0, quote.shieldedInputs)
                        assertTrue(quote.fee > 0)
                        assertEquals(quote.fee, quote.explicitFee)
                        assertEquals(quote.amount + quote.fee, quote.total)
                        assertEquals(10_000_000L, quote.total + quote.change)
                        if (amount is BeamSendAmount.Exact) {
                            assertEquals(amount.amount, quote.amount)
                            assertTrue(quote.change > 0)
                        } else {
                            assertEquals(0L, quote.change)
                            assertEquals(0L, quote.remainder)
                        }
                        // The public result contains identity/state only; no byte export occurs here.
                        val signed = session.signOffline(operationId, request, quote.version)
                        assertEquals(BeamOfflineSignResult(signed.transactionId, BeamOfflineSendState.Signed), signed)
                        val owned = session.sendOperations().single()
                        assertEquals(operationId, owned.operationId)
                        assertEquals(signed.transactionId, owned.transactionId)
                        assertEquals(BeamSendDeliveryMode.Offline, owned.deliveryMode)
                        assertEquals(BeamOfflineSendState.Signed, owned.offlineState)
                        assertEquals(quote.amount, owned.amount)
                        assertEquals(quote.fee, owned.fee)
                        assertEquals(ready.contextId, owned.contextId)
                        assertEquals(rules.signature, owned.rules)
                        assertFalse(owned.resolution is BeamSendResolution.Submitted)
                        bytes = session.exportSignedTransaction(operationId)
                        val inspected = BeamTransactionInspector.inspect(bytes, rules)
                        assertEquals(bytes.size, inspected.serializedSize)
                        assertEquals(rules, inspected.rules)
                        assertEquals(owned.mainKernelId, inspected.mainKernelId)
                        assertEquals(owned.serializedHash, inspected.serializedTransactionHash)
                        assertEquals(MessageDigest.getInstance("SHA-256").digest(bytes)
                            .joinToString("") { "%02x".format(it) }, inspected.serializedTransactionHash)
                        assertEquals(1, inspected.ordinaryInputCount)
                        assertEquals(0, inspected.shieldedInputCount)
                        assertEquals(if (amount is BeamSendAmount.Exact) 1 else 0, inspected.ordinaryOutputCount)
                        assertEquals(ready.height.toULong(), inspected.mainKernelHeight.minimum)
                        exported = session.sendOperations().single()
                        assertEquals(owned.copy(offlineState = BeamOfflineSendState.Exported), exported)
                        assertContentEquals(bytes, session.exportSignedTransaction(operationId))
                        // Positive JNI handle linkage with valid signed bytes; never invoke run/network.
                        val relay = BeamRelayNative.create(bytes, rules.network.ordinal, rules.signature)
                        try { assertTrue(relay != 0L) } finally { BeamRelayNative.destroy(relay) }
                        session.stop()
                        assertEquals(BeamWalletState.Stopped, session.state.value)
                        assertEquals(exported, session.sendOperations().single())
                    } finally { session.close() }
                    val reopened = factory.openExisting(config(path), key())
                    try {
                        assertEquals(BeamWalletState.Stopped, reopened.state.value)
                        assertEquals(exported, reopened.sendOperations().single())
                        assertFalse(reopened.abortPrepared(operationId), "Export cannot release owned inputs")
                        assertContentEquals(bytes, reopened.exportSignedTransaction(operationId))
                        assertEquals(exported, reopened.sendOperations().single())
                        assertEquals(BeamWalletState.Stopped, reopened.state.value)
                    } finally { reopened.close() }
                }
            }
        }
    }

    @Test
    fun savedOfflineContext_reopensReadyWithoutStartingNetwork() = withFixture { directory ->
        seedOfflineContext(directory.toString())
        runBlocking {
            var previous: BeamOfflineSigningState.Ready? = null
            repeat(2) {
                val backend = createPlatformBackend()
                backend.open(config(directory), key())
                val ready = backend.snapshot.value.offlineSigningState as BeamOfflineSigningState.Ready
                assertEquals(500L, ready.height)
                assertEquals(0L, ready.shieldedCount)
                assertEquals(BackendPhase.Stopped, backend.snapshot.value.phase)
                if (previous != null) assertEquals(previous, ready)
                backend.stop()
                assertEquals(ready, backend.snapshot.value.offlineSigningState)
                backend.close()
                previous = ready
            }
            val session = BeamWalletFactory().openExisting(config(directory), key())
            try {
                val ready = withTimeout(5000) { session.offlineSigningState.first { it is BeamOfflineSigningState.Ready } }
                assertEquals<BeamOfflineSigningState?>(previous, ready)
                assertEquals(BeamWalletState.Stopped, session.state.value)
            } finally { session.close() }
            assertEquals(BeamOfflineSigningState.Unavailable, session.offlineSigningState.value)
        }
    }

    @Test
    fun nativeHistoryMatrix() = withFixture { directory ->
        runNativeMatrix(directory.toString())
    }

    @Test
    fun completedWallet_reopensWithHistoryInPublicFlowAndPagesBeforeStart() = withFixture { directory ->
        seedWallet(directory.toString(), -1, initialized = true, history = true)
        runBlocking {
            repeat(2) {
                val session = BeamWalletFactory().openExisting(config(directory), key())
                try {
                    val page = session.transactionPage(0, 10)
                    assertEquals(2, page.items.size)
                    val transactions = withTimeout(5_000) { session.transactions.first { it.size == 2 } }
                    assertEquals(page.items, transactions)
                    assertEquals(listOf(456L, 123L), transactions.map { it.proofHeight })
                    assertEquals(BeamTransactionDirection.Incoming, transactions[0].direction)
                    assertEquals(BeamTransactionDirection.Outgoing, transactions[1].direction)
                    assertTrue(transactions.all { it.status == BeamTransactionStatus.Completed })
                    assertEquals(listOf(transactions[1]), session.transactionPage(1, 1).items)
                    assertTrue(session.transactionPage(2, 1).items.isEmpty())
                    assertEquals(BeamWalletState.Stopped, session.state.value)
                    assertFalse(session.balance.value.isAuthoritative)
                    session.stop()
                    assertEquals(transactions, session.transactions.value)
                } finally {
                    session.close()
                }
            }
        }
    }

    @Test
    fun failedStart_retainsOpenedHistoryAndNonAuthoritativeBalance() = withFixture { directory ->
        seedWallet(directory.toString(), -1, initialized = true, history = true)
        runBlocking {
            val backend = createPlatformBackend()
            val failingBackend = object : BeamBackend by backend {
                override suspend fun start(): Unit = throw IllegalStateException("Synthetic offline start failure")
            }
            val session = BeamWalletFactory { failingBackend }.openExisting(config(directory), key())
            try {
                val before = withTimeout(5_000) { session.transactions.first { it.size == 2 } }
                assertFailsWith<IllegalStateException> { session.start() }
                assertEquals(before, session.transactions.value)
                assertEquals(before, session.transactionPage(0, 10).items)
                assertEquals(BeamWalletState.Stopped, session.state.value)
                assertFalse(session.balance.value.isAuthoritative)
            } finally {
                session.close()
            }
        }
    }

    @Test
    fun emptyAndUninitializedWallets_doNotPublishHistoryOnOpen() = withFixture { directory ->
        runBlocking {
            for (type in listOf(-1, 0, 1, 2, 3)) {
                val path = directory.resolve("pending-$type")
                seedWallet(path.toString(), type, initialized = false, history = true)
                assertEmptyOnOpen(path)
            }
            val empty = directory.resolve("empty")
            seedWallet(empty.toString(), -1, initialized = true, history = false)
            assertEmptyOnOpen(empty)
        }
    }

    private suspend fun assertEmptyOnOpen(path: Path) {
        val backend = createPlatformBackend()
        val session = BeamWalletFactory { backend }.openExisting(config(path), key())
        try {
            assertTrue(backend.snapshot.value.transactions.isEmpty())
            assertTrue(session.transactions.value.isEmpty())
            assertTrue(session.transactionPage(0, 10).items.isEmpty())
            assertEquals(BeamWalletState.Stopped, session.state.value)
            assertFalse(session.balance.value.isAuthoritative)
        } finally {
            session.close()
        }
    }

    private fun withFixture(block: (Path) -> Unit) {
        val fixture = System.getenv("BEAM_OFFLINE_HISTORY_FIXTURE")
        if (System.getenv("BEAM_EXPECT_OFFLINE_HISTORY_FIXTURE") == "1") {
            check(!fixture.isNullOrBlank()) { "Required offline history JNI fixture was not provided" }
        }
        assumeTrue("Requires the standalone offline history JNI fixture", !fixture.isNullOrBlank())
        NativeLibraryLoader.load()
        System.load(checkNotNull(fixture))
        val directory = Files.createTempDirectory("beam-offline-history-")
        try {
            block(directory)
        } finally {
            Files.walk(directory).use { paths ->
                paths.sorted(Comparator.reverseOrder()).forEach { Files.deleteIfExists(it) }
            }
        }
    }

    private fun config(path: Path) = BeamSdkConfig(BeamNetwork.Testnet, path.toString())
    private fun key() = ByteArray(32) { 0x41 }
    private external fun seedWallet(directory: String, restoreType: Int, initialized: Boolean, history: Boolean)
    private external fun runNativeMatrix(directory: String)
    private external fun seedOfflineContext(directory: String)
    private external fun seedFundedOfflineContext(directory: String): Long
}
