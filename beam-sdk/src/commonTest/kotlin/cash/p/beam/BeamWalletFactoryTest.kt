package cash.p.beam

import cash.p.beam.internal.BackendPhase
import cash.p.beam.internal.BackendSnapshot
import cash.p.beam.internal.BeamBackend
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

class BeamWalletFactoryTest {
    @Test
    fun createNew_invalidSeed_rejectsBeforeNativeCall() = runTest {
        val backend = FakeBackend()

        assertFailsWith<IllegalArgumentException> {
            factory(backend).createNew(config(), ByteArray(63), ByteArray(32))
        }

        assertEquals(0, backend.createCalls)
    }

    @Test
    fun createNew_validSecrets_clearsOnlyInternalCopies() = runTest {
        val backend = FakeBackend()
        val seed = ByteArray(64) { 7 }
        val key = ByteArray(32) { 9 }

        factory(backend).createNew(config(), seed, key)

        assertTrue(backend.seedReference?.all { it == 0.toByte() } == true)
        assertTrue(backend.keyReference?.all { it == 0.toByte() } == true)
        assertTrue(seed.all { it == 7.toByte() })
        assertTrue(key.all { it == 9.toByte() })
    }

    @Test
    fun restore_customSnapshotWithoutTrustedHash_rejectsBeforeNativeCall() = runTest {
        val backend = FakeBackend()

        assertFailsWith<IllegalArgumentException> {
            factory(backend).restore(
                config(),
                ByteArray(64),
                ByteArray(32),
                RestoreSource.SnapshotThenScan(snapshotUrl = "https://mobile-restore.beam.mw/custom.bin"),
            )
        }

        assertEquals(0, backend.createCalls)
    }

    @Test
    fun restore_sourceIsPartOfTheSingleCreateCall() = runTest {
        val backend = FakeBackend()
        val source = RestoreSource.Height(10)

        factory(backend).restore(config(), ByteArray(64), ByteArray(32), source)

        assertEquals(1, backend.createCalls)
        assertEquals(source, backend.createdRestoreSource)
    }

    @Test
    fun session_repeatedStartStopClose_isIdempotent() = runTest {
        val backend = FakeBackend()
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))

        session.start()
        session.start()
        session.stop()
        session.stop()
        session.close()
        session.close()

        assertEquals(1, backend.startCalls)
        assertEquals(1, backend.stopCalls)
        assertEquals(1, backend.closeCalls)
        assertIs<BeamWalletState.Closed>(session.state.value)
    }

    @Test
    fun session_stopMakesCachedBalanceNonAuthoritative() = runTest {
        val backend = FakeBackend()
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))

        session.start()
        session.state.first { it is BeamWalletState.Ready }
        assertTrue(session.balance.value.isAuthoritative)

        session.stop()

        assertIs<BeamWalletState.Stopped>(session.state.value)
        assertFalse(session.balance.value.isAuthoritative)
    }

    @Test
    fun session_cancelledClose_stillReleasesBackendAndReachesClosed() = runTest {
        val closeStarted = CompletableDeferred<Unit>()
        val allowClose = CompletableDeferred<Unit>()
        val backend = FakeBackend(closeStarted = closeStarted, allowClose = allowClose)
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))

        val closing = launch { session.close() }
        closeStarted.await()
        closing.cancel()
        allowClose.complete(Unit)
        closing.join()

        assertEquals(1, backend.closeCalls)
        assertIs<BeamWalletState.Closed>(session.state.value)
        session.close()
        assertEquals(1, backend.closeCalls)
    }

    @Test
    fun session_cancelledStop_stillStopsBackendAndCanRestart() = runTest {
        val stopStarted = CompletableDeferred<Unit>()
        val allowStop = CompletableDeferred<Unit>()
        val backend = FakeBackend(stopStarted = stopStarted, allowStop = allowStop)
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))
        session.start()

        val stopping = launch { session.stop() }
        stopStarted.await()
        stopping.cancel()
        allowStop.complete(Unit)
        stopping.join()

        assertEquals(1, backend.stopCalls)
        assertIs<BeamWalletState.Stopped>(session.state.value)
        session.start()
        assertEquals(2, backend.startCalls)
    }

    @Test
    fun session_stopCancelsAnInFlightStartBeforeReturning() = runTest {
        val startEntered = CompletableDeferred<Unit>()
        val backend = FakeBackend(startEntered = startEntered, suspendStart = true)
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))

        val starting = launch { session.start() }
        startEntered.await()
        session.stop()
        starting.join()

        assertEquals(1, backend.startCalls)
        assertEquals(1, backend.stopCalls)
        assertIs<BeamWalletState.Stopped>(session.state.value)
    }

    @Test
    fun session_cancelledStartAfterBackendActivation_stopsBackend() = runTest {
        val backendActivated = CompletableDeferred<Unit>()
        val allowStartReturn = CompletableDeferred<Unit>()
        val backend = FakeBackend(
            backendActivated = backendActivated,
            allowStartReturn = allowStartReturn,
        )
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))

        val starting = launch { session.start() }
        backendActivated.await()
        starting.cancel()
        allowStartReturn.complete(Unit)
        starting.join()

        assertEquals(1, backend.startCalls)
        assertEquals(1, backend.stopCalls)
        assertIs<BeamWalletState.Stopped>(session.state.value)
    }

    @Test
    fun session_reconciliationDuringStart_rejectsWithoutBlockingStop() = runTest {
        val startEntered = CompletableDeferred<Unit>()
        val backend = FakeBackend(startEntered = startEntered, suspendStart = true)
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))
        val starting = launch { session.start() }
        startEntered.await()

        assertFailsWith<IllegalStateException> { session.resolveSend(OPERATION_ID) }
        assertFailsWith<IllegalStateException> { session.abortPrepared(OPERATION_ID) }
        withContext(Dispatchers.Default.limitedParallelism(1)) {
            withTimeout(5_000) { session.stop() }
        }
        starting.join()

        assertEquals(0, backend.resolveCalls)
        assertEquals(0, backend.abortCalls)
        assertEquals(1, backend.stopCalls)
        assertIs<BeamWalletState.Stopped>(session.state.value)
    }

    @Test
    fun session_readyOnlyOperationsDuringStop_rejectBeforeBackendCalls() = runTest {
        val stopStarted = CompletableDeferred<Unit>()
        val allowStop = CompletableDeferred<Unit>()
        val backend = FakeBackend(stopStarted = stopStarted, allowStop = allowStop)
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))
        session.start()
        session.state.first { it is BeamWalletState.Ready }

        val stopping = launch { session.stop() }
        stopStarted.await()
        assertIs<BeamWalletState.Ready>(session.state.value)

        assertFailsWith<IllegalStateException> { session.receiveAddress() }
        assertFailsWith<IllegalStateException> {
            session.previewSend(BeamSendRequest(receiverToken = "token", amount = 1))
        }
        assertFailsWith<IllegalStateException> {
            session.prepareSend(
                operationId = OPERATION_ID,
                request = BeamSendRequest(receiverToken = "token", amount = 1),
                previewVersion = 1,
            )
        }
        assertFailsWith<IllegalStateException> { session.commitSend(OPERATION_ID) }

        assertEquals(0, backend.receiveAddressCalls)
        assertEquals(0, backend.previewCalls)
        assertEquals(0, backend.prepareCalls)
        assertEquals(0, backend.commitCalls)
        allowStop.complete(Unit)
        stopping.join()
        assertIs<BeamWalletState.Stopped>(session.state.value)
    }

    @Test
    fun previewSend_notReady_rejectsWithoutReservation() = runTest {
        val backend = FakeBackend()
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))

        assertFailsWith<IllegalStateException> {
            session.previewSend(BeamSendRequest(receiverToken = "token", amount = 1))
        }

        assertEquals(0, backend.previewCalls)
    }

    @Test
    fun receiveAddress_stoppedWallet_forwardsToBackend() = runTest {
        val backend = FakeBackend()
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))

        val address = session.receiveAddress()

        assertEquals("token", address.token)
        assertEquals(1, backend.receiveAddressCalls)
    }

    @Test
    fun commitSend_notReady_rejectsBeforeNativeCall() = runTest {
        val backend = FakeBackend()
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))

        assertFailsWith<IllegalStateException> {
            session.commitSend(OPERATION_ID)
        }

        assertEquals(0, backend.commitCalls)
    }

    @Test
    fun prepareSend_readyWallet_forwardsStableOperationId() = runTest {
        val backend = FakeBackend()
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))
        session.start()
        session.state.first { it is BeamWalletState.Ready }

        val prepared = session.prepareSend(
            operationId = OPERATION_ID,
            request = BeamSendRequest(receiverToken = "token", amount = 100),
            previewVersion = 4,
        )

        assertEquals(OPERATION_ID, backend.preparedOperationId)
        assertEquals("tx-1", prepared.transactionId)
    }

    @Test
    fun transactionPage_fullPage_exposesNextOffset() = runTest {
        val backend = FakeBackend(transactionCount = 3)
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))

        val first = session.transactionPage(offset = 0, limit = 2)
        val second = session.transactionPage(offset = requireNotNull(first.nextOffset), limit = 2)

        assertEquals(listOf("tx-0", "tx-1"), first.items.map { it.id })
        assertEquals(2, first.nextOffset)
        assertEquals(listOf("tx-2"), second.items.map { it.id })
        assertEquals(null, second.nextOffset)
    }

    private fun factory(backend: FakeBackend) = BeamWalletFactory { backend }

    private fun config() = BeamSdkConfig(BeamNetwork.Mainnet, storagePath = "wallet")

    private companion object {
        const val OPERATION_ID = "123e4567-e89b-42d3-a456-426614174000"
    }
}

private class FakeBackend(
    transactionCount: Int = 0,
    private val closeStarted: CompletableDeferred<Unit>? = null,
    private val allowClose: CompletableDeferred<Unit>? = null,
    private val stopStarted: CompletableDeferred<Unit>? = null,
    private val allowStop: CompletableDeferred<Unit>? = null,
    private val startEntered: CompletableDeferred<Unit>? = null,
    private val suspendStart: Boolean = false,
    private val backendActivated: CompletableDeferred<Unit>? = null,
    private val allowStartReturn: CompletableDeferred<Unit>? = null,
) : BeamBackend {
    private val mutableSnapshot = MutableStateFlow(BackendSnapshot())
    private val transactionItems = List(transactionCount) { index -> transaction("tx-$index") }

    override val snapshot: StateFlow<BackendSnapshot> = mutableSnapshot
    var createCalls = 0
    var startCalls = 0
    var stopCalls = 0
    var closeCalls = 0
    var previewCalls = 0
    var receiveAddressCalls = 0
    var commitCalls = 0
    var prepareCalls = 0
    var resolveCalls = 0
    var abortCalls = 0
    var seedReference: ByteArray? = null
    var keyReference: ByteArray? = null
    var createdRestoreSource: RestoreSource? = null
    var preparedOperationId: String? = null

    override suspend fun create(
        config: BeamSdkConfig,
        seed: ByteArray,
        databaseKey: ByteArray,
        restoreSource: RestoreSource?,
    ) {
        createCalls++
        seedReference = seed
        keyReference = databaseKey
        createdRestoreSource = restoreSource
    }

    override suspend fun open(config: BeamSdkConfig, databaseKey: ByteArray) = Unit

    override suspend fun start() {
        startCalls++
        startEntered?.complete(Unit)
        if (suspendStart) CompletableDeferred<Unit>().await()
        backendActivated?.complete(Unit)
        allowStartReturn?.let { completion ->
            withContext(NonCancellable) { completion.await() }
        }
        mutableSnapshot.value = BackendSnapshot(
            phase = BackendPhase.Ready,
            balance = BeamBalance(isAuthoritative = true),
        )
    }

    override suspend fun stop() {
        stopCalls++
        stopStarted?.complete(Unit)
        allowStop?.await()
    }

    override suspend fun close() {
        closeCalls++
        closeStarted?.complete(Unit)
        allowClose?.await()
    }

    override suspend fun receiveAddress(type: BeamAddressType): BeamAddress {
        receiveAddressCalls++
        return BeamAddress(
            token = "token",
            type = type,
            network = BeamNetwork.Mainnet,
        )
    }

    override suspend fun transactions(offset: Int, limit: Int): List<BeamTransaction> =
        transactionItems.drop(offset).take(limit)

    override suspend fun previewSend(request: BeamSendRequest): BeamSendPreview {
        previewCalls++
        return BeamSendPreview("hash", 1, request.amount, 10, request.amount + 10, BeamAddressType.Offline)
    }

    override suspend fun prepareSend(
        operationId: String,
        request: BeamSendRequest,
        previewVersion: Long,
    ): PreparedBeamSend {
        prepareCalls++
        preparedOperationId = operationId
        return PreparedBeamSend(operationId, "tx-1")
    }

    override suspend fun commitSend(operationId: String): BeamSendResolution {
        commitCalls++
        return BeamSendResolution.Submitted("tx-1")
    }

    override suspend fun resolveSend(operationId: String): BeamSendResolution {
        resolveCalls++
        return BeamSendResolution.Prepared("tx-1")
    }

    override suspend fun abortPrepared(operationId: String): Boolean {
        abortCalls++
        return true
    }
}

private fun transaction(id: String) = BeamTransaction(
    id = id,
    direction = BeamTransactionDirection.Outgoing,
    amount = 1,
    fee = 1,
    createdAtEpochSeconds = 1,
    minHeight = null,
    proofHeight = null,
    kernelId = null,
    status = BeamTransactionStatus.Pending,
    failureReason = null,
)
