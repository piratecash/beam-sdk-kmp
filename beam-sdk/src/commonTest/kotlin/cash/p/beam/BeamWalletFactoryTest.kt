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
import kotlin.test.assertNull
import kotlin.test.assertTrue

class BeamWalletFactoryTest {
    @Test
    fun offlineSigningState_describesSavedContextAcrossStopAndInvalidation() = runTest {
        val backend = FakeBackend()
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))
        assertEquals(BeamOfflineSigningState.Unavailable, session.offlineSigningState.value)
        val ready = BeamOfflineSigningState.Ready("7:context", 500, 200000)
        backend.emitContext(ready)
        session.offlineSigningState.first { it == ready }
        assertIs<BeamWalletState.Stopped>(session.state.value)
        session.start()
        backend.emitContext(ready)
        session.offlineSigningState.first { it == ready }
        session.stop()
        assertEquals(ready, session.offlineSigningState.value)
        backend.emitContext(BeamOfflineSigningState.Invalidated)
        session.offlineSigningState.first { it == BeamOfflineSigningState.Invalidated }
        backend.emitContext(BeamOfflineSigningState.Preparing)
        session.offlineSigningState.first { it == BeamOfflineSigningState.Preparing }
        session.close()
        assertEquals(BeamOfflineSigningState.Unavailable, session.offlineSigningState.value)
    }

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

        session.awaitStopped()
        assertFalse(session.balance.value.isAuthoritative)
    }

    @Test
    fun session_stopAndRestart_keepTheAmountsTheDatabaseReported() = runTest {
        // The backend publishes no stopped snapshot here, so the session's own cached-balance
        // copies are the only thing that can still be reporting the amounts after the assertions.
        val backend = FakeBackend()
        backend.publishSnapshotOnStop = false
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))

        session.start()
        session.state.first { it is BeamWalletState.Ready }
        backend.emitBalance(
            BeamBalance(available = 4200, receiving = 7, loadedFromDatabase = true, isAuthoritative = true)
        )
        session.balance.first { it.available == 4200L }

        session.stop()
        session.awaitStopped()

        // A stop withdraws the right to spend, not the amounts: the balance row keeps showing them.
        assertEquals(4200, session.balance.value.available)
        assertEquals(7, session.balance.value.receiving)
        assertTrue(session.balance.value.isLoaded)
        assertFalse(session.balance.value.isAuthoritative)

        // The start path clears authority on the same cached balance and must keep the amounts too.
        val restartEntered = CompletableDeferred<Unit>()
        backend.startEntered = restartEntered
        backend.suspendStart = true
        val restart = launch { session.start() }
        // The backend is entered only after start() has taken the mutex, moved to Connecting and
        // rewritten the cached balance, so this is a deterministic observation of that copy.
        restartEntered.await()
        assertIs<BeamWalletState.Connecting>(session.state.value)

        assertEquals(4200, session.balance.value.available)
        assertEquals(7, session.balance.value.receiving)
        assertTrue(session.balance.value.isLoaded)
        assertFalse(session.balance.value.isAuthoritative)
        restart.cancel()
        restart.join()
    }

    @Test
    fun session_cancelledStart_keepsTheAmountsTheDatabaseReported() = runTest {
        val startEntered = CompletableDeferred<Unit>()
        val backend = FakeBackend(startEntered = startEntered, suspendStart = true)
        backend.publishSnapshotOnStop = false
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))

        // What the wallet database reported before the node was ever contacted.
        backend.emitBalance(BeamBalance(available = 4200, receiving = 7, loadedFromDatabase = true))
        session.balance.first { it.available == 4200L }

        val attempt = launch { session.start() }
        startEntered.await()
        assertEquals(4200, session.balance.value.available)
        assertTrue(session.balance.value.isLoaded)
        assertFalse(session.balance.value.isAuthoritative)

        // Cancelling a start that never reached the node runs the cleanup path, which stops the
        // backend and rewrites the cached balance; it must not drop the amounts either.
        attempt.cancel()
        attempt.join()
        session.awaitStopped()

        assertEquals(1, backend.stopCalls)
        assertEquals(4200, session.balance.value.available)
        assertEquals(7, session.balance.value.receiving)
        assertTrue(session.balance.value.isLoaded)
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
        session.awaitStopped()
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
        session.awaitStopped()
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
        session.awaitStopped()
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
        assertFailsWith<IllegalStateException> { session.sendOperations() }
        assertFailsWith<IllegalStateException> { session.recoverSendOperations() }
        withContext(Dispatchers.Default.limitedParallelism(1)) {
            withTimeout(5_000) { session.stop() }
        }
        starting.join()

        assertEquals(0, backend.resolveCalls)
        assertEquals(0, backend.abortCalls)
        assertEquals(1, backend.stopCalls)
        session.awaitStopped()
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
        assertFailsWith<IllegalStateException> { session.sendOperations() }
        assertFailsWith<IllegalStateException> { session.recoverSendOperations() }

        assertEquals(0, backend.receiveAddressCalls)
        assertEquals(0, backend.previewCalls)
        assertEquals(0, backend.prepareCalls)
        assertEquals(0, backend.commitCalls)
        allowStop.complete(Unit)
        stopping.join()
        session.awaitStopped()
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
    fun syncingState_carriesScanCounters() = runTest {
        // The height pair follows the chain tip, so it can sit still or grow while blocks are
        // being scanned. syncDone/syncTotal are the only numbers that actually advance, and the
        // public state has to carry them for a progress indicator to mean anything.
        val backend = FakeBackend()
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))

        backend.emitSyncing(currentHeight = 100, targetHeight = 200, syncDone = 1173, syncTotal = 1226)

        val state = session.state.first { it is BeamWalletState.Syncing } as BeamWalletState.Syncing
        assertEquals(100, state.currentHeight)
        assertEquals(200, state.targetHeight)
        assertEquals(1173, state.syncDone)
        assertEquals(1226, state.syncTotal)
    }

    @Test
    fun syncingState_withoutScanCounters_keepsThemNull() = runTest {
        // An older native library reports no counters. The state must stay usable rather than
        // inventing a number, so the consumer can fall back to the height ratio.
        val backend = FakeBackend()
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))

        backend.emitSyncing(currentHeight = 100, targetHeight = 200, syncDone = null, syncTotal = null)

        val state = session.state.first { it is BeamWalletState.Syncing } as BeamWalletState.Syncing
        assertEquals(100, state.currentHeight)
        assertEquals(200, state.targetHeight)
        assertNull(state.syncDone)
        assertNull(state.syncTotal)
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
    fun offlineSigningDoesNotStartOrCommitAndExportIsExplicit() = runTest {
        val backend = FakeBackend()
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))
        val request = BeamQuoteRequest("token", BeamSendAmount.Max, BeamSendContext.Offline("context"))
        val quote = session.quoteSend(request)
        assertEquals(BeamOfflineSendState.Signed, session.signOffline(OPERATION_ID, request, quote.version).state)
        assertEquals(0, backend.startCalls)
        assertEquals(0, backend.prepareCalls)
        assertEquals(0, backend.commitCalls)
        assertEquals(0, backend.exportCalls)
        assertTrue(session.exportSignedTransaction(OPERATION_ID).contentEquals(byteArrayOf(1, 2, 3)))
    }

    @Test
    fun offlineSigningRejectsRunningOwnerAndBoundsBeforeBackend() = runTest {
        val backend = FakeBackend()
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))
        val request = BeamQuoteRequest("token", BeamSendAmount.Exact(1), BeamSendContext.Offline("context"))
        assertFailsWith<IllegalArgumentException> { session.quoteSend(request.copy(receiverToken = "x".repeat(65_537))) }
        assertEquals(0, backend.quoteCalls)
        session.start()
        assertFailsWith<BeamFailure.SendBusy> { session.signOffline(OPERATION_ID, request, "a".repeat(64)) }
        assertEquals(0, backend.signCalls)
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

    @Test
    fun sendInventory_isLocalAndRecoveryRequiresReady() = runTest {
        val backend = FakeBackend()
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))
        assertTrue(session.sendOperations().isEmpty())
        assertFailsWith<IllegalStateException> { session.recoverSendOperations() }
        assertEquals(0, backend.recoveryCalls)
        assertEquals(0, backend.startCalls)

        session.start()
        session.state.first { it is BeamWalletState.Ready }
        session.prepareSend(OPERATION_ID, BeamSendRequest("token", 100), 1)
        val prepared = session.sendOperations().single()
        assertEquals(OPERATION_ID, prepared.operationId)
        assertEquals(BeamSendResolution.Prepared("tx-1"), prepared.resolution)
        assertEquals(BeamSendResolution.Submitted("tx-1"), session.recoverSendOperations().single().resolution)
        assertEquals(1, backend.recoveryCalls)
        session.stop()
        assertEquals(prepared.transactionId, session.sendOperations().single().transactionId)
        assertFailsWith<IllegalStateException> { session.recoverSendOperations() }
        session.close()
        assertFailsWith<IllegalStateException> { session.sendOperations() }
        assertFailsWith<IllegalStateException> { session.recoverSendOperations() }
    }

    @Test
    fun acceptedPrepare_cancelledResponseRemainsDiscoverableInOriginatingSession() = runTest {
        val accepted = CompletableDeferred<Unit>()
        val backend = FakeBackend(prepareAccepted = accepted, losePrepareResponse = true)
        val session = factory(backend).createNew(config(), ByteArray(64), ByteArray(32))
        val other = factory(FakeBackend()).createNew(config().copy(storagePath = "other"), ByteArray(64), ByteArray(32))
        session.start()
        session.state.first { it is BeamWalletState.Ready }
        val preparing = launch { session.prepareSend(OPERATION_ID, BeamSendRequest("token", 100), 1) }
        accepted.await()
        preparing.cancel()
        preparing.join()
        assertEquals(OPERATION_ID, session.sendOperations().single().operationId)
        assertTrue(other.sendOperations().isEmpty())
        assertEquals(BeamSendResolution.Submitted("tx-1"), session.recoverSendOperations().single().resolution)
        assertEquals(1, backend.prepareCalls)
        session.close()
        other.close()
    }

    private fun factory(backend: FakeBackend) = BeamWalletFactory { backend }

    private fun config() = BeamSdkConfig(BeamNetwork.Mainnet, storagePath = "wallet")

    private companion object {
        const val OPERATION_ID = "123e4567-e89b-42d3-a456-426614174000"
    }
}

// The session applies backend snapshots on its own Dispatchers.Default scope, so right after
// stop() returns a snapshot collected earlier may still be in flight. The contract is eventual:
// the last published snapshot (Stopped) wins, which is what this waits for.
private suspend fun BeamWalletSession.awaitStopped() {
    assertIs<BeamWalletState.Stopped>(state.first { it is BeamWalletState.Stopped })
}

private class FakeBackend(
    transactionCount: Int = 0,
    private val prepareAccepted: CompletableDeferred<Unit>? = null,
    private val losePrepareResponse: Boolean = false,
    private val closeStarted: CompletableDeferred<Unit>? = null,
    private val allowClose: CompletableDeferred<Unit>? = null,
    private val stopStarted: CompletableDeferred<Unit>? = null,
    private val allowStop: CompletableDeferred<Unit>? = null,
    var startEntered: CompletableDeferred<Unit>? = null,
    var suspendStart: Boolean = false,
    private val backendActivated: CompletableDeferred<Unit>? = null,
    private val allowStartReturn: CompletableDeferred<Unit>? = null,
) : BeamBackend {
    private val mutableSnapshot = MutableStateFlow(BackendSnapshot())
    private val transactionItems = List(transactionCount) { index -> transaction("tx-$index") }

    // The JNI backend publishes its stopped snapshot only after the native read succeeds, so a
    // failing read leaves the session's own cached balance as the single source of the amounts.
    // Tests that pin those copies turn the republish off to remove the competing source.
    var publishSnapshotOnStop: Boolean = true

    override val snapshot: StateFlow<BackendSnapshot> = mutableSnapshot
    fun emitContext(value: BeamOfflineSigningState) {
        mutableSnapshot.value = mutableSnapshot.value.copy(offlineSigningState = value)
    }
    fun emitBalance(value: BeamBalance) {
        mutableSnapshot.value = mutableSnapshot.value.copy(balance = value)
    }
    fun emitSyncing(currentHeight: Long, targetHeight: Long, syncDone: Long?, syncTotal: Long?) {
        mutableSnapshot.value = mutableSnapshot.value.copy(
            phase = BackendPhase.Syncing,
            currentHeight = currentHeight,
            targetHeight = targetHeight,
            syncDone = syncDone,
            syncTotal = syncTotal,
        )
    }
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
    var recoveryCalls = 0
    private var operations = emptyList<BeamSendOperation>()
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
        if (!publishSnapshotOnStop) return
        // Mirror the JNI backend: a stop publishes a Stopped snapshot, so a Ready snapshot that
        // was still being collected on the session scope is always followed by Stopped.
        mutableSnapshot.value = mutableSnapshot.value.copy(
            phase = BackendPhase.Stopped,
            balance = mutableSnapshot.value.balance.copy(isAuthoritative = false),
        )
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

    var quoteCalls = 0
    var signCalls = 0
    var exportCalls = 0
    override suspend fun quoteSend(request: BeamQuoteRequest): BeamSendQuote {
        quoteCalls++
        return BeamSendQuote(100, 10, 110, 10, 0, 0, 1, 0, BeamAddressType.PublicOffline,
            "a".repeat(64), (request.context as? BeamSendContext.Offline)?.contextId.orEmpty(), "rules")
    }
    override suspend fun signOffline(operationId: String, request: BeamQuoteRequest, quoteVersion: String): BeamOfflineSignResult {
        signCalls++
        return BeamOfflineSignResult("tx-offline", BeamOfflineSendState.Signed)
    }
    override suspend fun exportSignedTransaction(operationId: String): ByteArray {
        exportCalls++
        return byteArrayOf(1, 2, 3)
    }

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
        operations = listOf(BeamSendOperation(operationId, "tx-1", "hash", request.amount, 10,
            BeamSendResolution.Prepared("tx-1")))
        prepareAccepted?.complete(Unit)
        if (losePrepareResponse) CompletableDeferred<Unit>().await()
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

    override suspend fun sendOperations(): List<BeamSendOperation> = operations
    override suspend fun recoverSendOperations(): List<BeamSendOperation> {
        recoveryCalls++
        operations = operations.map { it.copy(resolution = BeamSendResolution.Submitted(it.transactionId)) }
        return operations
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
