package cash.p.beam.sample

import cash.p.beam.BeamAddress
import cash.p.beam.BeamAddressType
import cash.p.beam.BeamBalance
import cash.p.beam.BeamNetwork
import cash.p.beam.BeamSdkConfig
import cash.p.beam.BeamSendPreview
import cash.p.beam.BeamSendRequest
import cash.p.beam.BeamSendResolution
import cash.p.beam.BeamTransaction
import cash.p.beam.BeamTransactionPage
import cash.p.beam.BeamWalletSession
import cash.p.beam.BeamWalletState
import cash.p.beam.PreparedBeamSend
import cash.p.beam.RestoreSource
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout

class BeamDemoControllerTest {
    @Test
    fun openOrCreate_opensAndStartsExistingWallet() = runBlocking {
        val directory = createTempDirectory("beam-demo-auto-open")
        val session = FakeDemoSession(blockFirstStart = false)
        val factory = FakeDemoWalletFactory(session)
        val controller = BeamDemoController(
            initialStoragePath = directory.toString(),
            walletFactory = factory,
            walletExists = { true },
        )
        try {
            controller.edit { copy(databaseKeyHex = "02".repeat(32)) }

            controller.openOrCreate()

            withTimeout(5_000) { controller.state.first { it.walletState is BeamWalletState.Ready && !it.busy } }
            assertEquals(0, factory.createCalls)
            assertEquals(1, factory.openCalls)
            assertEquals(1, session.startCalls)
            assertEquals(listOf("receive:PublicOffline", "start"), session.lifecycleEvents)
            assertEquals(BeamAddressType.PublicOffline, controller.state.value.address?.type)
        } finally {
            controller.close()
            deleteTree(directory)
        }
    }

    @Test
    fun openOrCreate_createsAndStartsWalletWhenDatabaseIsMissing() = runBlocking {
        val directory = createTempDirectory("beam-demo-auto-create")
        val session = FakeDemoSession(blockFirstStart = false)
        val factory = FakeDemoWalletFactory(session)
        val controller = BeamDemoController(
            initialStoragePath = directory.toString(),
            walletFactory = factory,
            walletExists = { false },
        )
        try {
            controller.edit {
                copy(seedHex = "01".repeat(64), databaseKeyHex = "02".repeat(32))
            }

            controller.openOrCreate()

            withTimeout(5_000) { controller.state.first { it.walletState is BeamWalletState.Ready && !it.busy } }
            assertEquals(1, factory.createCalls)
            assertEquals(0, factory.openCalls)
            assertEquals(1, session.startCalls)
            assertEquals(listOf("receive:PublicOffline", "start"), session.lifecycleEvents)
            assertEquals(BeamAddressType.PublicOffline, controller.state.value.address?.type)
        } finally {
            controller.close()
            deleteTree(directory)
        }
    }

    @Test
    fun demoHandle_initializesOnlyOnceAcrossCompositionRecreation() = runBlocking {
        val directory = createTempDirectory("beam-demo-handle")
        val session = FakeDemoSession(blockFirstStart = false)
        val factory = FakeDemoWalletFactory(session)
        val controller = BeamDemoController(
            initialStoragePath = directory.toString(),
            walletFactory = factory,
            walletExists = { false },
        )
        val handle = BeamDemoHandle(controller)
        try {
            controller.edit { copy(seedHex = "01".repeat(64), databaseKeyHex = "02".repeat(32)) }

            handle.initializeOnce()
            handle.initializeOnce()

            withTimeout(5_000) { controller.state.first { it.walletState is BeamWalletState.Ready && !it.busy } }
            assertEquals(1, factory.createCalls)
            assertEquals(1, session.startCalls)
        } finally {
            handle.closeAndJoin()
            deleteTree(directory)
        }
    }

    @Test
    fun manualStart_waitsForInFlightManualStop() = runBlocking {
        val directory = createTempDirectory("beam-demo-stop-start")
        val session = FakeDemoSession(blockFirstStart = false, blockFirstStop = true)
        val controller = BeamDemoController(directory.toString(), FakeDemoWalletFactory(session))
        try {
            controller.edit { copy(seedHex = "01".repeat(64), databaseKeyHex = "02".repeat(32)) }
            controller.create()
            withTimeout(5_000) { controller.state.first { it.walletState is BeamWalletState.Ready && !it.busy } }

            controller.stop()
            withTimeout(5_000) { session.firstStopEntered.await() }
            controller.start()
            delay(50)
            assertEquals(1, session.startCalls)

            session.allowFirstStop.complete(Unit)
            withTimeout(5_000) {
                while (session.startCalls != 2) delay(10)
            }
            assertEquals(listOf("start", "stop", "start"), session.lifecycleEvents.filter { it != "receive:PublicOffline" })
        } finally {
            session.allowFirstStop.complete(Unit)
            controller.closeAndJoin()
            deleteTree(directory)
        }
    }

    @Test
    fun heightRecovery_usesDedicatedDatabaseAndClosesPreviousWallet() = runBlocking {
        val directory = createTempDirectory("beam-demo-recovery")
        val baseSession = FakeDemoSession(blockFirstStart = false)
        val restoredSession = FakeDemoSession(blockFirstStart = false)
        val factory = FakeDemoWalletFactory(baseSession, restoredSession = restoredSession)
        val expectedRecoveryPath = directory.resolve("recovery").resolve("height-12345").normalize()
        val controller = BeamDemoController(
            initialStoragePath = directory.toString(),
            walletFactory = factory,
            walletExists = { false },
        )
        try {
            controller.edit { copy(seedHex = "01".repeat(64), databaseKeyHex = "02".repeat(32)) }
            controller.create()
            withTimeout(5_000) { controller.state.first { it.walletState is BeamWalletState.Ready && !it.busy } }

            controller.edit { copy(restoreHeight = "12345") }
            controller.restoreHeight()

            val recovered = withTimeout(5_000) {
                controller.state.first {
                    it.activeWallet?.storagePath?.let { path -> Paths.get(path).normalize() } == expectedRecoveryPath &&
                        !it.busy
                }
            }
            assertEquals(1, factory.restoreCalls)
            assertEquals(RestoreSource.Height(12345), factory.lastRestoreSource)
            assertEquals(
                expectedRecoveryPath,
                factory.lastRestoreConfig?.storagePath?.let { path -> Paths.get(path).normalize() },
            )
            assertTrue(recovered.activeWallet?.storagePath != directory.toString())
            assertEquals(1, baseSession.closeCalls)
            assertEquals(1, restoredSession.startCalls)
        } finally {
            controller.closeAndJoin()
            deleteTree(directory)
        }
    }

    @Test
    fun failedRecovery_reopensPreviousWallet() = runBlocking {
        val directory = createTempDirectory("beam-demo-recovery-rollback")
        val baseSession = FakeDemoSession(blockFirstStart = false)
        val factory = FakeDemoWalletFactory(
            session = baseSession,
            restoreFailure = IllegalStateException("restore failed"),
        )
        val controller = BeamDemoController(
            initialStoragePath = directory.toString(),
            walletFactory = factory,
            walletExists = { false },
        )
        try {
            controller.edit { copy(seedHex = "01".repeat(64), databaseKeyHex = "02".repeat(32)) }
            controller.create()
            withTimeout(5_000) { controller.state.first { it.walletState is BeamWalletState.Ready && !it.busy } }

            controller.edit { copy(restoreHeight = "12345") }
            controller.restoreHeight()

            val rolledBack = withTimeout(5_000) {
                controller.state.first { it.message == "restore failed" && !it.busy }
            }
            assertEquals(directory.toString(), rolledBack.activeWallet?.storagePath)
            assertEquals(1, factory.restoreCalls)
            assertEquals(1, factory.openCalls)
            assertEquals(1, baseSession.closeCalls)
            assertEquals(2, baseSession.startCalls)
        } finally {
            controller.closeAndJoin()
            deleteTree(directory)
        }
    }

    @Test
    fun invalidRecoveryInput_keepsCurrentWalletOpen() = runBlocking {
        val directory = createTempDirectory("beam-demo-invalid-recovery")
        val session = FakeDemoSession(blockFirstStart = false)
        val factory = FakeDemoWalletFactory(session)
        val controller = BeamDemoController(
            initialStoragePath = directory.toString(),
            walletFactory = factory,
            walletExists = { false },
        )
        try {
            controller.edit { copy(seedHex = "01".repeat(64), databaseKeyHex = "02".repeat(32)) }
            controller.create()
            withTimeout(5_000) { controller.state.first { it.walletState is BeamWalletState.Ready && !it.busy } }

            controller.edit { copy(restoreHeight = "invalid") }
            controller.restoreHeight()

            val unchanged = withTimeout(5_000) {
                controller.state.first { it.message == "Enter a valid restore height" && !it.busy }
            }
            assertEquals(directory.toString(), unchanged.activeWallet?.storagePath)
            assertEquals(0, session.closeCalls)
            assertEquals(0, factory.restoreCalls)
        } finally {
            controller.closeAndJoin()
            deleteTree(directory)
        }
    }

    @Test
    fun invalidSnapshotConfiguration_keepsCurrentWalletOpenAndSkipsDownload() = runBlocking {
        val directory = createTempDirectory("beam-demo-invalid-snapshot")
        val session = FakeDemoSession(blockFirstStart = false)
        val factory = FakeDemoWalletFactory(session)
        val controller = BeamDemoController(
            initialStoragePath = directory.toString(),
            walletFactory = factory,
            walletExists = { false },
        )
        try {
            controller.edit { copy(seedHex = "01".repeat(64), databaseKeyHex = "02".repeat(32)) }
            controller.create()
            withTimeout(5_000) { controller.state.first { it.walletState is BeamWalletState.Ready && !it.busy } }

            controller.edit {
                copy(snapshotUrl = "https://mobile-restore.beam.mw/custom.bin", snapshotSha256 = "")
            }
            controller.restoreSnapshot()
            val missingHash = withTimeout(5_000) {
                controller.state.first {
                    it.message == "A custom snapshot URL requires a trusted SHA-256" && !it.busy
                }
            }
            assertEquals(directory.toString(), missingHash.activeWallet?.storagePath)

            controller.edit { copy(snapshotSha256 = "not-a-sha-256") }
            controller.restoreSnapshot()
            val invalidHash = withTimeout(5_000) {
                controller.state.first {
                    it.message == "Trusted snapshot SHA-256 must contain 64 hexadecimal characters" && !it.busy
                }
            }
            assertEquals(directory.toString(), invalidHash.activeWallet?.storagePath)
            assertEquals(0, session.closeCalls)
            assertEquals(0, factory.restoreCalls)
        } finally {
            controller.closeAndJoin()
            deleteTree(directory)
        }
    }

    @Test
    fun activeIdentityOwnsNetworkJournalAndSnapshotCancellation() = runBlocking {
        val activeDirectory = createTempDirectory("beam-demo-active")
        val editedDirectory = createTempDirectory("beam-demo-edited")
        val session = FakeDemoSession()
        val controller = BeamDemoController(activeDirectory.toString(), FakeDemoWalletFactory(session))
        try {
            controller.edit {
                copy(
                    seedHex = "01".repeat(64),
                    databaseKeyHex = "02".repeat(32),
                    receiver = "receiver",
                    amount = "100",
                )
            }
            controller.create()

            val downloading = withTimeout(5_000) {
                controller.state.first {
                    it.activeWallet != null &&
                        it.walletState is BeamWalletState.Restoring &&
                        it.busy
                }
            }
            assertEquals(
                ActiveWalletIdentity(BeamNetwork.Testnet, activeDirectory.toString()),
                downloading.activeWallet,
            )

            controller.stop()
            withTimeout(5_000) { controller.state.first { it.walletState is BeamWalletState.Stopped } }
            assertEquals(1, session.stopCalls)

            controller.start()
            withTimeout(5_000) { controller.state.first { it.walletState is BeamWalletState.Ready && !it.busy } }
            controller.onHostBackground()
            withTimeout(5_000) {
                while (
                    session.stopCalls < 2 ||
                    controller.state.value.walletState !is BeamWalletState.Stopped
                ) delay(10)
            }
            controller.onHostForeground()
            withTimeout(5_000) {
                while (session.startCalls < 3) delay(10)
            }
            withTimeout(5_000) { controller.state.first { it.walletState is BeamWalletState.Ready && !it.busy } }
            controller.edit { copy(storagePath = editedDirectory.toString()) }
            controller.toggleNetwork()
            controller.receiveAddress()
            withTimeout(5_000) { controller.state.first { it.address != null && !it.busy } }
            controller.previewSend()
            withTimeout(5_000) { controller.state.first { it.preview != null && !it.busy } }
            controller.confirmSend()
            withTimeout(5_000) { controller.state.first { it.sendResolution is BeamSendResolution.Submitted } }

            val finalState = controller.state.value
            assertEquals(BeamNetwork.Mainnet, finalState.network)
            assertEquals(editedDirectory.toString(), finalState.storagePath)
            assertEquals(
                ActiveWalletIdentity(BeamNetwork.Testnet, activeDirectory.toString()),
                finalState.activeWallet,
            )
            assertEquals(BeamNetwork.Testnet, finalState.address?.network)
            val pending = assertNotNull(
                DemoSendJournal.load(activeDirectory.toString(), BeamNetwork.Testnet),
            )
            assertEquals(activeDirectory.toString(), pending.owner.storagePath)
            assertNull(DemoSendJournal.load(editedDirectory.toString(), BeamNetwork.Testnet))
            assertEquals(2, session.stopCalls)
            assertEquals(3, session.startCalls)
        } finally {
            DemoSendJournal.load(activeDirectory.toString(), BeamNetwork.Testnet)?.let { pending ->
                DemoSendJournal.clear(activeDirectory.toString(), pending.owner, pending.operationId)
            }
            controller.close()
            deleteTree(activeDirectory)
            deleteTree(editedDirectory)
        }
    }

    @Test
    fun prepareFailure_reconcilesClaimAndClearsAuthoritativeNotPrepared() = runBlocking {
        val directory = createTempDirectory("beam-demo-prepare-failure")
        val session = FakeDemoSession(
            blockFirstStart = false,
            prepareFailure = IllegalStateException("wallet stopped before preparation"),
            resolvedSend = BeamSendResolution.NotPrepared,
        )
        val controller = BeamDemoController(directory.toString(), FakeDemoWalletFactory(session))
        try {
            controller.edit {
                copy(
                    seedHex = "01".repeat(64),
                    databaseKeyHex = "02".repeat(32),
                    receiver = "receiver",
                    amount = "100",
                )
            }
            controller.create()
            withTimeout(5_000) { controller.state.first { it.walletState is BeamWalletState.Ready && !it.busy } }
            controller.previewSend()
            withTimeout(5_000) { controller.state.first { it.preview != null && !it.busy } }

            controller.confirmSend()
            withTimeout(5_000) {
                controller.state.first { it.sendResolution == BeamSendResolution.NotPrepared }
            }
            withTimeout(5_000) {
                while (DemoSendJournal.load(directory.toString(), BeamNetwork.Testnet) != null) delay(10)
            }

            assertEquals(1, session.resolveCalls)
        } finally {
            DemoSendJournal.load(directory.toString(), BeamNetwork.Testnet)?.let { pending ->
                DemoSendJournal.clear(directory.toString(), pending.owner, pending.operationId)
            }
            controller.close()
            deleteTree(directory)
        }
    }

    @Test
    fun close_duringNonCancellableCreation_closesTheReturnedUnboundWallet() = runBlocking {
        val directory = createTempDirectory("beam-demo-close-creation")
        val createEntered = CompletableDeferred<Unit>()
        val allowCreateToReturn = CompletableDeferred<Unit>()
        val session = FakeDemoSession(blockFirstStart = false)
        val factory = FakeDemoWalletFactory(session) {
            createEntered.complete(Unit)
            withContext(NonCancellable) { allowCreateToReturn.await() }
        }
        val controller = BeamDemoController(directory.toString(), factory)
        try {
            controller.edit {
                copy(seedHex = "01".repeat(64), databaseKeyHex = "02".repeat(32))
            }
            controller.create()
            withTimeout(5_000) { createEntered.await() }

            controller.close()
            allowCreateToReturn.complete(Unit)

            withTimeout(5_000) {
                while (session.closeCalls != 1) delay(10)
            }
            assertNull(controller.state.value.activeWallet)
        } finally {
            allowCreateToReturn.complete(Unit)
            controller.close()
            deleteTree(directory)
        }
    }

    @Test
    fun unresolvedSend_blocksSwitchToRecoveryDatabase() = runBlocking {
        val directory = createTempDirectory("beam-demo-block-recovery")
        val session = FakeDemoSession(blockFirstStart = false)
        val factory = FakeDemoWalletFactory(session)
        val controller = BeamDemoController(directory.toString(), factory)
        val owner = ActiveWalletIdentity(BeamNetwork.Testnet, directory.toString())
        try {
            controller.edit { copy(seedHex = "01".repeat(64), databaseKeyHex = "02".repeat(32)) }
            controller.create()
            withTimeout(5_000) { controller.state.first { it.walletState is BeamWalletState.Ready && !it.busy } }
            assertTrue(DemoSendJournal.claim(directory.toString(), owner, PENDING_OPERATION_ID))

            controller.edit { copy(restoreHeight = "12345") }
            controller.restoreHeight()

            val blocked = withTimeout(5_000) {
                controller.state.first {
                    it.message == "Resolve the pending Beam send before switching wallet databases" && !it.busy
                }
            }
            assertEquals(owner, blocked.activeWallet)
            assertEquals(0, factory.restoreCalls)
            assertEquals(0, session.closeCalls)
        } finally {
            DemoSendJournal.clear(directory.toString(), owner, PENDING_OPERATION_ID)
            controller.closeAndJoin()
            deleteTree(directory)
        }
    }

    @Test
    fun startupWithUnresolvedRecoverySend_reopensOwningDatabaseBeforeConfiguredBase() = runBlocking {
        val directory = createTempDirectory("beam-demo-resume-owner")
        val recoveryPath = directory.resolve("recovery/height-12345").toString()
        val owner = ActiveWalletIdentity(BeamNetwork.Mainnet, recoveryPath)
        val session = FakeDemoSession(
            blockFirstStart = false,
            resolvedSend = BeamSendResolution.Terminal(
                "tx-id",
                cash.p.beam.BeamTransactionStatus.Completed,
            ),
        )
        val factory = FakeDemoWalletFactory(session)
        val controller = BeamDemoController(
            initialStoragePath = directory.toString(),
            walletFactory = factory,
            walletExists = { true },
            initialState = DemoState(
                network = BeamNetwork.Testnet,
                storagePath = directory.toString(),
                databaseKeyHex = "02".repeat(32),
            ),
        )
        try {
            assertTrue(DemoSendJournal.claim(directory.toString(), owner, PENDING_OPERATION_ID))

            controller.openOrCreate()

            val resumed = withTimeout(5_000) {
                controller.state.first {
                    it.activeWallet == owner &&
                        it.walletState is BeamWalletState.Ready &&
                        !it.busy
                }
            }
            assertEquals(owner, resumed.activeWallet)
            assertEquals(BeamSdkConfig(BeamNetwork.Mainnet, recoveryPath), factory.lastOpenConfig)
            withTimeout(5_000) {
                while (DemoSendJournal.load(directory.toString(), BeamNetwork.Testnet) != null) delay(10)
            }
            assertEquals(1, session.resolveCalls)
        } finally {
            DemoSendJournal.load(directory.toString(), BeamNetwork.Testnet)?.let { pending ->
                DemoSendJournal.clear(directory.toString(), pending.owner, pending.operationId)
            }
            controller.closeAndJoin()
            deleteTree(directory)
        }
    }

    @Test
    fun reopen_installsJournalReconciliationBeforeCancellableNetworkStart() = runBlocking {
        val directory = createTempDirectory("beam-demo-reopen-journal")
        val session = FakeDemoSession(resolvedSend = BeamSendResolution.Terminal("tx-id", cash.p.beam.BeamTransactionStatus.Completed))
        val controller = BeamDemoController(directory.toString(), FakeDemoWalletFactory(session))
        try {
            val owner = ActiveWalletIdentity(BeamNetwork.Testnet, directory.toString())
            assertEquals(
                true,
                DemoSendJournal.claim(
                    directory.toString(),
                    owner,
                    "123e4567-e89b-42d3-a456-426614174000",
                ),
            )
            controller.edit { copy(databaseKeyHex = "02".repeat(32)) }

            controller.open()

            withTimeout(5_000) {
                while (
                    session.resolveCalls != 1 ||
                    DemoSendJournal.load(directory.toString(), BeamNetwork.Testnet) != null
                ) delay(10)
            }
            assertEquals(1, session.startCalls)
        } finally {
            DemoSendJournal.load(directory.toString(), BeamNetwork.Testnet)?.let { pending ->
                DemoSendJournal.clear(directory.toString(), pending.owner, pending.operationId)
            }
            controller.close()
            deleteTree(directory)
        }
    }

    private fun deleteTree(directory: java.nio.file.Path) {
        Files.walk(directory).use { paths ->
            paths.sorted(Comparator.reverseOrder()).forEach(Files::deleteIfExists)
        }
    }

    private companion object {
        const val PENDING_OPERATION_ID = "323e4567-e89b-42d3-a456-426614174000"
    }
}

private class FakeDemoWalletFactory(
    private val session: BeamWalletSession,
    private val restoredSession: BeamWalletSession = session,
    private val restoreFailure: Throwable? = null,
    private val beforeCreateReturn: suspend () -> Unit = {},
) : DemoWalletFactory {
    var createCalls: Int = 0
        private set
    var openCalls: Int = 0
        private set
    var restoreCalls: Int = 0
        private set
    var lastRestoreConfig: BeamSdkConfig? = null
        private set
    var lastRestoreSource: RestoreSource? = null
        private set
    var lastOpenConfig: BeamSdkConfig? = null
        private set

    override suspend fun createNew(
        config: BeamSdkConfig,
        seed: ByteArray,
        databaseKey: ByteArray,
    ): BeamWalletSession {
        createCalls += 1
        beforeCreateReturn()
        return session
    }

    override suspend fun openExisting(
        config: BeamSdkConfig,
        databaseKey: ByteArray,
    ): BeamWalletSession {
        openCalls += 1
        lastOpenConfig = config
        return session
    }

    override suspend fun restore(
        config: BeamSdkConfig,
        seed: ByteArray,
        databaseKey: ByteArray,
        source: RestoreSource,
    ): BeamWalletSession {
        restoreCalls += 1
        lastRestoreConfig = config
        lastRestoreSource = source
        restoreFailure?.let { throw it }
        return restoredSession
    }
}

private class FakeDemoSession(
    private val blockFirstStart: Boolean = true,
    private val blockFirstStop: Boolean = false,
    private val prepareFailure: Throwable? = null,
    private val resolvedSend: BeamSendResolution = BeamSendResolution.Submitted("tx-id"),
) : BeamWalletSession {
    private val mutableState = MutableStateFlow<BeamWalletState>(BeamWalletState.Stopped)
    private val firstStartCanFinish = CompletableDeferred<Unit>()
    val firstStopEntered = CompletableDeferred<Unit>()
    val allowFirstStop = CompletableDeferred<Unit>()
    override val state: StateFlow<BeamWalletState> = mutableState
    override val balance: StateFlow<BeamBalance> = MutableStateFlow(BeamBalance())
    override val transactions: StateFlow<List<BeamTransaction>> = MutableStateFlow(emptyList())
    @Volatile
    var startCalls = 0
    @Volatile
    var stopCalls = 0
    @Volatile
    var resolveCalls = 0
    @Volatile
    var closeCalls = 0
    val lifecycleEvents = mutableListOf<String>()

    override suspend fun start() {
        lifecycleEvents += "start"
        startCalls++
        if (startCalls == 1 && blockFirstStart) {
            mutableState.value = BeamWalletState.Restoring(
                cash.p.beam.BeamRestoreProgress(cash.p.beam.BeamRestorePhase.DownloadingSnapshot),
            )
            firstStartCanFinish.await()
            if (mutableState.value is BeamWalletState.Stopped) return
        }
        mutableState.value = BeamWalletState.Ready(10)
    }

    override suspend fun stop() {
        lifecycleEvents += "stop"
        stopCalls++
        if (stopCalls == 1 && blockFirstStop) {
            firstStopEntered.complete(Unit)
            allowFirstStop.await()
        }
        firstStartCanFinish.complete(Unit)
        mutableState.value = BeamWalletState.Stopped
    }

    override suspend fun close() {
        closeCalls++
    }

    override suspend fun receiveAddress(type: BeamAddressType): BeamAddress {
        lifecycleEvents += "receive:${type.name}"
        return BeamAddress("token", type, BeamNetwork.Testnet)
    }

    override suspend fun transactionPage(offset: Int, limit: Int): BeamTransactionPage =
        BeamTransactionPage(emptyList(), null)

    override suspend fun previewSend(request: BeamSendRequest): BeamSendPreview = BeamSendPreview(
        requestHash = "hash",
        previewVersion = 1,
        amount = request.amount,
        fee = 10,
        total = request.amount + 10,
        receiverType = BeamAddressType.PublicOffline,
    )

    override suspend fun prepareSend(
        operationId: String,
        request: BeamSendRequest,
        previewVersion: Long,
    ): PreparedBeamSend {
        prepareFailure?.let { throw it }
        return PreparedBeamSend(operationId, "tx-id")
    }

    override suspend fun commitSend(operationId: String): BeamSendResolution =
        BeamSendResolution.Submitted("tx-id")

    override suspend fun resolveSend(operationId: String): BeamSendResolution {
        resolveCalls++
        return resolvedSend
    }

    override suspend fun abortPrepared(operationId: String): Boolean = false
}
