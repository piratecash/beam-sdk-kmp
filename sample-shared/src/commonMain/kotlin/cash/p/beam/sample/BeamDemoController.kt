package cash.p.beam.sample

import co.touchlab.kermit.Logger
import cash.p.beam.BeamAddress
import cash.p.beam.BeamAddressType
import cash.p.beam.BeamBalance
import cash.p.beam.BeamNetwork
import cash.p.beam.BeamSdkConfig
import cash.p.beam.BeamSendPreview
import cash.p.beam.BeamSendRequest
import cash.p.beam.BeamSendResolution
import cash.p.beam.BeamTransaction
import cash.p.beam.BeamWalletFactory
import cash.p.beam.BeamWalletSession
import cash.p.beam.BeamWalletState
import cash.p.beam.RestoreSource
import io.horizontalsystems.hdwalletkit.Mnemonic
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.datetime.LocalDate

private val logger = Logger.withTag("BeamDemo")

internal data class DemoState(
    val network: BeamNetwork = BeamNetwork.Testnet,
    val storagePath: String,
    val seedHex: String = "",
    val databaseKeyHex: String = "",
    val restoreHeight: String = "",
    val restoreDate: String = "",
    val snapshotUrl: String = "",
    val snapshotSha256: String = "",
    val walletState: BeamWalletState = BeamWalletState.Closed,
    val balance: BeamBalance = BeamBalance(),
    val receiveAddressType: BeamAddressType = BeamAddressType.PublicOffline,
    val address: BeamAddress? = null,
    val transactions: List<BeamTransaction> = emptyList(),
    val receiver: String = "",
    val amount: String = "",
    val comment: String = "",
    val preview: BeamSendPreview? = null,
    val sendResolution: BeamSendResolution? = null,
    val activeWallet: ActiveWalletIdentity? = null,
    val busy: Boolean = false,
    val message: String? = null,
)

internal data class ActiveWalletIdentity(
    val network: BeamNetwork,
    val storagePath: String,
)

internal data class DemoPendingSend(
    val owner: ActiveWalletIdentity,
    val operationId: String,
)

internal interface DemoWalletFactory {
    suspend fun createNew(config: BeamSdkConfig, seed: ByteArray, databaseKey: ByteArray): BeamWalletSession
    suspend fun openExisting(config: BeamSdkConfig, databaseKey: ByteArray): BeamWalletSession
    suspend fun restore(
        config: BeamSdkConfig,
        seed: ByteArray,
        databaseKey: ByteArray,
        source: RestoreSource,
    ): BeamWalletSession
}

private object SdkDemoWalletFactory : DemoWalletFactory {
    private val delegate = BeamWalletFactory()

    override suspend fun createNew(config: BeamSdkConfig, seed: ByteArray, databaseKey: ByteArray) =
        delegate.createNew(config, seed, databaseKey)

    override suspend fun openExisting(config: BeamSdkConfig, databaseKey: ByteArray) =
        delegate.openExisting(config, databaseKey)

    override suspend fun restore(
        config: BeamSdkConfig,
        seed: ByteArray,
        databaseKey: ByteArray,
        source: RestoreSource,
    ) = delegate.restore(config, seed, databaseKey, source)
}

internal class BeamDemoController(
    initialStoragePath: String,
    private val walletFactory: DemoWalletFactory = SdkDemoWalletFactory,
    private val walletExists: (String) -> Boolean = DemoWalletStorage::walletExists,
    initialState: DemoState = DemoState(storagePath = initialStoragePath),
) {
    private val sendJournalRoot = initialStoragePath

    private data class BoundSession(
        val wallet: BeamWalletSession,
        val identity: ActiveWalletIdentity,
    )

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val mutableState = MutableStateFlow(initialState)
    private var session: BoundSession? = null
    private var observation: Job? = null
    private var sendReconciliation: Job? = null
    private var pendingStop: Job? = null
    @Volatile
    private var walletAction: Job? = null
    @Volatile
    private var closeJob: Job? = null
    @Volatile
    private var closed = false
    private var hostIsForeground = true
    private var wantsRunning = false

    val state: StateFlow<DemoState> = mutableState

    fun edit(update: DemoState.() -> DemoState) {
        mutableState.update { current -> current.update().copy(message = null) }
    }

    fun toggleNetwork() = edit {
        copy(network = if (network == BeamNetwork.Mainnet) BeamNetwork.Testnet else BeamNetwork.Mainnet)
    }

    fun dismissMessage() {
        mutableState.update { it.copy(message = null) }
    }

    fun selectReceiveAddressType(type: BeamAddressType) {
        mutableState.update {
            it.copy(receiveAddressType = type, address = null, message = null)
        }
        receiveAddress(type)
    }

    fun create() = launchWalletAction(
        actionName = "create",
        requiresSeed = { _, _ -> true },
    ) { input, config ->
        input.withCredentials { seed, databaseKey ->
            walletFactory.createNew(config, seed, databaseKey)
        }
    }

    fun open() = launchWalletAction(actionName = "open") { input, config ->
        input.withDatabaseKey { databaseKey -> walletFactory.openExisting(config, databaseKey) }
    }

    fun openOrCreate() = launchWalletAction(
        actionName = "auto-open-or-create",
        requiresSeed = { _, targetPath -> !walletExists(targetPath) },
    ) { input, config ->
        if (walletExists(config.storagePath)) {
            logger.i { "auto initialization selected existing ${input.network.name} wallet" }
            input.withDatabaseKey { databaseKey -> walletFactory.openExisting(config, databaseKey) }
        } else {
            logger.i { "auto initialization selected new ${input.network.name} wallet" }
            input.withCredentials { seed, databaseKey ->
                walletFactory.createNew(config, seed, databaseKey)
            }
        }
    }

    fun restoreHeight() = launchWalletAction(
        actionName = "restore-height",
        targetStoragePath = { input ->
            val height = input.restoreHeight.toLongOrNull()?.takeIf { it >= 0 }
                ?: error("Enter a valid restore height")
            DemoWalletStorage.recoveryPath(input.storagePath, "height-$height")
        },
        requiresSeed = { _, targetPath -> !walletExists(targetPath) },
    ) { input, config ->
        val source = RestoreSource.Height(requireNotNull(input.restoreHeight.toLongOrNull()))
        input.restoreOrOpen(config, source)
    }

    fun restoreDate() = launchWalletAction(
        actionName = "restore-date",
        targetStoragePath = { input ->
            val date = LocalDate.parse(input.restoreDate)
            DemoWalletStorage.recoveryPath(input.storagePath, "date-$date")
        },
        requiresSeed = { _, targetPath -> !walletExists(targetPath) },
    ) { input, config ->
        input.restoreOrOpen(config, RestoreSource.Date(LocalDate.parse(input.restoreDate)))
    }

    fun restoreFull() = launchWalletAction(
        actionName = "restore-full",
        targetStoragePath = { input -> DemoWalletStorage.recoveryPath(input.storagePath, "full") },
        requiresSeed = { _, targetPath -> !walletExists(targetPath) },
    ) { input, config ->
        input.restoreOrOpen(config, RestoreSource.FullScan)
    }

    fun restoreSnapshot() = launchWalletAction(
        actionName = "restore-snapshot",
        targetStoragePath = { input ->
            val source = input.snapshotRestoreSource()
            val suffix = source.expectedSha256?.lowercase()?.take(12) ?: "official"
            DemoWalletStorage.recoveryPath(input.storagePath, "snapshot-$suffix")
        },
        requiresSeed = { _, targetPath -> !walletExists(targetPath) },
    ) { input, config ->
        input.restoreOrOpen(config, input.snapshotRestoreSource())
    }

    fun stop() {
        wantsRunning = false
        logger.d { "manual stop requested" }
        pendingStop = stopActiveWallet()
    }

    private fun stopActiveWallet(): Job? {
        val active = session ?: return null
        return scope.launch {
            try {
                active.wallet.stop()
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (error: Throwable) {
                mutableState.update { it.copy(message = error.message ?: error::class.simpleName) }
            }
        }
    }

    fun start() {
        wantsRunning = true
        if (!hostIsForeground) return
        val active = session
        val stopToAwait = pendingStop
        logger.d { "manual start requested" }
        launch {
            stopToAwait?.join()
            if (hostIsForeground && wantsRunning && session === active) {
                active?.wallet?.start() ?: error("Wallet is still initializing")
            }
        }
    }

    fun onHostBackground() {
        if (!hostIsForeground) return
        hostIsForeground = false
        logger.d { "host entered background" }
        if (wantsRunning) pendingStop = stopActiveWallet()
    }

    fun onHostForeground() {
        if (hostIsForeground) return
        hostIsForeground = true
        logger.d { "host entered foreground" }
        if (!wantsRunning) return
        val active = session ?: return
        val stopToAwait = pendingStop
        launch {
            stopToAwait?.join()
            if (hostIsForeground && wantsRunning && session === active) active.wallet.start()
        }
    }

    fun receiveAddress() = receiveAddress(mutableState.value.receiveAddressType)

    fun receiveAddress(type: BeamAddressType) = launch {
        val active = session ?: error("Wallet is still initializing")
        val address = active.wallet.receiveAddress(type)
        if (session === active && mutableState.value.receiveAddressType == type) {
            mutableState.update { it.copy(address = address) }
            logger.i { "receive token generated type=${type.name} network=${address.network.name}" }
        }
    }

    fun previewSend() = launch {
        val input = mutableState.value
        val request = input.sendRequest()
        val active = session ?: error("Wallet is still initializing")
        val preview = active.wallet.previewSend(request)
        if (session === active) mutableState.update { it.copy(preview = preview, sendResolution = null) }
    }

    fun confirmSend() = launch {
        val input = mutableState.value
        val preview = input.preview ?: error("Preview the send first")
        val active = session ?: error("Open or create a wallet first")
        val wallet = active.wallet
        val operationId = newOperationId()
        check(DemoSendJournal.claim(sendJournalRoot, active.identity, operationId)) {
            "Another Beam send operation is still unresolved"
        }
        val resolution = try {
            wallet.prepareSend(operationId, input.sendRequest(), preview.previewVersion)
            wallet.commitSend(operationId)
        } catch (cancelled: CancellationException) {
            if (session === active) monitorSend(active, operationId, initial = null)
            throw cancelled
        } catch (error: Throwable) {
            val reconciled = runCatching { wallet.resolveSend(operationId) }.getOrNull()
            if (reconciled == null && session === active) {
                mutableState.update { current ->
                    current.copy(message = error.message ?: "Beam send preparation failed")
                }
            }
            reconciled
        }
        if (session === active) {
            mutableState.update { it.copy(sendResolution = resolution, preview = null) }
            monitorSend(active, operationId, resolution)
        }
    }

    fun close() {
        if (closed) return
        closed = true
        wantsRunning = false
        logger.d { "controller close requested" }
        observation?.cancel()
        sendReconciliation?.cancel()
        val inFlightWalletAction = walletAction
        val stopToAwait = pendingStop
        closeJob = scope.launch {
            withContext(NonCancellable) {
                inFlightWalletAction?.cancelAndJoin()
                stopToAwait?.join()
                val active = session
                session = null
                active?.wallet?.close()
            }
            logger.d { "controller closed" }
            scope.cancel()
        }
    }

    suspend fun closeAndJoin() {
        close()
        closeJob?.join()
    }

    private fun launchWalletAction(
        actionName: String,
        targetStoragePath: (DemoState) -> String = { it.storagePath.trim() },
        requiresSeed: (DemoState, String) -> Boolean = { _, _ -> false },
        createSession: suspend (DemoState, BeamSdkConfig) -> BeamWalletSession,
    ) {
        if (closed) return
        wantsRunning = true
        val action = launch(start = CoroutineStart.LAZY) {
            var unboundWallet: BeamWalletSession? = null
            try {
                val requestedInput = mutableState.value
                val requestedPath = targetStoragePath(requestedInput).also {
                    require(it.isNotBlank()) { "Wallet storage path must not be blank" }
                }
                val requestedIdentity = ActiveWalletIdentity(requestedInput.network, requestedPath)
                val previous = session
                val pendingSend = DemoSendJournal.load(sendJournalRoot, requestedInput.network)
                if (previous != null && pendingSend != null) {
                    check(previous.identity == pendingSend.owner) {
                        "The unresolved Beam send belongs to another wallet database"
                    }
                    check(requestedIdentity == pendingSend.owner) {
                        "Resolve the pending Beam send before switching wallet databases"
                    }
                }
                val resumePendingSend = previous == null && pendingSend != null
                val input = if (resumePendingSend) {
                    requestedInput.copy(network = requireNotNull(pendingSend).owner.network)
                } else {
                    requestedInput
                }
                val targetPath = if (resumePendingSend) {
                    requireNotNull(pendingSend).owner.storagePath
                } else {
                    requestedPath
                }
                input.validateCredentials(
                    seedRequired = !resumePendingSend && requiresSeed(input, targetPath),
                )
                val targetConfig = BeamSdkConfig(input.network, targetPath)
                logger.i { "wallet action=$actionName network=${input.network.name}" }
                sendReconciliation?.cancel()
                previous?.wallet?.close()
                session = null
                mutableState.update {
                    it.copy(
                        network = input.network,
                        activeWallet = null,
                        walletState = BeamWalletState.Closed,
                        balance = BeamBalance(),
                        address = null,
                        transactions = emptyList(),
                        preview = null,
                        sendResolution = null,
                    )
                }
                val wallet = try {
                    if (resumePendingSend) {
                        logger.w { "resuming unresolved send in its owning wallet database" }
                        input.withDatabaseKey { databaseKey ->
                            walletFactory.openExisting(targetConfig, databaseKey)
                        }
                    } else {
                        createSession(input, targetConfig)
                    }
                } catch (error: Throwable) {
                    if (previous != null && !closed) {
                        try {
                            val reopened = input.withDatabaseKey { databaseKey ->
                                walletFactory.openExisting(
                                    BeamSdkConfig(previous.identity.network, previous.identity.storagePath),
                                    databaseKey,
                                )
                            }
                            val restored = installSession(reopened, previous.identity, input)
                            if (hostIsForeground && wantsRunning) restored.wallet.start()
                            logger.w { "wallet action=$actionName failed; previous wallet reopened" }
                        } catch (rollbackError: Throwable) {
                            error.addSuppressed(rollbackError)
                            logger.e(rollbackError) { "failed to reopen previous wallet after action=$actionName" }
                        }
                    }
                    throw error
                }
                unboundWallet = wallet
                currentCoroutineContext().ensureActive()
                if (closed) return@launch
                val active = installSession(
                    wallet,
                    ActiveWalletIdentity(input.network, targetPath),
                    input,
                )
                unboundWallet = null
                if (hostIsForeground && wantsRunning) wallet.start()
                logger.i { "wallet action=$actionName installed and started=${hostIsForeground && wantsRunning}" }
            } finally {
                val wallet = unboundWallet
                if (wallet != null) {
                    withContext(NonCancellable) { wallet.close() }
                }
            }
        }
        walletAction = action
        action.start()
    }

    private suspend fun installSession(
        wallet: BeamWalletSession,
        identity: ActiveWalletIdentity,
        input: DemoState,
    ): BoundSession {
        val active = BoundSession(wallet, identity)
        session = active
        mutableState.update { it.copy(activeWallet = active.identity) }
        observe(active)
        DemoSendJournal.load(sendJournalRoot, active.identity.network)
            ?.takeIf { it.owner == active.identity }
            ?.let { pending ->
                // Stopped resolution is supported, so claim ownership before a cancellable network
                // start. Background/Stop cannot strand a durable operation journal.
                monitorSend(active, pending.operationId, initial = null)
            }
        try {
            val address = wallet.receiveAddress(input.receiveAddressType)
            if (session === active && mutableState.value.receiveAddressType == address.type) {
                mutableState.update { it.copy(address = address) }
                logger.i { "receive token generated type=${address.type.name} network=${address.network.name}" }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Throwable) {
            logger.e(error) { "receive token generation failed" }
            mutableState.update {
                it.copy(message = error.message ?: "Unable to generate a BEAM receive token")
            }
        }
        return active
    }

    private suspend fun DemoState.restoreOrOpen(
        config: BeamSdkConfig,
        source: RestoreSource,
    ): BeamWalletSession = if (walletExists(config.storagePath)) {
        withDatabaseKey { databaseKey -> walletFactory.openExisting(config, databaseKey) }
    } else {
        withCredentials { seed, databaseKey -> walletFactory.restore(config, seed, databaseKey, source) }
    }

    private fun monitorSend(
        active: BoundSession,
        operationId: String,
        initial: BeamSendResolution?,
    ) {
        sendReconciliation?.cancel()
        sendReconciliation = scope.launch {
            val wallet = active.wallet
            var resolution = initial
            while (true) {
                if (session !== active) return@launch
                val current = resolution
                if (current == null) {
                    resolution = try {
                        wallet.resolveSend(operationId)
                    } catch (cancelled: CancellationException) {
                        throw cancelled
                    } catch (_: Throwable) {
                        delay(SEND_RECONCILIATION_INTERVAL_MILLIS)
                        continue
                    }
                    continue
                }
                mutableState.update { it.copy(sendResolution = current) }
                try {
                    when (current) {
                        BeamSendResolution.NotPrepared,
                        is BeamSendResolution.Terminal,
                        -> {
                            DemoSendJournal.clear(sendJournalRoot, active.identity, operationId)
                            return@launch
                        }
                        is BeamSendResolution.Prepared,
                        is BeamSendResolution.Committing,
                        is BeamSendResolution.Indeterminate,
                        -> {
                            if (wallet.state.value is BeamWalletState.Ready) {
                                resolution = wallet.commitSend(operationId)
                            }
                        }
                        else -> resolution = wallet.resolveSend(operationId)
                    }
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (error: Throwable) {
                    mutableState.update {
                        it.copy(message = error.message ?: "Send reconciliation failed")
                    }
                    try {
                        resolution = wallet.resolveSend(operationId)
                    } catch (cancelled: CancellationException) {
                        throw cancelled
                    } catch (_: Throwable) {
                        // Keep the durable operation ID and retry while the session is alive.
                    }
                }
                delay(SEND_RECONCILIATION_INTERVAL_MILLIS)
            }
        }
    }

    private fun observe(active: BoundSession) {
        observation?.cancel()
        observation = scope.launch {
            val wallet = active.wallet
            launch {
                wallet.state.collectLatest { value ->
                    if (session === active) {
                        mutableState.update { it.copy(walletState = value) }
                        logger.d { "wallet state=${value::class.simpleName}" }
                    }
                }
            }
            launch {
                wallet.balance.collectLatest { value ->
                    if (session === active) mutableState.update { it.copy(balance = value) }
                }
            }
            launch {
                wallet.transactions.collectLatest { value ->
                    if (session === active) mutableState.update { it.copy(transactions = value) }
                }
            }
        }
    }

    private fun launch(
        start: CoroutineStart = CoroutineStart.DEFAULT,
        block: suspend () -> Unit,
    ): Job = scope.launch(start = start) {
        mutableState.update { it.copy(busy = true, message = null) }
        try {
            block()
        } catch (_: CancellationException) {
            // stop() intentionally cancels an in-flight snapshot download/start.
        } catch (error: Throwable) {
            logger.e(error) { "demo operation failed" }
            mutableState.update { it.copy(message = error.message ?: error::class.simpleName) }
        } finally {
            mutableState.update { it.copy(busy = false) }
        }
    }

    private companion object {
        const val SEND_RECONCILIATION_INTERVAL_MILLIS = 1_000L
    }
}

internal fun configuredDemoState(fallbackStoragePath: String): DemoState {
    val words = DemoConfig.WORDS.trim().split(Regex("\\s+")).filter(String::isNotEmpty)
    val seedResult = runCatching {
        if (words.isEmpty()) {
            ""
        } else {
            Mnemonic().toSeed(words).let { seed ->
                try {
                    seed.joinToString(separator = "") { byte -> "%02x".format(byte.toInt() and 0xff) }
                } finally {
                    seed.fill(0)
                }
            }
        }
    }
    val network = BeamNetwork.entries.firstOrNull {
        it.name.equals(DemoConfig.NETWORK.trim(), ignoreCase = true)
    } ?: BeamNetwork.Testnet
    return DemoState(
        network = network,
        storagePath = DemoConfig.STORAGE_PATH.trim().ifEmpty { fallbackStoragePath },
        seedHex = seedResult.getOrDefault(""),
        databaseKeyHex = DemoConfig.DATABASE_KEY_HEX.trim(),
        message = seedResult.exceptionOrNull()?.let { "Invalid mnemonic in local.properties: ${it.message}" },
    )
}

private fun DemoState.config() = BeamSdkConfig(network = network, storagePath = storagePath.trim())

private fun DemoState.seed(): ByteArray = seedHex.hexBytes(64, "BIP39 seed")

private fun DemoState.databaseKey(): ByteArray = databaseKeyHex.hexBytes(32, "database key")

private fun DemoState.validateCredentials(seedRequired: Boolean) {
    val key = databaseKey()
    try {
        if (seedRequired) seed().fill(0)
    } finally {
        key.fill(0)
    }
}

private suspend fun <T> DemoState.withDatabaseKey(block: suspend (ByteArray) -> T): T {
    val key = databaseKey()
    return try {
        block(key)
    } finally {
        key.fill(0)
    }
}

private suspend fun <T> DemoState.withCredentials(
    block: suspend (seed: ByteArray, databaseKey: ByteArray) -> T,
): T {
    val seed = seed()
    val key = databaseKey()
    return try {
        block(seed, key)
    } finally {
        seed.fill(0)
        key.fill(0)
    }
}

private fun DemoState.snapshotRestoreSource(): RestoreSource.SnapshotThenScan {
    val url = snapshotUrl.trim().ifEmpty { null }
    val hash = snapshotSha256.trim().ifEmpty { null }
    require(url == null || hash != null) { "A custom snapshot URL requires a trusted SHA-256" }
    hash?.let {
        require(Regex("[0-9a-fA-F]{64}").matches(it)) {
            "Trusted snapshot SHA-256 must contain 64 hexadecimal characters"
        }
    }
    return RestoreSource.SnapshotThenScan(snapshotUrl = url, expectedSha256 = hash)
}

private fun DemoState.sendRequest(): BeamSendRequest {
    val atomicAmount = amount.toLongOrNull() ?: error("Amount must be atomic BEAM units")
    return BeamSendRequest(receiverToken = receiver.trim(), amount = atomicAmount, comment = comment)
}

private fun String.hexBytes(expectedSize: Int, label: String): ByteArray {
    val clean = trim()
    require(clean.length == expectedSize * 2) { "$label must contain ${expectedSize * 2} hex characters" }
    return ByteArray(expectedSize) { index ->
        clean.substring(index * 2, index * 2 + 2).toIntOrNull(16)?.toByte()
            ?: error("$label contains non-hex characters")
    }
}

internal expect fun newOperationId(): String

internal expect object DemoSendJournal {
    fun load(journalRoot: String, legacyNetwork: BeamNetwork): DemoPendingSend?
    fun claim(journalRoot: String, owner: ActiveWalletIdentity, operationId: String): Boolean
    fun clear(journalRoot: String, owner: ActiveWalletIdentity, operationId: String)
}

internal expect object DemoWalletStorage {
    fun walletExists(storagePath: String): Boolean
    fun recoveryPath(storagePath: String, recoveryId: String): String
}
