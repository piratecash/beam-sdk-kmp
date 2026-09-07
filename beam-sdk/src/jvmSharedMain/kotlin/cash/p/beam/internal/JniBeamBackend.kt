package cash.p.beam.internal

import co.touchlab.kermit.Logger
import cash.p.beam.BeamAddress
import cash.p.beam.BeamAddressType
import cash.p.beam.BeamBalance
import cash.p.beam.BeamFailure
import cash.p.beam.BeamNetwork
import cash.p.beam.BeamSdkConfig
import cash.p.beam.BeamSendPreview
import cash.p.beam.BeamSendRequest
import cash.p.beam.BeamSendResolution
import cash.p.beam.BeamTransaction
import cash.p.beam.BeamTransactionDirection
import cash.p.beam.BeamTransactionStatus
import cash.p.beam.PreparedBeamSend
import cash.p.beam.RestoreSource
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

internal actual fun createPlatformBackend(): BeamBackend = JniBeamBackend()

private val logger = Logger.withTag("BeamSDK")

private class JniBeamBackend : BeamBackend {
    private val gate = Mutex()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val mutableSnapshot = MutableStateFlow(BackendSnapshot())
    private var handle = NO_HANDLE
    private var pollJob: Job? = null
    @Volatile
    private var startJob: Job? = null
    private var config: BeamSdkConfig? = null
    private var snapshotRestoreState = SnapshotRestoreState()
    private var lastLoggedPhase: String? = null
    private var lastLoggedCurrentHeight: Long? = null
    private var lastLoggedTargetHeight: Long? = null
    private var lastLoggedSyncDone: Long? = null
    private var lastLoggedSyncTotal: Long? = null

    override val snapshot: StateFlow<BackendSnapshot> = mutableSnapshot

    override suspend fun create(
        config: BeamSdkConfig,
        seed: ByteArray,
        databaseKey: ByteArray,
        restoreSource: RestoreSource?,
    ): Unit = gate.withLock {
        check(handle == NO_HANDLE) { "Beam backend already opened" }
        logger.i {
            "create requested network=${config.network.name} mode=${restoreSource?.logName() ?: "NewWallet"}"
        }
        ensureLoaded()
        val restoreConfiguration = restoreConfiguration(config, restoreSource)
        handle = nativeCall {
            BeamNative.create(
                storagePath = config.storagePath,
                databaseKey = databaseKey,
                seed = seed,
                network = config.network.ordinal,
                restoreType = restoreConfiguration.first,
                restoreValue = restoreConfiguration.second,
                logLevel = config.logLevel.ordinal,
            )
        }
        checkValidHandle()
        this.config = config
        val pendingSnapshot = restoreSource as? RestoreSource.SnapshotThenScan
        snapshotRestoreState = SnapshotRestoreState(
            pendingSource = pendingSnapshot,
            downloadedFile = pendingSnapshot?.let {
                completedSnapshotPath(config.storagePath).takeIf(java.nio.file.Files::isRegularFile)
            },
        )
        logger.i { "create completed network=${config.network.name}" }
    }

    override suspend fun open(config: BeamSdkConfig, databaseKey: ByteArray): Unit = gate.withLock {
        check(handle == NO_HANDLE) { "Beam backend already opened" }
        logger.i { "open requested network=${config.network.name}" }
        ensureLoaded()
        handle = nativeCall {
            BeamNative.open(
                storagePath = config.storagePath,
                databaseKey = databaseKey,
                network = config.network.ordinal,
                logLevel = config.logLevel.ordinal,
            )
        }
        checkValidHandle()
        this.config = config
        val pendingSnapshot = nativeCall { BeamNative.snapshotRestoreIntent(requireHandle()) }
            .takeIf(String::isNotEmpty)
            ?.let { json.decodeFromString<SnapshotRestoreIntentDto>(it).toRestoreSource() }
        snapshotRestoreState = SnapshotRestoreState(
            pendingSource = pendingSnapshot,
            downloadedFile = completedSnapshotPath(config.storagePath)
                .takeIf(java.nio.file.Files::isRegularFile),
        )
        logger.i { "open completed network=${config.network.name} pendingSnapshot=${pendingSnapshot != null}" }
    }

    override suspend fun start() {
        val ownerJob = currentCoroutineContext()[Job]
        startJob = ownerJob
        logger.d { "start requested" }
        try {
            gate.withLock {
                snapshotRestoreState.sourceToDownload?.let { source ->
                    val activeConfig = requireNotNull(config) { "Beam backend configuration is missing" }
                    val httpClient = platformSnapshotHttpClient()
                    try {
                        mutableSnapshot.value = mutableSnapshot.value.copy(
                            phase = BackendPhase.DownloadingSnapshot,
                            failureMessage = null,
                        )
                        val download = SnapshotDownloader(httpClient).download(
                            network = activeConfig.network,
                            storagePath = activeConfig.storagePath,
                            requestedUrl = source.snapshotUrl,
                            expectedSha256 = source.expectedSha256,
                        ) { downloaded, total ->
                            mutableSnapshot.value = mutableSnapshot.value.copy(
                                phase = BackendPhase.DownloadingSnapshot,
                                restoreBytes = downloaded,
                                restoreTotalBytes = total,
                            )
                        }
                        mutableSnapshot.value = mutableSnapshot.value.copy(
                            phase = BackendPhase.ValidatingSnapshot,
                            restoreBytes = download.bytes,
                            restoreTotalBytes = download.bytes,
                        )
                        snapshotRestoreState = snapshotRestoreState.afterDownload(download.path)
                    } catch (cancelled: CancellationException) {
                        throw cancelled
                    } catch (error: Throwable) {
                        val failure = error.asBeamFailure()
                        mutableSnapshot.value = BackendSnapshot(
                            phase = BackendPhase.Error,
                            failureMessage = failure.message,
                            failureRetryable = failure.retryable,
                            failureKind = failure.backendKind(),
                        )
                        throw failure
                    } finally {
                        httpClient.close()
                    }
                }
                nativeCall { BeamNative.start(requireHandle()) }
                if (pollJob?.isActive != true) {
                    pollJob = scope.launch { pollSnapshots() }
                }
                logger.d { "native wallet started" }
            }
        } catch (error: Throwable) {
            if (error !is CancellationException) logger.e(error) { "start failed" }
            throw error
        } finally {
            if (startJob === ownerJob) startJob = null
        }
    }

    override suspend fun stop() {
        logger.d { "stop requested" }
        withContext(NonCancellable) {
            cancelPendingStart()
            gate.withLock {
                pollJob?.cancelAndJoin()
                pollJob = null
                nativeCall { BeamNative.stop(requireHandle()) }
                mutableSnapshot.value = mutableSnapshot.value.copy(
                    phase = BackendPhase.Stopped,
                    balance = mutableSnapshot.value.balance.copy(isAuthoritative = false),
                )
            }
        }
        logger.d { "stop completed" }
    }

    override suspend fun close() {
        logger.d { "close requested" }
        withContext(NonCancellable) {
            cancelPendingStart()
            gate.withLock {
                if (handle == NO_HANDLE) return@withLock
                pollJob?.cancelAndJoin()
                pollJob = null
                val closingHandle = handle
                handle = NO_HANDLE
                try {
                    nativeCall { BeamNative.close(closingHandle) }
                } finally {
                    // Keep a downloaded snapshot until native import reaches a success phase.
                    // The DB persists its path, so an interrupted restore can resume after reopen.
                    snapshotRestoreState = SnapshotRestoreState()
                    config = null
                    mutableSnapshot.value = BackendSnapshot()
                    scope.cancel()
                }
            }
        }
        logger.d { "close completed" }
    }

    private suspend fun cancelPendingStart() {
        val current = currentCoroutineContext()[Job]
        startJob?.takeUnless { it === current }?.cancelAndJoin()
    }

    override suspend fun receiveAddress(type: BeamAddressType): BeamAddress = ioCall {
        json.decodeFromString<AddressDto>(BeamNative.receiveAddress(requireHandle(), type.ordinal)).toDomain().also {
            logger.i { "receive token generated type=${type.name} network=${it.network.name}" }
        }
    }

    override suspend fun transactions(offset: Int, limit: Int): List<BeamTransaction> = ioCall {
        json.decodeFromString<List<TransactionDto>>(
            BeamNative.transactions(requireHandle(), offset, limit),
        ).map(TransactionDto::toDomain)
    }

    override suspend fun previewSend(request: BeamSendRequest): BeamSendPreview = ioCall {
        json.decodeFromString<PreviewDto>(
            BeamNative.previewSend(
                requireHandle(),
                request.receiverToken,
                request.amount,
                request.comment,
            ),
        ).toDomain()
    }

    override suspend fun prepareSend(
        operationId: String,
        request: BeamSendRequest,
        previewVersion: Long,
    ): PreparedBeamSend = ioCall {
        val dto = json.decodeFromString<PreparedDto>(
            BeamNative.prepareSend(
                requireHandle(),
                operationId,
                request.receiverToken,
                request.amount,
                request.comment,
                previewVersion,
            ),
        )
        PreparedBeamSend(operationId = operationId, transactionId = dto.transactionId)
    }

    override suspend fun commitSend(operationId: String): BeamSendResolution = ioCall {
        BeamNative.commitSend(requireHandle(), operationId).toResolution()
    }

    override suspend fun resolveSend(operationId: String): BeamSendResolution = gate.withLock {
        ioCall { BeamNative.resolveSend(requireHandle(), operationId).toResolution() }
    }

    override suspend fun abortPrepared(operationId: String): Boolean = gate.withLock {
        ioCall { BeamNative.abortPrepared(requireHandle(), operationId) }
    }

    private suspend fun pollSnapshots() {
        while (currentCoroutineContext().isActive) {
            try {
                val dto = json.decodeFromString<SnapshotDto>(
                    BeamNative.snapshot(requireHandle()),
                )
                mutableSnapshot.value = dto.toDomain()
                if (
                    lastLoggedPhase != dto.phase ||
                    lastLoggedCurrentHeight != dto.currentHeight ||
                    lastLoggedTargetHeight != dto.targetHeight ||
                    lastLoggedSyncDone != dto.syncDone ||
                    lastLoggedSyncTotal != dto.syncTotal
                ) {
                    lastLoggedPhase = dto.phase
                    lastLoggedCurrentHeight = dto.currentHeight
                    lastLoggedTargetHeight = dto.targetHeight
                    lastLoggedSyncDone = dto.syncDone
                    lastLoggedSyncTotal = dto.syncTotal
                    logger.d {
                        "state phase=${dto.phase} current=${dto.currentHeight} target=${dto.targetHeight}" +
                            " syncDone=${dto.syncDone ?: "n/a"} syncTotal=${dto.syncTotal ?: "n/a"}"
                    }
                }
                if (snapshotRestoreState.downloadedFile != null && dto.phase in SNAPSHOT_SUCCESS_NATIVE_PHASES) {
                    snapshotRestoreState = snapshotRestoreState.afterSuccessfulImport()
                }
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (error: Throwable) {
                logger.e(error) { "native state polling failed" }
                mutableSnapshot.value = BackendSnapshot(
                    phase = BackendPhase.Error,
                    failureMessage = error.message ?: "Native snapshot failed",
                    failureRetryable = true,
                    failureKind = BackendFailureKind.Native,
                )
            }
            delay(POLL_INTERVAL_MILLIS)
        }
    }

    private suspend fun <T> ioCall(block: () -> T): T = withContext(Dispatchers.IO) {
        nativeCall(block)
    }

    private fun ensureLoaded() {
        NativeLibraryLoader.load()
    }

    private fun requireHandle(): Long = handle.takeUnless { it == NO_HANDLE }
        ?: error("Beam backend is not open")

    private fun checkValidHandle() {
        if (handle == NO_HANDLE) throw BeamFailure.Native("Beam Core did not return a wallet handle")
    }

    private fun <T> nativeCall(block: () -> T): T = try {
        block()
    } catch (error: BeamFailure) {
        logger.e(error) { "native operation failed category=${error::class.simpleName}" }
        throw error
    } catch (error: Throwable) {
        val failure = error.asBeamFailure()
        logger.e(error) { "native operation failed category=${failure::class.simpleName}" }
        throw failure
    }

    private fun restoreConfiguration(
        config: BeamSdkConfig,
        source: RestoreSource?,
    ): Pair<Int, String> = when (source) {
        null -> RESTORE_NEW to ""
        is RestoreSource.Height -> RESTORE_HEIGHT to source.heightInclusive.toString()
        is RestoreSource.Date -> RESTORE_DATE to source.utcDate.toString()
        is RestoreSource.SnapshotThenScan -> {
            val intent = SnapshotRestoreIntentDto(
                snapshotUrl = source.snapshotUrl,
                expectedSha256 = source.expectedSha256,
                path = completedSnapshotPath(config.storagePath).toAbsolutePath().toString(),
            )
            RESTORE_SNAPSHOT to json.encodeToString(SnapshotRestoreIntentDto.serializer(), intent)
        }
        RestoreSource.FullScan -> RESTORE_FULL to ""
    }

    private companion object {
        const val NO_HANDLE = 0L
        const val POLL_INTERVAL_MILLIS = 250L
        const val RESTORE_NEW = -1
        const val RESTORE_HEIGHT = 0
        const val RESTORE_DATE = 1
        const val RESTORE_SNAPSHOT = 2
        const val RESTORE_FULL = 3
        val SNAPSHOT_SUCCESS_NATIVE_PHASES = setOf("CatchingUp", "Syncing", "Ready")
    }
}

internal data class SnapshotRestoreState(
    val pendingSource: RestoreSource.SnapshotThenScan? = null,
    val downloadedFile: java.nio.file.Path? = null,
) {
    val sourceToDownload: RestoreSource.SnapshotThenScan?
        get() = pendingSource.takeIf { downloadedFile == null }

    fun afterDownload(path: java.nio.file.Path): SnapshotRestoreState =
        SnapshotRestoreState(downloadedFile = path)

    fun afterSuccessfulImport(): SnapshotRestoreState {
        downloadedFile?.let(java.nio.file.Files::deleteIfExists)
        return SnapshotRestoreState()
    }
}

private fun RestoreSource.logName(): String = when (this) {
    is RestoreSource.Height -> "Height"
    is RestoreSource.Date -> "Date"
    is RestoreSource.SnapshotThenScan -> "SnapshotThenScan"
    RestoreSource.FullScan -> "FullScan"
}

private fun Throwable.asBeamFailure(): BeamFailure {
    if (this is CancellationException) throw this
    if (this is BeamFailure) return this
    nativeFailure("VALIDATION")?.let { return BeamFailure.Validation(it) }
    nativeFailure("INSUFFICIENT_FUNDS")?.let { return BeamFailure.InsufficientFunds(it) }
    nativeFailure("STORAGE")?.let { return BeamFailure.Storage(it, this) }
    nativeFailure("NATIVE")?.let { return BeamFailure.Native(it, this) }
    return if (this is IllegalArgumentException) {
        BeamFailure.Validation(message ?: "Invalid Beam SDK input")
    } else {
        BeamFailure.Native(message ?: "Beam native call failed", this)
    }
}

private fun Throwable.nativeFailure(kind: String): String? {
    val prefix = "BEAM_${kind}|"
    return message?.takeIf { it.startsWith(prefix) }?.removePrefix(prefix)
}

private fun BeamFailure.backendKind(): BackendFailureKind = when (this) {
    is BeamFailure.Node -> BackendFailureKind.Node
    is BeamFailure.Quorum -> BackendFailureKind.Quorum
    is BeamFailure.Download -> BackendFailureKind.Download
    is BeamFailure.Storage -> BackendFailureKind.Storage
    else -> BackendFailureKind.Native
}

internal fun nativeVersionForTests(): String {
    NativeLibraryLoader.load()
    return BeamNative.version()
}

internal fun nativeStringRoundTripForTests(value: String): String {
    NativeLibraryLoader.load()
    return BeamNative.stringRoundTripForTests(value)
}

internal object BeamNative {
    external fun version(): String
    external fun stringRoundTripForTests(value: String): String
    external fun create(
        storagePath: String,
        databaseKey: ByteArray,
        seed: ByteArray,
        network: Int,
        restoreType: Int,
        restoreValue: String,
        logLevel: Int,
    ): Long
    external fun open(storagePath: String, databaseKey: ByteArray, network: Int, logLevel: Int): Long
    external fun snapshotRestoreIntent(handle: Long): String
    external fun start(handle: Long)
    external fun stop(handle: Long)
    external fun close(handle: Long)
    external fun snapshot(handle: Long): String
    external fun receiveAddress(handle: Long, type: Int): String
    external fun transactions(handle: Long, offset: Int, limit: Int): String
    external fun previewSend(handle: Long, receiver: String, amount: Long, comment: String): String
    external fun prepareSend(
        handle: Long,
        operationId: String,
        receiver: String,
        amount: Long,
        comment: String,
        previewVersion: Long,
    ): String
    external fun commitSend(handle: Long, operationId: String): String
    external fun resolveSend(handle: Long, operationId: String): String
    external fun abortPrepared(handle: Long, operationId: String): Boolean
    external fun seedPreparedSendForTests(handle: Long, operationId: String): String
    external fun seedInterruptedBootstrapForTests(handle: Long)
    external fun bodyRequestsPendingForTests(handle: Long): Boolean
    external fun newWalletCreatedAtForTests(handle: Long): Long
    external fun newWalletTipFreshForTests(creationTimestamp: Long, tipTimestamp: Long): Boolean
}

internal expect object NativeLibraryLoader {
    fun load()
}

private val json = Json {
    ignoreUnknownKeys = false
    explicitNulls = false
}

@Serializable
private data class SnapshotRestoreIntentDto(
    val snapshotUrl: String? = null,
    val expectedSha256: String? = null,
    val path: String? = null,
) {
    fun toRestoreSource(): RestoreSource.SnapshotThenScan = RestoreSource.SnapshotThenScan(
        snapshotUrl = snapshotUrl,
        expectedSha256 = expectedSha256,
    )
}

@Serializable
private data class SnapshotDto(
    val phase: String,
    val currentHeight: Long = 0,
    val targetHeight: Long = 0,
    val balance: BalanceDto = BalanceDto(),
    val transactions: List<TransactionDto> = emptyList(),
    val restoreCurrent: Long? = null,
    val restoreTarget: Long? = null,
    val syncDone: Long? = null,
    val syncTotal: Long? = null,
    val restoreBytes: Long? = null,
    val restoreTotalBytes: Long? = null,
    val failureMessage: String? = null,
    val failureRetryable: Boolean = true,
    val failureKind: String = BackendFailureKind.Node.name,
) {
    fun toDomain(): BackendSnapshot = BackendSnapshot(
        phase = BackendPhase.valueOf(phase),
        currentHeight = currentHeight,
        targetHeight = targetHeight,
        balance = balance.toDomain(phase == BackendPhase.Ready.name),
        transactions = transactions.map(TransactionDto::toDomain),
        restoreCurrent = restoreCurrent,
        restoreTarget = restoreTarget,
        restoreBytes = restoreBytes,
        restoreTotalBytes = restoreTotalBytes,
        failureMessage = failureMessage,
        failureRetryable = failureRetryable,
        failureKind = BackendFailureKind.valueOf(failureKind),
    )
}

@Serializable
private data class BalanceDto(
    val available: Long = 0,
    val receiving: Long = 0,
    val sending: Long = 0,
    val maturing: Long = 0,
    val shielded: Long = 0,
) {
    fun toDomain(authoritative: Boolean): BeamBalance = BeamBalance(
        available = available,
        receiving = receiving,
        sending = sending,
        maturing = maturing,
        shielded = shielded,
        isAuthoritative = authoritative,
    )
}

@Serializable
private data class AddressDto(val token: String, val type: String, val network: String) {
    fun toDomain(): BeamAddress = BeamAddress(
        token = token,
        type = BeamAddressType.valueOf(type),
        network = BeamNetwork.valueOf(network),
    )
}

@Serializable
private data class TransactionDto(
    val id: String,
    val direction: String,
    val amount: Long,
    val fee: Long,
    val createdAtEpochSeconds: Long,
    val minHeight: Long? = null,
    val proofHeight: Long? = null,
    val kernelId: String? = null,
    val status: String,
    val failureReason: String? = null,
) {
    fun toDomain(): BeamTransaction = BeamTransaction(
        id = id,
        direction = BeamTransactionDirection.valueOf(direction),
        amount = amount,
        fee = fee,
        createdAtEpochSeconds = createdAtEpochSeconds,
        minHeight = minHeight,
        proofHeight = proofHeight,
        kernelId = kernelId,
        status = BeamTransactionStatus.valueOf(status),
        failureReason = failureReason,
    )
}

@Serializable
private data class PreviewDto(
    val requestHash: String,
    val previewVersion: Long,
    val amount: Long,
    val fee: Long,
    val total: Long,
    val receiverType: String,
) {
    fun toDomain(): BeamSendPreview = BeamSendPreview(
        requestHash = requestHash,
        previewVersion = previewVersion,
        amount = amount,
        fee = fee,
        total = total,
        receiverType = BeamAddressType.valueOf(receiverType),
    )
}

@Serializable
private data class PreparedDto(val transactionId: String)

@Serializable
private data class ResolutionDto(
    val kind: String,
    val transactionId: String? = null,
    val status: String? = null,
)

private fun String.toResolution(): BeamSendResolution {
    val dto = json.decodeFromString<ResolutionDto>(this)
    return when (dto.kind) {
        "NotPrepared" -> BeamSendResolution.NotPrepared
        "Prepared" -> BeamSendResolution.Prepared(requireNotNull(dto.transactionId))
        "Committing" -> BeamSendResolution.Committing(requireNotNull(dto.transactionId))
        "Submitted" -> BeamSendResolution.Submitted(requireNotNull(dto.transactionId))
        "Indeterminate" -> BeamSendResolution.Indeterminate(requireNotNull(dto.transactionId))
        "Terminal" -> BeamSendResolution.Terminal(
            transactionId = requireNotNull(dto.transactionId),
            status = BeamTransactionStatus.valueOf(requireNotNull(dto.status)),
        )
        else -> throw BeamFailure.Native("Unknown send resolution: ${dto.kind}")
    }
}
