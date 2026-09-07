package cash.p.beam

import cash.p.beam.internal.BackendPhase
import cash.p.beam.internal.BeamBackend
import cash.p.beam.internal.createPlatformBackend
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

public class BeamWalletFactory internal constructor(
    private val backendProvider: () -> BeamBackend,
) {
    public constructor() : this(::createPlatformBackend)

    public suspend fun createNew(
        config: BeamSdkConfig,
        seed: ByteArray,
        databaseKey: ByteArray,
    ): BeamWalletSession = create(config, seed, databaseKey, source = null)

    public suspend fun restore(
        config: BeamSdkConfig,
        seed: ByteArray,
        databaseKey: ByteArray,
        source: RestoreSource,
    ): BeamWalletSession = create(config, seed, databaseKey, source = source)

    public suspend fun openExisting(
        config: BeamSdkConfig,
        databaseKey: ByteArray,
    ): BeamWalletSession {
        validateConfig(config)
        validateDatabaseKey(databaseKey)
        val backend = backendProvider()
        val keyCopy = databaseKey.copyOf()
        try {
            backend.open(config, keyCopy)
        } finally {
            keyCopy.fill(0)
        }
        return DefaultBeamWalletSession(backend, config.network)
    }

    private suspend fun create(
        config: BeamSdkConfig,
        seed: ByteArray,
        databaseKey: ByteArray,
        source: RestoreSource?,
    ): BeamWalletSession {
        validateConfig(config)
        require(seed.size == SEED_SIZE_BYTES) { "BIP39 seed must contain exactly 64 bytes" }
        validateDatabaseKey(databaseKey)
        validateRestoreSource(source)
        val backend = backendProvider()
        val seedCopy = seed.copyOf()
        val keyCopy = databaseKey.copyOf()
        try {
            // The restore source is part of the native create transaction. A process death can
            // therefore never turn a height/date/snapshot restore into a different scan mode.
            backend.create(config, seedCopy, keyCopy, source)
        } finally {
            seedCopy.fill(0)
            keyCopy.fill(0)
        }
        return DefaultBeamWalletSession(backend, config.network)
    }

    private fun validateConfig(config: BeamSdkConfig) {
        require(config.storagePath.isNotBlank()) { "storagePath must not be blank" }
    }

    private fun validateDatabaseKey(databaseKey: ByteArray) {
        require(databaseKey.size == DATABASE_KEY_SIZE_BYTES) {
            "databaseKey must contain exactly 32 random bytes"
        }
    }

    private fun validateRestoreSource(source: RestoreSource?) {
        if (source is RestoreSource.Height) {
            require(source.heightInclusive >= 0) { "Restore height must not be negative" }
        }
        if (source is RestoreSource.SnapshotThenScan) {
            require(source.snapshotUrl == null || source.snapshotUrl.isNotBlank()) {
                "snapshotUrl must be null or non-blank"
            }
            require(source.snapshotUrl == null || source.expectedSha256 != null) {
                "A custom snapshot URL requires a trusted SHA-256"
            }
            source.expectedSha256?.let { hash ->
                require(SHA256_PATTERN.matches(hash)) {
                    "expectedSha256 must contain 64 hexadecimal characters"
                }
            }
        }
    }

    private companion object {
        const val SEED_SIZE_BYTES = 64
        const val DATABASE_KEY_SIZE_BYTES = 32
        val SHA256_PATTERN = Regex("[0-9a-fA-F]{64}")
    }
}

private class DefaultBeamWalletSession(
    private val backend: BeamBackend,
    private val network: BeamNetwork,
) : BeamWalletSession {
    private val mutex = Mutex()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val mutableState = MutableStateFlow<BeamWalletState>(BeamWalletState.Stopped)
    private val mutableBalance = MutableStateFlow(BeamBalance())
    private val mutableTransactions = MutableStateFlow<List<BeamTransaction>>(emptyList())
    private var closed = false
    private var running = false
    private var starting = false
    private var stopping = false
    private var lifecycleGeneration = 0L
    private var startAttempt: Deferred<Unit>? = null

    override val state: StateFlow<BeamWalletState> = mutableState
    override val balance: StateFlow<BeamBalance> = mutableBalance
    override val transactions: StateFlow<List<BeamTransaction>> = mutableTransactions

    init {
        scope.launch {
            backend.snapshot.collectLatest { snapshot ->
                mutex.withLock {
                    if (closed) return@withLock
                    mutableBalance.value = snapshot.balance
                    mutableTransactions.value = snapshot.transactions
                    if (!stopping) mutableState.value = snapshot.toWalletState()
                }
            }
        }
    }

    override suspend fun start() {
        val (generation, attempt) = mutex.withLock {
            checkOpen()
            if (running || starting || stopping) return
            starting = true
            lifecycleGeneration += 1
            mutableState.value = BeamWalletState.Connecting
            mutableBalance.value = mutableBalance.value.copy(isAuthoritative = false)
            val deferred = scope.async(start = CoroutineStart.LAZY) { backend.start() }
            startAttempt = deferred
            lifecycleGeneration to deferred
        }
        try {
            attempt.start()
            attempt.await()
            currentCoroutineContext().ensureActive()
            mutex.withLock {
                currentCoroutineContext().ensureActive()
                if (!closed && lifecycleGeneration == generation && !stopping) running = true
            }
        } finally {
            withContext(NonCancellable) {
                attempt.cancelAndJoin()
                val cleanupBackend = mutex.withLock {
                    if (startAttempt === attempt) startAttempt = null
                    if (lifecycleGeneration == generation) {
                        starting = false
                        if (!running && !stopping && !closed) {
                            stopping = true
                            lifecycleGeneration += 1
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                }
                if (cleanupBackend) {
                    try {
                        backend.stop()
                    } finally {
                        mutex.withLock {
                            running = false
                            stopping = false
                            if (!closed) {
                                mutableState.value = BeamWalletState.Stopped
                                mutableBalance.value = mutableBalance.value.copy(isAuthoritative = false)
                            }
                        }
                    }
                }
            }
        }
    }

    override suspend fun stop() {
        val attempt = mutex.withLock {
            checkOpen()
            if ((!running && !starting) || stopping) return
            stopping = true
            lifecycleGeneration += 1
            startAttempt
        }
        withContext(NonCancellable) {
            try {
                attempt?.cancelAndJoin()
                backend.stop()
            } finally {
                mutex.withLock {
                    running = false
                    starting = false
                    stopping = false
                    if (!closed) {
                        mutableState.value = BeamWalletState.Stopped
                        mutableBalance.value = mutableBalance.value.copy(isAuthoritative = false)
                    }
                }
            }
        }
    }

    override suspend fun close() {
        val attempt = mutex.withLock {
            if (closed) return
            closed = true
            lifecycleGeneration += 1
            startAttempt
        }
        withContext(NonCancellable) {
            try {
                attempt?.cancelAndJoin()
                backend.close()
            } finally {
                mutex.withLock {
                    running = false
                    starting = false
                    stopping = false
                    mutableState.value = BeamWalletState.Closed
                    mutableBalance.value = BeamBalance()
                    mutableTransactions.value = emptyList()
                    scope.cancel()
                }
            }
        }
    }

    override suspend fun receiveAddress(type: BeamAddressType): BeamAddress = mutex.withLock {
        checkOpen()
        checkLifecycleStable()
        backend.receiveAddress(type).also { require(it.network == network) }
    }

    override suspend fun transactionPage(offset: Int, limit: Int): BeamTransactionPage = mutex.withLock {
        checkOpen()
        require(offset >= 0) { "offset must not be negative" }
        require(limit in 1..MAX_PAGE_SIZE) { "limit must be in 1..$MAX_PAGE_SIZE" }
        val items = backend.transactions(offset, limit)
        BeamTransactionPage(items, (offset + items.size).takeIf { items.size == limit })
    }

    override suspend fun previewSend(request: BeamSendRequest): BeamSendPreview = mutex.withLock {
        checkReady()
        validateRequest(request)
        backend.previewSend(request)
    }

    override suspend fun prepareSend(
        operationId: String,
        request: BeamSendRequest,
        previewVersion: Long,
    ): PreparedBeamSend = mutex.withLock {
        checkReady()
        validateOperationId(operationId)
        validateRequest(request)
        backend.prepareSend(operationId, request, previewVersion)
    }

    override suspend fun commitSend(operationId: String): BeamSendResolution = mutex.withLock {
        checkReady()
        validateOperationId(operationId)
        backend.commitSend(operationId)
    }

    override suspend fun resolveSend(operationId: String): BeamSendResolution {
        mutex.withLock {
            checkReconciliationAvailable()
            validateOperationId(operationId)
        }
        return backend.resolveSend(operationId)
    }

    override suspend fun abortPrepared(operationId: String): Boolean {
        mutex.withLock {
            checkReconciliationAvailable()
            validateOperationId(operationId)
        }
        return backend.abortPrepared(operationId)
    }

    private fun checkOpen() {
        check(!closed) { "Beam wallet session is closed" }
    }

    private fun checkReady() {
        checkOpen()
        checkLifecycleStable()
        check(state.value is BeamWalletState.Ready) { "Beam wallet is not ready" }
    }

    private fun checkReconciliationAvailable() {
        checkOpen()
        checkLifecycleStable()
    }

    private fun checkLifecycleStable() {
        check(!starting && !stopping) {
            "Beam wallet operation is unavailable during a lifecycle transition"
        }
    }

    private fun validateRequest(request: BeamSendRequest) {
        require(request.receiverToken.isNotBlank()) { "receiverToken must not be blank" }
        require(request.amount > 0) { "amount must be positive" }
        require(request.comment.encodeToByteArray().size <= MAX_COMMENT_BYTES) {
            "comment must not exceed $MAX_COMMENT_BYTES UTF-8 bytes"
        }
    }

    private fun validateOperationId(operationId: String) {
        require(operationId.matches(UUID_PATTERN)) { "operationId must be a canonical UUID" }
    }

    private companion object {
        const val MAX_PAGE_SIZE = 200
        const val MAX_COMMENT_BYTES = 1_024
        val UUID_PATTERN = Regex(
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89aAbB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}",
        )
    }
}

private fun cash.p.beam.internal.BackendSnapshot.toWalletState(): BeamWalletState = when (phase) {
    BackendPhase.Stopped -> BeamWalletState.Stopped
    BackendPhase.Connecting -> BeamWalletState.Connecting
    BackendPhase.ResolvingBirthday -> restoreState(BeamRestorePhase.ResolvingBirthday)
    BackendPhase.DownloadingSnapshot -> restoreState(BeamRestorePhase.DownloadingSnapshot)
    BackendPhase.ValidatingSnapshot -> restoreState(BeamRestorePhase.ValidatingSnapshot)
    BackendPhase.CountingShieldedOutputs -> restoreState(BeamRestorePhase.CountingShieldedOutputs)
    BackendPhase.ScanningWalletOutputs -> restoreState(BeamRestorePhase.ScanningWalletOutputs)
    BackendPhase.ImportingSnapshot -> restoreState(BeamRestorePhase.ImportingSnapshot)
    BackendPhase.CatchingUp -> restoreState(BeamRestorePhase.CatchingUp)
    BackendPhase.Syncing -> BeamWalletState.Syncing(currentHeight, targetHeight)
    BackendPhase.Ready -> BeamWalletState.Ready(currentHeight)
    BackendPhase.Offline -> BeamWalletState.Offline(currentHeight.takeIf { it > 0 })
    BackendPhase.Error -> BeamWalletState.Error(
        when (failureKind) {
            cash.p.beam.internal.BackendFailureKind.Node ->
                BeamFailure.Node(failureMessage ?: "Unknown Beam node failure")
            cash.p.beam.internal.BackendFailureKind.Quorum ->
                BeamFailure.Quorum(failureMessage ?: "Beam recovery quorum failed")
            cash.p.beam.internal.BackendFailureKind.Download ->
                BeamFailure.Download(
                    failureMessage ?: "Beam snapshot download failed",
                    retryable = failureRetryable,
                )
            cash.p.beam.internal.BackendFailureKind.Storage ->
                BeamFailure.Storage(failureMessage ?: "Beam storage failed")
            cash.p.beam.internal.BackendFailureKind.Native ->
                BeamFailure.Native(failureMessage ?: "Beam native failure")
        },
    )
}

private fun cash.p.beam.internal.BackendSnapshot.restoreState(
    phase: BeamRestorePhase,
): BeamWalletState = BeamWalletState.Restoring(
    BeamRestoreProgress(
        phase = phase,
        currentHeight = restoreCurrent,
        targetHeight = restoreTarget,
        downloadedBytes = restoreBytes,
        totalBytes = restoreTotalBytes,
    ),
)
