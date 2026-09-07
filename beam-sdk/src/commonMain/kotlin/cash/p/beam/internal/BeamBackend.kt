package cash.p.beam.internal

import cash.p.beam.BeamAddress
import cash.p.beam.BeamAddressType
import cash.p.beam.BeamBalance
import cash.p.beam.BeamSdkConfig
import cash.p.beam.BeamSendPreview
import cash.p.beam.BeamSendRequest
import cash.p.beam.BeamSendResolution
import cash.p.beam.BeamTransaction
import cash.p.beam.PreparedBeamSend
import cash.p.beam.RestoreSource
import kotlinx.coroutines.flow.StateFlow

internal interface BeamBackend {
    val snapshot: StateFlow<BackendSnapshot>

    suspend fun create(
        config: BeamSdkConfig,
        seed: ByteArray,
        databaseKey: ByteArray,
        restoreSource: RestoreSource?,
    )
    suspend fun open(config: BeamSdkConfig, databaseKey: ByteArray)
    suspend fun start()
    suspend fun stop()
    suspend fun close()
    suspend fun receiveAddress(type: BeamAddressType): BeamAddress
    suspend fun transactions(offset: Int, limit: Int): List<BeamTransaction>
    suspend fun previewSend(request: BeamSendRequest): BeamSendPreview
    suspend fun prepareSend(
        operationId: String,
        request: BeamSendRequest,
        previewVersion: Long,
    ): PreparedBeamSend
    suspend fun commitSend(operationId: String): BeamSendResolution
    suspend fun resolveSend(operationId: String): BeamSendResolution
    suspend fun abortPrepared(operationId: String): Boolean
}

internal data class BackendSnapshot(
    val phase: BackendPhase = BackendPhase.Stopped,
    val currentHeight: Long = 0,
    val targetHeight: Long = 0,
    val balance: BeamBalance = BeamBalance(),
    val transactions: List<BeamTransaction> = emptyList(),
    val restoreCurrent: Long? = null,
    val restoreTarget: Long? = null,
    val restoreBytes: Long? = null,
    val restoreTotalBytes: Long? = null,
    val failureMessage: String? = null,
    val failureRetryable: Boolean = true,
    val failureKind: BackendFailureKind = BackendFailureKind.Node,
)

internal enum class BackendFailureKind {
    Node,
    Quorum,
    Download,
    Storage,
    Native,
}

internal enum class BackendPhase {
    Stopped,
    Connecting,
    ResolvingBirthday,
    DownloadingSnapshot,
    ValidatingSnapshot,
    CountingShieldedOutputs,
    ScanningWalletOutputs,
    ImportingSnapshot,
    CatchingUp,
    Syncing,
    Ready,
    Offline,
    Error,
}

internal expect fun createPlatformBackend(): BeamBackend
