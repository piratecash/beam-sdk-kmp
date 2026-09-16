package cash.p.beam

import kotlinx.datetime.LocalDate

public const val BEAM_DECIMALS: Int = 8
public const val BEAM_ASSET_ID: UInt = 0u

public enum class BeamNetwork {
    Mainnet,
    Testnet,
}

public data class BeamSdkConfig(
    val network: BeamNetwork,
    val storagePath: String,
    val logLevel: BeamLogLevel = BeamLogLevel.Info,
)

public enum class BeamLogLevel {
    None,
    Error,
    Info,
    Debug,
}

public sealed interface RestoreSource {
    /**
     * Restores wallet records at and after [heightInclusive].
     *
     * The current trust-minimized implementation still scans the prefix from genesis in
     * count-only mode to reconstruct the global shielded-output index. Consequently, a recent
     * birthday does not imply low network traffic. A 2026-09-07 mainnet measurement for a wallet
     * created less than one day earlier received 1.74 GB at approximately 7.5% progress; a purely
     * linear projection would be about 23.2 GB at completion, although real block sizes make that
     * projection approximate. Prefer [SnapshotThenScan] for end-user recovery until a trusted
     * recent shielded-count checkpoint can avoid the genesis-prefix scan.
     */
    public data class Height(val heightInclusive: Long) : RestoreSource

    /**
     * Resolves [utcDate] to a birthday height and then uses the same recovery path as [Height],
     * including its current genesis-prefix traffic characteristics.
     */
    public data class Date(val utcDate: LocalDate) : RestoreSource

    public data class SnapshotThenScan(
        val snapshotUrl: String? = null,
        val expectedSha256: String? = null,
    ) : RestoreSource

    public data object FullScan : RestoreSource
}

public sealed interface BeamWalletState {
    public data object Closed : BeamWalletState
    public data object Stopped : BeamWalletState
    public data object Connecting : BeamWalletState
    public data class Restoring(val progress: BeamRestoreProgress) : BeamWalletState
    public data class Syncing(val currentHeight: Long, val targetHeight: Long) : BeamWalletState
    public data class Ready(val height: Long) : BeamWalletState
    public data class Offline(val lastKnownHeight: Long?) : BeamWalletState
    public data class Error(val failure: BeamFailure) : BeamWalletState
}

public enum class BeamRestorePhase {
    ResolvingBirthday,
    DownloadingSnapshot,
    ValidatingSnapshot,
    CountingShieldedOutputs,
    ScanningWalletOutputs,
    ImportingSnapshot,
    CatchingUp,
}

public data class BeamRestoreProgress(
    val phase: BeamRestorePhase,
    val currentHeight: Long? = null,
    val targetHeight: Long? = null,
    val downloadedBytes: Long? = null,
    val totalBytes: Long? = null,
)

/**
 * BEAM balance buckets following the wallet protocol API.
 *
 * [available] is the total currently spendable amount, including [shielded]. The shielded value
 * is exposed as a diagnostic subset, not an additional amount to add to the total.
 */
public data class BeamBalance(
    val available: Long = 0,
    val receiving: Long = 0,
    val sending: Long = 0,
    val maturing: Long = 0,
    val shielded: Long = 0,
    val isAuthoritative: Boolean = false,
)

public enum class BeamAddressType {
    Offline,
    PublicOffline,
    MaxPrivacy,
}

public data class BeamAddress(
    val token: String,
    val type: BeamAddressType,
    val network: BeamNetwork,
)

public enum class BeamTransactionDirection {
    Incoming,
    Outgoing,
    Self,
}

public enum class BeamTransactionStatus {
    Pending,
    InProgress,
    Registering,
    Confirming,
    Completed,
    Failed,
    Canceled,
    Unknown,
}

public data class BeamTransaction(
    val id: String,
    val direction: BeamTransactionDirection,
    val amount: Long,
    val fee: Long,
    val createdAtEpochSeconds: Long,
    val minHeight: Long?,
    val proofHeight: Long?,
    val kernelId: String?,
    val status: BeamTransactionStatus,
    val failureReason: String?,
    // The other party, in whatever form beam core holds it: a payment token, a base58 endpoint, or a
    // wallet id as hex. Null when the protocol reports none. Display only - parse or send to none of
    // these forms; an endpoint in particular is an identity key, not a payable address.
    val counterparty: String? = null,
)

public data class BeamTransactionPage(
    val items: List<BeamTransaction>,
    val nextOffset: Int?,
)

public data class BeamSendRequest(
    val receiverToken: String,
    val amount: Long,
    val comment: String = "",
)

public data class BeamSendPreview(
    val requestHash: String,
    val previewVersion: Long,
    val amount: Long,
    val fee: Long,
    val total: Long,
    val receiverType: BeamAddressType,
)

public data class PreparedBeamSend(
    val operationId: String,
    val transactionId: String,
)

/** A durable send owned by this wallet database and network. */
public data class BeamSendOperation(
    val operationId: String,
    val transactionId: String,
    val requestHash: String,
    val amount: Long,
    val fee: Long,
    val resolution: BeamSendResolution,
    val deliveryMode: BeamSendDeliveryMode = BeamSendDeliveryMode.Online,
    val offlineState: BeamOfflineSendState? = null,
    val contextId: String? = null,
    val rules: String? = null,
    val serializedHash: String? = null,
    val mainKernelId: String? = null,
    /** Original kernel and all inputs observed at this height; zero means unconfirmed/unknown. */
    val observedProofHeight: Long = 0,
)

public sealed interface BeamSendResolution {
    public data object NotPrepared : BeamSendResolution
    public data class Prepared(val transactionId: String) : BeamSendResolution
    public data class Committing(val transactionId: String) : BeamSendResolution
    public data class Submitted(val transactionId: String) : BeamSendResolution
    public data class Indeterminate(val transactionId: String) : BeamSendResolution
    public data class Terminal(
        val transactionId: String,
        val status: BeamTransactionStatus,
    ) : BeamSendResolution
}

public sealed class BeamFailure(
    message: String,
    cause: Throwable? = null,
    public val retryable: Boolean,
) : Exception(message, cause) {
    public class ContextUnavailable(message: String) : BeamFailure(message, retryable = true)
    public class StaleQuote(message: String) : BeamFailure(message, retryable = true)
    public class SendBusy(message: String) : BeamFailure(message, retryable = true)
    public class InvalidAddress(message: String) : BeamFailure(message, retryable = false)
    public class QuoteUnavailable(message: String) : BeamFailure(message, retryable = true)
    public class SigningInterrupted(message: String) : BeamFailure(message, retryable = true)
    public class OperationConflict(message: String) : BeamFailure(message, retryable = false)
    public class Validation(message: String) : BeamFailure(message, retryable = false)
    public class InsufficientFunds(message: String) : BeamFailure(message, retryable = false)
    /** Admission postponed by an unresolved outgoing transaction. Retry with the same operationId. */
    public class SendAdmissionDeferred(message: String, cause: Throwable? = null) :
        BeamFailure(message, cause, retryable = true)
    public class Node(message: String, cause: Throwable? = null) :
        BeamFailure(message, cause, retryable = true)
    public class Quorum(message: String) : BeamFailure(message, retryable = true)
    public class Download(
        message: String,
        cause: Throwable? = null,
        retryable: Boolean = true,
    ) : BeamFailure(message, cause, retryable)
    public class Storage(message: String, cause: Throwable? = null) :
        BeamFailure(message, cause, retryable = false)
    public class Native(message: String, cause: Throwable? = null) :
        BeamFailure(message, cause, retryable = false)
    public class Unsupported(message: String) : BeamFailure(message, retryable = false)
    public class Cancelled : BeamFailure("Operation cancelled", retryable = true)
}

/** Readiness of a saved signing context, independent of connectivity and the unknown live tip.
 * Ready does not promise current chain acceptance, nor authorize signing/export or broadcast.
 */
public sealed interface BeamOfflineSigningState {
    public data object Unavailable : BeamOfflineSigningState
    public data object Preparing : BeamOfflineSigningState
    public data class Ready(
        val contextId: String,
        val height: Long,
        val shieldedCount: Long,
    ) : BeamOfflineSigningState
    public data object Invalidated : BeamOfflineSigningState
}
