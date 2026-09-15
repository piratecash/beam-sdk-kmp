package cash.p.beam

/** Native BEAM only. Max is one Core transaction and may leave a remainder at input limits. */
public sealed interface BeamSendAmount {
    public data class Exact(val amount: Long) : BeamSendAmount
    public data object Max : BeamSendAmount
}

public sealed interface BeamSendContext {
    public data object Online : BeamSendContext
    /** Saved context identity from [BeamOfflineSigningState.Ready], not a claim about the live tip. */
    public data class Offline(val contextId: String) : BeamSendContext
}

public data class BeamQuoteRequest(
    val receiverToken: String,
    val amount: BeamSendAmount,
    val context: BeamSendContext,
    val comment: String = "",
)

/** Opaque version binds receiver, amount mode, context/rules and wallet/reservations.
 * No coins are reserved. A successful quote does not promise later chain acceptance.
 * [fee] includes shielded input fees; [explicitFee] is the main standard kernel fee.
 */
public data class BeamSendQuote(
    val amount: Long,
    val fee: Long,
    val total: Long,
    val explicitFee: Long,
    val change: Long,
    val remainder: Long,
    val ordinaryInputs: Int,
    val shieldedInputs: Int,
    val receiverType: BeamAddressType,
    val version: String,
    val contextId: String,
    val rules: String,
)

public enum class BeamSendDeliveryMode { Online, Offline }
public enum class BeamOfflineSendState { Signing, Signed, Exported }

/** Local ownership only, in the original encrypted WalletDB/network. Contains no signed bytes. */
public data class BeamOfflineSignResult(val transactionId: String, val state: BeamOfflineSendState)
