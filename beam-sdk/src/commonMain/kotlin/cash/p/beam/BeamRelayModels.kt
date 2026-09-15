package cash.p.beam

/** Explicit validation rules and network deadline; endpoints are SDK managed. */
public data class BeamRelayConfig(
    val rules: BeamTransactionRules,
    val timeoutMillis: Long = 30_000,
) {
    init {
        require(timeoutMillis in 1..120_000) { "Relay timeout must be 1..120000 milliseconds" }
    }
}

/** Identity is recomputed from the exact canonical bytes before any network access. */
public data class BeamRelayResult(
    val transaction: BeamInspectedTransaction,
    val outcome: BeamRelayOutcome,
)

public sealed interface BeamRelayOutcome {
    /** A node accepted the transaction. This is neither confirmation nor finality. */
    public data object Accepted : BeamRelayOutcome

    /** This node declined this attempt. Earlier/other attempts may still be accepted. */
    public data class Rejected(val nodeStatus: Int) : BeamRelayOutcome

    /** Deadline expired. When true, bytes may have reached a node without an answer. */
    public data class Timeout(val acceptanceUnknown: Boolean) : BeamRelayOutcome

    /** Transport failed after entering the send boundary; acceptance cannot be determined. */
    public data object UnknownAcceptance : BeamRelayOutcome

    /** No transaction send was attempted by this call. Says nothing about previous calls. */
    public data object NetworkUnavailable : BeamRelayOutcome
}
