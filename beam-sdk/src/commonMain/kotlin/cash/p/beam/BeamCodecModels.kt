package cash.p.beam

/** Source-independent token metadata. No network identifier is encoded by these Core tokens. */
public data class BeamParsedToken(
    val type: BeamAddressType,
    val voucherCount: Int,
)

/** Exact pinned Core rules fingerprint, paired with the explicitly selected network. */
public data class BeamTransactionRules(
    val network: BeamNetwork,
    val signature: String,
)

/** Inclusive heights; ULong preserves Core's unlimited upper bound without signed overflow. */
public data class BeamTransactionHeightRange(val minimum: ULong, val maximum: ULong)

/**
 * Structural/context-free inspection only. [serializedTransactionHash] is SHA-256 of all
 * canonical serialized Transaction bytes. [mainKernelId] identifies the unique top-level
 * standard kernel, even when normalized shielded inputs precede it.
 *
 * No amounts, receiver identity, ownership, confirmation or spendability are inferred.
 * A node must perform contextual validation, including shielded spend-proof verification,
 * at relay. Successful inspection does not prove that the bytes originated on [rules]' network.
 */
public data class BeamInspectedTransaction(
    val rules: BeamTransactionRules,
    val serializedTransactionHash: String,
    val mainKernelId: String,
    val mainKernelHeight: BeamTransactionHeightRange,
    val validHeight: BeamTransactionHeightRange,
    val ordinaryInputCount: Int,
    val ordinaryOutputCount: Int,
    val shieldedInputCount: Int,
    /** Includes the nested shielded recipient output. */
    val kernelCount: Int,
    val serializedSize: Int,
)
