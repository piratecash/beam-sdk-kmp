package cash.p.beam

import cash.p.beam.internal.BeamCodecPlatform

/**
 * Stateless native-BEAM signed transaction inspector; never creates a wallet or adds history.
 * Decodes at most 1 MiB with at most 256 elements per vector, 1024 aggregate vector elements,
 * and Core's parsing depth limit of two. Accepted one-sided shape has nesting depth one:
 * one main standard kernel containing one shielded recipient output, plus shielded inputs.
 * Asset-control kernels, interactive/aggregated transactions and unsupported proof parameters are rejected.
 * Core asset-hiding proofs are required even for native BEAM and retain Core validation;
 * confidential raw bytes do not prove asset identity.
 * These are SDK admission limits, not a promise to accept every consensus-valid transaction.
 *
 * Checks exact canonical reserialization, shape, rules fingerprint and Core context-free
 * validity. Core Transaction.IsValid does NOT verify the shielded spend proof against chain
 * context. Relay must obtain contextual node validation; this API proves neither current
 * spendability nor amounts/recipient identity from confidential bytes. A matching fingerprint
 * selects validation rules; raw Transaction bytes do not authenticate a source network.
 * Calls are synchronous and may verify range proofs: use a worker dispatcher for UI callers.
 */
public object BeamTransactionInspector {
    public const val MAX_TRANSACTION_BYTES: Int = 1_048_576

    /** Returns this SDK's supported rules without opening a session or contacting a node. */
    public fun supportedRules(network: BeamNetwork): BeamTransactionRules = BeamCodecPlatform.rules(network)

    /**
     * Throws IllegalArgumentException for malformed/oversized bytes, unsupported shape or
     * mismatched rules. The height range is checked under these rules, not against a live tip.
     * [bytes] must not be concurrently modified while this call copies them into native memory.
     */
    public fun inspect(bytes: ByteArray, rules: BeamTransactionRules): BeamInspectedTransaction {
        require(bytes.isNotEmpty() && bytes.size <= MAX_TRANSACTION_BYTES) { "Invalid transaction length" }
        require(rules.signature.length in 1..2048 && rules.signature.all {
            it.code in 32..126 || it == '\n' || it == '\t'
        }) {
            "Invalid rules signature"
        }
        return BeamCodecPlatform.inspect(bytes, rules)
    }
}
