package cash.p.beam

import cash.p.beam.internal.BeamRelayPlatform

/**
 * Relays a foreign or locally signed native-BEAM one-sided transaction without a wallet.
 * No account, storage, history or reservations are created or changed.
 *
 * The exact canonical bytes undergo BeamTransactionInspector validation before networking.
 * Node acceptance supplies contextual validation, not confirmation. Retry identical bytes;
 * a reject, timeout, cancellation or disconnect never authorizes releasing sender reservations.
 */
public object BeamTransactionRelay {
    /**
     * Malformed/unsupported input or mismatched rules throws IllegalArgumentException.
     * The caller must not mutate [bytes] while this call makes its private copy.
     *
     * Cancellation throws CancellationException only after native transport and callbacks
     * have drained. It does not prove the transaction was not sent. The configured timeout
     * covers DNS/connection/submission, after local validation. OS DNS cleanup may outlast
     * that deadline; no new sends are admitted after it expires.
     */
    public suspend fun relay(bytes: ByteArray, config: BeamRelayConfig): BeamRelayResult {
        require(bytes.isNotEmpty() && bytes.size <= BeamTransactionInspector.MAX_TRANSACTION_BYTES) {
            "Invalid transaction length"
        }
        return BeamRelayPlatform.relay(bytes.copyOf(), config)
    }
}
