package cash.p.beam

import cash.p.beam.internal.BeamCodecPlatform

/**
 * Stateless native-BEAM one-sided token parser. Requires no wallet, keys, database,
 * session, Ready state, amount or network, and makes no network requests.
 *
 * Uses pinned Core token/parameter encodings and address classification; additionally
 * rejects trailing/noncanonical binary fields and bounds allocations. Supports Offline,
 * PublicOffline (including legacy public generators), and MaxPrivacy. Voucher signatures
 * are checked where present; public-generator parsing does not authenticate its publisher.
 * This does not replace send-preview admission, fee, voucher-availability or spend checks.
 */
public object BeamTokenParser {
    public const val MAX_TOKEN_CHARACTERS: Int = 65_536

    /** Throws IllegalArgumentException for malformed, unsupported or oversized input. */
    public fun parse(token: String): BeamParsedToken {
        require(token.isNotEmpty() && token.length <= MAX_TOKEN_CHARACTERS) { "Invalid token length" }
        require(token.all { it in '0'..'9' || it in 'A'..'Z' || it in 'a'..'z' }) {
            "Token must be ASCII hex or Base58"
        }
        return BeamCodecPlatform.parseToken(token)
    }
}
