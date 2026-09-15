package cash.p.beam.internal

import cash.p.beam.BeamInspectedTransaction
import cash.p.beam.BeamNetwork
import cash.p.beam.BeamParsedToken
import cash.p.beam.BeamTransactionRules

internal expect object BeamCodecPlatform {
    fun parseToken(token: String): BeamParsedToken
    fun rules(network: BeamNetwork): BeamTransactionRules
    fun inspect(bytes: ByteArray, rules: BeamTransactionRules): BeamInspectedTransaction
}
