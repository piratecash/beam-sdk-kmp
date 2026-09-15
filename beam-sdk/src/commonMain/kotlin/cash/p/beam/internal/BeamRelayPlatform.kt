package cash.p.beam.internal

import cash.p.beam.BeamRelayConfig
import cash.p.beam.BeamRelayResult

internal expect object BeamRelayPlatform {
    suspend fun relay(bytes: ByteArray, config: BeamRelayConfig): BeamRelayResult
}
