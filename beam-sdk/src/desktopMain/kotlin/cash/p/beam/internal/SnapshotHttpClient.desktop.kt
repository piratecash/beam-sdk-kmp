package cash.p.beam.internal

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO

internal actual fun platformSnapshotHttpClient(): HttpClient = HttpClient(CIO) {
    followRedirects = false
    expectSuccess = false
}
