package cash.p.beam.internal

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.engine.cio.endpoint

internal actual fun platformSnapshotHttpClient(): HttpClient = HttpClient(CIO) {
    followRedirects = false
    expectSuccess = false
    engine {
        requestTimeout = 0
        endpoint {
            connectTimeout = 5_000
            socketTimeout = 60_000
        }
    }
}
