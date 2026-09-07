package cash.p.beam.sample.desktop

import androidx.compose.ui.window.Window
import androidx.compose.ui.window.application
import cash.p.beam.sample.BeamDemoApp
import cash.p.beam.sample.createBeamDemoHandle
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import co.touchlab.kermit.platformLogWriter
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking

public fun main() {
    Logger.setLogWriters(platformLogWriter())
    Logger.setMinSeverity(Severity.Debug)
    val storage = Paths.get(System.getProperty("user.home"), ".beam-sdk-kmp-demo", "testnet").toString()
    val handle = createBeamDemoHandle(storage)
    application {
        Window(
            onCloseRequest = {
                runBlocking { handle.closeAndJoin() }
                exitApplication()
            },
            title = "Beam SDK KMP Demo",
        ) {
            BeamDemoApp(handle)
        }
    }
}
