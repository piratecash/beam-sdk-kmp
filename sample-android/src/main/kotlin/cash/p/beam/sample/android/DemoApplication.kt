package cash.p.beam.sample.android

import android.app.Application
import co.touchlab.kermit.Logger
import co.touchlab.kermit.Severity
import co.touchlab.kermit.platformLogWriter

public class DemoApplication : Application() {
    override fun onCreate() {
        super.onCreate()
        Logger.setLogWriters(platformLogWriter())
        Logger.setMinSeverity(Severity.Debug)
    }
}
