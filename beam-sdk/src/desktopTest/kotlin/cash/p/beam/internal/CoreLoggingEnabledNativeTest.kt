package cash.p.beam.internal

import kotlin.io.path.fileSize
import kotlin.test.Test
import kotlin.test.assertTrue

class CoreLoggingEnabledNativeTest {
    @Test
    fun logLevelInfo_writesAFileUnderTheStoragePath() {
        val storage = CoreLogging.newStorage("info")
        CoreLogging.openAndClose(storage, CoreLogging.INFO)
        val files = CoreLogging.logFiles(storage)
        assertTrue(files.isNotEmpty(), "BeamLogLevel.Info wrote no log file under $storage/logs")
        assertTrue(
            files.any { it.fileSize() > 0 },
            "BeamLogLevel.Info created a log file but wrote nothing into it: $files",
        )
    }
}
