package cash.p.beam.internal

import kotlin.test.Test
import kotlin.test.assertTrue

class CoreLoggingDisabledNativeTest {
    @Test
    fun logLevelNone_writesNoFileAnywhere() {
        val storage = CoreLogging.newStorage("none")
        CoreLogging.openAndClose(storage, CoreLogging.NONE)
        val files = CoreLogging.logFiles(storage)
        // None is the shipped default, so this is the behaviour every consumer gets unopted.
        assertTrue(files.isEmpty(), "BeamLogLevel.None wrote log files: $files")
    }
}
