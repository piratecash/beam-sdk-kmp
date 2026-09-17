package cash.p.beam.internal

import kotlin.test.Test
import kotlin.test.assertTrue

class CoreLoggingFirstOpenWinsNativeTest {
    @Test
    fun secondOpenWithADifferentLevel_keepsTheFirstConfiguration() {
        val first = CoreLogging.newStorage("first")
        val second = CoreLogging.newStorage("second")

        CoreLogging.openAndClose(first, CoreLogging.NONE)
        // A later session asks for Debug. Beam core's logger is process-global and its create()
        // throws while one exists, so the request is ignored rather than honoured.
        CoreLogging.openAndClose(second, CoreLogging.DEBUG)

        assertTrue(
            CoreLogging.logFiles(second).isEmpty(),
            "the second open re-bound the process logger to its own level and directory",
        )
        assertTrue(
            CoreLogging.logFiles(first).isEmpty(),
            "the first open bound None yet a log file appeared",
        )
    }
}
