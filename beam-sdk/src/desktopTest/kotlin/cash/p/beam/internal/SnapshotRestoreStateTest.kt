package cash.p.beam.internal

import cash.p.beam.RestoreSource
import java.nio.file.Files
import kotlin.io.path.deleteIfExists
import kotlin.io.path.writeBytes
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertNull

class SnapshotRestoreStateTest {
    @Test
    fun resumedImport_successThenStopStart_doesNotRequestAnotherDownload() {
        val candidate = Files.createTempFile("beam-snapshot-resumed-", ".bin")
        candidate.writeBytes(byteArrayOf(1))
        try {
            var state = SnapshotRestoreState(
                pendingSource = RestoreSource.SnapshotThenScan(),
                downloadedFile = candidate,
            )

            // Reopen resumes native import from the completed candidate without downloading it again.
            assertNull(state.sourceToDownload)

            state = state.afterSuccessfulImport()

            assertFalse(Files.exists(candidate))
            assertNull(state.pendingSource)
            assertNull(state.downloadedFile)
            // A later Stop -> Start sees neither a candidate nor an obsolete download intent.
            assertNull(state.sourceToDownload)
        } finally {
            candidate.deleteIfExists()
        }
    }
}
