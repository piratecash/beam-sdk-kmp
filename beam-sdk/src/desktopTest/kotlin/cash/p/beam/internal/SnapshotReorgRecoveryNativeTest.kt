package cash.p.beam.internal

import kotlin.test.Test
import kotlin.test.assertEquals
import org.junit.Assume.assumeTrue

class SnapshotReorgRecoveryNativeTest {
    @Test
    fun snapshotReorg_executesNativeAdversarialRecoveryFixture() {
        val fixture = System.getenv("BEAM_SNAPSHOT_REORG_FIXTURE")
        if (System.getenv("BEAM_EXPECT_SNAPSHOT_REORG_FIXTURE") == "1") {
            check(!fixture.isNullOrBlank()) { "Required snapshot reorg JNI fixture was not provided" }
        }
        assumeTrue("Requires the dedicated snapshot reorg JNI fixture", !fixture.isNullOrBlank())
        System.load(checkNotNull(fixture))
        assertEquals("SNAPSHOT_REORG_JNI_OK", runNativeFixture())
        println("SNAPSHOT_REORG_JNI_OK")
    }

    private external fun runNativeFixture(): String
}
