package cash.p.beam.internal

import cash.p.beam.BeamFailure
import kotlin.test.Test
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue
import org.junit.Assume.assumeTrue

class SendAdmissionNativeTest {
    @Test
    fun prepareAdmission_mapsActualNativeExceptionToRetryableFailure() = checkAdmission(commit = false)

    @Test
    fun commitAdmission_mapsActualNativeExceptionToRetryableFailure() = checkAdmission(commit = true)

    private fun checkAdmission(commit: Boolean) {
        val fixture = System.getenv("BEAM_SEND_ADMISSION_FIXTURE")
        if (System.getenv("BEAM_EXPECT_SEND_ADMISSION_FIXTURE") == "1") {
            check(!fixture.isNullOrBlank()) { "Required send admission JNI fixture was not provided" }
        }
        assumeTrue("Requires the dedicated send admission JNI fixture", !fixture.isNullOrBlank())
        System.load(checkNotNull(fixture))
        val error = runCatching { deferFromNative(commit) }.exceptionOrNull()
        val native = assertIs<IllegalStateException>(error)
        assertTrue(native.message.orEmpty().startsWith("BEAM_SEND_ADMISSION_DEFERRED|"))
        // Both backend prepareSend and commitSend use this same nativeCall mapping.
        val failure = assertIs<BeamFailure.SendAdmissionDeferred>(native.asBeamFailure())
        assertTrue(failure.retryable)
        assertSame(native, failure.cause)
        assertSame(failure, failure.asBeamFailure())
    }

    // Exists only in the dedicated native fixture library, never in the production API.
    private external fun deferFromNative(commit: Boolean)
}
