package cash.p.beam.internal

import kotlin.test.Test
import kotlin.test.assertEquals

class NativeLibrarySmokeTest {
    @Test
    fun packagedHostLibrary_loadsAndReportsPinnedCore() {
        assertEquals(
            "beam-7.5.14493+9c4366aae08e7fbde7bc8d65f828a0deace68bfc",
            nativeVersionForTests(),
        )
    }

    @Test
    fun jniStrings_roundTripStandardUtf8IncludingSupplementaryCharacters() {
        val value = "Beam 🚀 🪙 \u0000 KMP"
        assertEquals(value, nativeStringRoundTripForTests(value))
    }
}
