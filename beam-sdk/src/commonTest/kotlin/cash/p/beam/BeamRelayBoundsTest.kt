package cash.p.beam

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFailsWith

class BeamRelayBoundsTest {
    private val rules = BeamTransactionRules(BeamNetwork.Mainnet, "synthetic")

    @Test
    fun invalidTimeoutsRejectWithoutNativeOrNetwork() {
        for (timeout in listOf(Long.MIN_VALUE, -1, 0, 120_001, Long.MAX_VALUE)) {
            assertFailsWith<IllegalArgumentException> { BeamRelayConfig(rules, timeout) }
        }
    }

    @Test
    fun byteBoundsRejectBeforeNativeLoad() = runTest {
        val config = BeamRelayConfig(rules)
        assertFailsWith<IllegalArgumentException> { BeamTransactionRelay.relay(byteArrayOf(), config) }
        assertFailsWith<IllegalArgumentException> {
            BeamTransactionRelay.relay(ByteArray(BeamTransactionInspector.MAX_TRANSACTION_BYTES + 1), config)
        }
    }
}
