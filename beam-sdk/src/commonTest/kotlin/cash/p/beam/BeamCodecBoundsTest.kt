package cash.p.beam

import kotlin.test.Test
import kotlin.test.assertFailsWith

class BeamCodecBoundsTest {
    @Test
    fun tokenBoundsRejectBeforeNativeLoad() {
        for (token in listOf("", " ", "beam:abc", "é", "\u0000", "1".repeat(BeamTokenParser.MAX_TOKEN_CHARACTERS + 1))) {
            assertFailsWith<IllegalArgumentException> { BeamTokenParser.parse(token) }
        }
    }

    @Test
    fun transactionBoundsRejectBeforeNativeLoad() {
        val rules = BeamTransactionRules(BeamNetwork.Mainnet, "test")
        assertFailsWith<IllegalArgumentException> { BeamTransactionInspector.inspect(byteArrayOf(), rules) }
        assertFailsWith<IllegalArgumentException> {
            BeamTransactionInspector.inspect(ByteArray(BeamTransactionInspector.MAX_TRANSACTION_BYTES + 1), rules)
        }
        assertFailsWith<IllegalArgumentException> {
            BeamTransactionInspector.inspect(byteArrayOf(1), rules.copy(signature = ""))
        }
    }
}
