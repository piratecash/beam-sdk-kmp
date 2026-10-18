package cash.p.beam.internal

import cash.p.beam.BeamAddressType
import cash.p.beam.BeamNetwork
import cash.p.beam.BeamTokenParser
import cash.p.beam.BeamTransactionInspector
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals

class StatelessCodecNativeTest {
    @Test
    fun rulesLoadConcurrentlyWithoutSessionAndRemainNetworkScoped() {
        val results = arrayOfNulls<String>(8)
        val workers = results.indices.map { i ->
            thread { results[i] = BeamTransactionInspector.supportedRules(BeamNetwork.entries[i % 2]).signature }
        }
        workers.forEach { it.join() }
        results.indices.forEach { i ->
            assertEquals(BeamTransactionInspector.supportedRules(BeamNetwork.entries[i % 2]).signature, results[i])
        }
        assertNotEquals(results[0], results[1])
    }

    @Test
    fun nativeEntryPointsIndependentlyEnforceBoundsAndMapExceptions() {
        NativeLibraryLoader.load()
        assertFailsWith<IllegalArgumentException> { BeamCodecNative.parseToken("1".repeat(65_537)) }
        assertFailsWith<IllegalArgumentException> { BeamCodecNative.parseToken("\u0000") }
        assertFailsWith<IllegalArgumentException> { BeamCodecNative.rules(-1) }
        assertFailsWith<IllegalArgumentException> { BeamTokenParser.parse("0notatoken") }
        val rules = BeamTransactionInspector.supportedRules(BeamNetwork.Mainnet)
        assertFailsWith<IllegalArgumentException> { BeamCodecNative.inspect(ByteArray(1_048_577), 0, rules.signature) }
        assertFailsWith<IllegalArgumentException> { BeamTransactionInspector.inspect(byteArrayOf(1), rules) }
        assertFailsWith<IllegalArgumentException> { BeamCodecNative.inspect(byteArrayOf(1), 1, rules.signature) }
        // JVM exception state and native scopes must remain usable after failures.
        assertEquals(rules, BeamTransactionInspector.supportedRules(BeamNetwork.Mainnet))
    }

    @Test
    fun parse_externalSbbsAddress_isRegular() {
        // Real SBBS address produced by a third-party Beam web wallet.
        val sbbsAddress = "e17cc06481d9ae88e1e0181efee407fa8c36a861b9df723845eddc8fb1ba552048"
        assertEquals(BeamAddressType.Regular, BeamTokenParser.parse(sbbsAddress).type)
    }
}
