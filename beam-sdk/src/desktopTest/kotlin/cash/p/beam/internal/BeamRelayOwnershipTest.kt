package cash.p.beam.internal

import cash.p.beam.BeamNetwork
import cash.p.beam.BeamRelayConfig
import cash.p.beam.BeamTransactionInspector
import cash.p.beam.BeamTransactionRelay
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class BeamRelayOwnershipTest {
    private class HeldRunner(private val fail: Boolean = false) : RelayNativeCalls {
        val started = CountDownLatch(1)
        val cancelled = CountDownLatch(1)
        val release = CountDownLatch(1)
        val drained = AtomicBoolean(false)
        val destroyed = AtomicInteger()
        override fun run(handle: Long, timeoutMillis: Long): Int {
            started.countDown()
            check(release.await(5, TimeUnit.SECONDS)) { "Test runner was not released" }
            drained.set(true)
            if (fail) error("Synthetic runner failure")
            return 0 // A late acceptance racing cancellation must not escape cancellation.
        }
        override fun cancel(handle: Long) { cancelled.countDown() }
        override fun destroy(handle: Long) {
            check(drained.get()) { "Handle destroyed before native drain" }
            destroyed.incrementAndGet()
        }
    }

    @Test
    fun cancellationWaitsForDrainAndDiscardsLateAcceptance() = runBlocking {
        val runner = HeldRunner()
        val operation = async(Dispatchers.Default) { awaitRelayNative(1, 1000, runner) }
        try {
            assertTrue(runner.started.await(5, TimeUnit.SECONDS))
            operation.cancel()
            assertTrue(runner.cancelled.await(5, TimeUnit.SECONDS))
            assertFalse(operation.isCompleted)
            assertEquals(0, runner.destroyed.get())
        } finally {
            runner.release.countDown()
            operation.cancelAndJoin()
        }
        assertTrue(operation.isCancelled)
        assertEquals(1, runner.destroyed.get())
    }

    @Test
    fun successAndFailureDestroyExactlyOnceAfterDrain() = runBlocking {
        for (fail in listOf(false, true)) {
            val runner = HeldRunner(fail)
            runner.release.countDown()
            if (fail) assertFailsWith<IllegalStateException> { awaitRelayNative(1, 1000, runner) }
            else assertEquals(0, awaitRelayNative(1, 1000, runner))
            assertEquals(1, runner.destroyed.get())
        }
    }

    @Test
    fun mismatchedRulesAndNativeBoundsRejectBeforeAnyRelayHandle() = runBlocking<Unit> {
        NativeLibraryLoader.load()
        val rules = BeamTransactionInspector.supportedRules(BeamNetwork.Mainnet)
        assertFailsWith<IllegalArgumentException> {
            BeamTransactionRelay.relay(byteArrayOf(1), BeamRelayConfig(rules.copy(network = BeamNetwork.Testnet)))
        }
        assertFailsWith<IllegalArgumentException> {
            BeamRelayNative.create(ByteArray(1_048_577), 0, rules.signature)
        }
        assertFailsWith<IllegalArgumentException> {
            BeamRelayNative.create(byteArrayOf(1), 1, rules.signature)
        }
    }
}
