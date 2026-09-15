package cash.p.beam.internal

import cash.p.beam.BeamRelayConfig
import cash.p.beam.BeamRelayOutcome
import cash.p.beam.BeamRelayResult
import cash.p.beam.BeamTransactionInspector
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

internal actual object BeamRelayPlatform {
    actual suspend fun relay(bytes: ByteArray, config: BeamRelayConfig): BeamRelayResult = withContext(Dispatchers.IO) {
        val inspected = BeamTransactionInspector.inspect(bytes, config.rules)
        ensureActive()
        val handle = BeamRelayNative.create(bytes, config.rules.network.ordinal, config.rules.signature)
        val encoded = awaitRelayNative(handle, config.timeoutMillis)
        val outcome = when (encoded) {
            0 -> BeamRelayOutcome.Accepted
            1 -> BeamRelayOutcome.NetworkUnavailable
            2 -> BeamRelayOutcome.UnknownAcceptance
            3 -> BeamRelayOutcome.Timeout(false)
            4 -> BeamRelayOutcome.Timeout(true)
            in 256..511 -> BeamRelayOutcome.Rejected(encoded - 256)
            else -> error("Invalid native relay outcome")
        }
        BeamRelayResult(inspected, outcome)
    }
}

/** The same ownership path is exercised with a latch-controlled runner in desktop tests. */
internal suspend fun awaitRelayNative(
    handle: Long,
    timeoutMillis: Long,
    calls: RelayNativeCalls = ProductionRelayNativeCalls,
): Int {
    val owner = RelayNativeOwner(handle, calls)
    try {
        // Cancellation signals the native runner immediately. This scope cannot finish
        // (and destroy its handle) until its blocking child has drained all native I/O.
        return coroutineScope {
            suspendCancellableCoroutine { continuation ->
                continuation.invokeOnCancellation { owner.cancel() }
                launch(Dispatchers.IO) {
                    try {
                        val result = calls.run(handle, timeoutMillis)
                        if (result == 5 || result == 6) continuation.cancel()
                        else continuation.resume(result)
                    } catch (failure: Throwable) {
                        continuation.resumeWithException(failure)
                    }
                }
            }
        }
    } finally {
        owner.close()
    }
}

internal interface RelayNativeCalls {
    fun run(handle: Long, timeoutMillis: Long): Int
    fun cancel(handle: Long)
    fun destroy(handle: Long)
}

private object ProductionRelayNativeCalls : RelayNativeCalls {
    override fun run(handle: Long, timeoutMillis: Long): Int = BeamRelayNative.run(handle, timeoutMillis)
    override fun cancel(handle: Long) = BeamRelayNative.cancel(handle)
    override fun destroy(handle: Long) = BeamRelayNative.destroy(handle)
}

/** Lock excludes a concurrent cancellation hook from destruction, including late hooks. */
private class RelayNativeOwner(val handle: Long, val calls: RelayNativeCalls) {
    private var closed = false
    @Synchronized fun cancel() {
        if (!closed) calls.cancel(handle)
    }
    @Synchronized fun close() {
        if (!closed) {
            closed = true
            calls.destroy(handle)
        }
    }
}

// Private ownership protocol: create, at most one run, cancel concurrently, destroy after join.
internal object BeamRelayNative {
    external fun create(bytes: ByteArray, network: Int, rules: String): Long
    external fun run(handle: Long, timeoutMillis: Long): Int
    external fun cancel(handle: Long)
    external fun destroy(handle: Long)
}
