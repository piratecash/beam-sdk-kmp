#include "transaction_relay.h"
#include "codec_jni_support.h"
#include <new>
#include <stdexcept>

namespace {
using beam::sdk::TransactionRelay;
TransactionRelay& operation(jlong handle) {
    // Internal JNI only. Kotlin's owner excludes cancellation from destruction and
    // joins run before delete; there is no public arbitrary-handle entry point.
    if (!handle) throw std::invalid_argument("Missing relay operation");
    return *reinterpret_cast<TransactionRelay*>(handle);
}
template<class T, class F> T guarded(JNIEnv* env, T fallback, F&& action) {
    try { return action(); }
    catch (const beam::sdk::jni::PendingJavaException&) {
        // Preserve the exception already raised by the JVM.
    }
    catch (const std::bad_alloc&) {
        beam::sdk::jni::throwJava(env, "java/lang/OutOfMemoryError", "Beam relay allocation failed");
    } catch (const std::invalid_argument&) {
        beam::sdk::jni::throwJava(env, "java/lang/IllegalArgumentException", "Invalid Beam relay input");
    } catch (...) {
        beam::sdk::jni::throwJava(env, "java/lang/IllegalStateException", "Beam relay failed");
    }
    return fallback;
}
}

extern "C" JNIEXPORT jlong JNICALL
Java_cash_p_beam_internal_BeamRelayNative_create(JNIEnv* env, jobject, jbyteArray bytes,
    jint network, jstring expectedRules) {
    return guarded<jlong>(env, 0, [&] {
        const auto rules = beam::sdk::jni::ascii(env, expectedRules, 2048);
        if (rules != beam::sdk::supportedRules(network).get_SignatureStr())
            throw std::invalid_argument("Beam rules mismatch");
        const auto input = beam::sdk::jni::transactionBytes(env, bytes);
        // Core/YAS runtime_error also denotes invalid serialization/proofs.
        std::unique_ptr<TransactionRelay> value;
        try { value = std::make_unique<TransactionRelay>(input, network, rules); }
        catch (const std::bad_alloc&) { throw; }
        catch (const std::exception&) { throw std::invalid_argument("Invalid transaction"); }
        return reinterpret_cast<jlong>(value.release());
    });
}

extern "C" JNIEXPORT jint JNICALL
Java_cash_p_beam_internal_BeamRelayNative_run(JNIEnv* env, jobject, jlong handle, jlong timeout) {
    return guarded<jint>(env, -1, [&] {
        if (timeout < 1 || timeout > 120'000) throw std::invalid_argument("Invalid relay timeout");
        return operation(handle).run(static_cast<uint32_t>(timeout)).encoded();
    });
}

extern "C" JNIEXPORT void JNICALL
Java_cash_p_beam_internal_BeamRelayNative_cancel(JNIEnv*, jobject, jlong handle) {
    if (handle) reinterpret_cast<TransactionRelay*>(handle)->cancel();
}

extern "C" JNIEXPORT void JNICALL
Java_cash_p_beam_internal_BeamRelayNative_destroy(JNIEnv*, jobject, jlong handle) {
    delete reinterpret_cast<TransactionRelay*>(handle);
}
