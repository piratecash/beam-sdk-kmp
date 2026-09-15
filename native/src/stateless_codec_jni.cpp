#include "codec_jni_support.h"
#include "3rdparty/nlohmann/json.hpp"
#include <new>
#include <stdexcept>

namespace beam::sdk::jni {

void pending(JNIEnv* env) {
    if (env->ExceptionCheck()) throw PendingJavaException{};
}

void throwJava(JNIEnv* env, const char* name, const char* message) {
    if (env->ExceptionCheck()) return;
    jclass type = env->FindClass(name);
    if (type) {
        env->ThrowNew(type, message);
        env->DeleteLocalRef(type);
    }
}

// Region APIs copy after the length gate and acquire no pinned array/string lease,
// so no release can be missed on any C++/Java exception path.
std::string ascii(JNIEnv* env, jstring value, size_t limit) {
    if (!value) throw std::invalid_argument("Missing codec string");
    const jsize size = env->GetStringLength(value);
    pending(env);
    if (size <= 0 || static_cast<size_t>(size) > limit)
        throw std::invalid_argument("Codec string length exceeds limit");
    std::vector<jchar> chars(static_cast<size_t>(size));
    env->GetStringRegion(value, 0, size, chars.data());
    pending(env);
    std::string result(static_cast<size_t>(size), '\0');
    for (jsize i = 0; i < size; ++i) {
        if (!chars[i] || chars[i] > 127) throw std::invalid_argument("Codec string must be ASCII");
        result[i] = static_cast<char>(chars[i]);
    }
    return result;
}

ByteBuffer transactionBytes(JNIEnv* env, jbyteArray input) {
    if (!input) throw std::invalid_argument("Missing transaction bytes");
    const jsize size = env->GetArrayLength(input);
    pending(env);
    if (size <= 0 || static_cast<size_t>(size) > kMaxTransactionBytes)
        throw std::invalid_argument("Transaction byte length exceeds limit");
    ByteBuffer bytes(static_cast<size_t>(size));
    env->GetByteArrayRegion(input, 0, size, reinterpret_cast<jbyte*>(bytes.data()));
    pending(env);
    return bytes;
}
} // namespace beam::sdk::jni

namespace {
using beam::sdk::jni::ascii;
using beam::sdk::jni::PendingJavaException;
using beam::sdk::jni::throwJava;

template<class F> jstring call(JNIEnv* env, F&& action) {
    try {
        const std::string result = action();
        return env->NewStringUTF(result.c_str());
    } catch (const PendingJavaException&) {
        // Preserve the JVM exception (including allocation failure).
    } catch (const std::bad_alloc&) {
        throwJava(env, "java/lang/OutOfMemoryError", "Beam codec allocation failed");
    } catch (const std::exception&) {
        // Core/YAS may use runtime_error for invalid proofs/serialization. Never
        // disclose raw tokens/transaction payloads through exception messages.
        throwJava(env, "java/lang/IllegalArgumentException", "Invalid or unsupported Beam codec input");
    } catch (...) {
        throwJava(env, "java/lang/IllegalStateException", "Beam codec failed");
    }
    return nullptr;
}
} // namespace

extern "C" JNIEXPORT jstring JNICALL
Java_cash_p_beam_internal_BeamCodecNative_parseToken(JNIEnv* env, jobject, jstring token) {
    return call(env, [&] {
        const auto parsed = beam::sdk::parseToken(ascii(env, token, beam::sdk::kMaxTokenChars));
        const char* type = nullptr;
        switch (parsed.type) {
            case beam::wallet::TxAddressType::Offline: type = "Offline"; break;
            case beam::wallet::TxAddressType::PublicOffline: type = "PublicOffline"; break;
            case beam::wallet::TxAddressType::MaxPrivacy: type = "MaxPrivacy"; break;
            default: throw std::logic_error("Unexpected address type");
        }
        return nlohmann::json{{"type", type}, {"voucherCount", parsed.voucherCount}}.dump();
    });
}

extern "C" JNIEXPORT jstring JNICALL
Java_cash_p_beam_internal_BeamCodecNative_rules(JNIEnv* env, jobject, jint network) {
    return call(env, [&] { return beam::sdk::supportedRules(network).get_SignatureStr(); });
}

extern "C" JNIEXPORT jstring JNICALL
Java_cash_p_beam_internal_BeamCodecNative_inspect(JNIEnv* env, jobject, jbyteArray input,
    jint network, jstring expectedRules) {
    return call(env, [&] {
        const auto rules = beam::sdk::supportedRules(network);
        const auto expected = ascii(env, expectedRules, 2048);
        if (expected != rules.get_SignatureStr()) throw std::invalid_argument("Beam rules mismatch");
        const auto bytes = beam::sdk::jni::transactionBytes(env, input);
        const auto result = beam::sdk::inspectTransaction(bytes, rules, expected);
        return nlohmann::json{
            {"serializedHash", result.serializedHash}, {"mainKernelId", result.mainKernelId},
            {"mainMin", std::to_string(result.mainHeight.m_Min)}, {"mainMax", std::to_string(result.mainHeight.m_Max)},
            {"validMin", std::to_string(result.validHeight.m_Min)}, {"validMax", std::to_string(result.validHeight.m_Max)},
            {"ordinaryInputs", result.ordinaryInputs}, {"ordinaryOutputs", result.ordinaryOutputs},
            {"shieldedInputs", result.shieldedInputs}, {"kernelCount", result.kernelCount}
        }.dump();
    });
}
