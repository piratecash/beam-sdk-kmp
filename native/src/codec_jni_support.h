#pragma once

#include "stateless_codec.h"
#include <jni.h>

namespace beam::sdk::jni {
struct PendingJavaException {};

ByteBuffer transactionBytes(JNIEnv*, jbyteArray);
std::string ascii(JNIEnv*, jstring, size_t limit);
void throwJava(JNIEnv*, const char* className, const char* message);
}
