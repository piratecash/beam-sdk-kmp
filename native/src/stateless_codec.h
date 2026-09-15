#pragma once

#include "core/block_crypt.h"
#include "wallet/core/common.h"
#include <string>

namespace beam::sdk {
// Admission limits, not estimates of every transaction accepted by consensus.
constexpr size_t kMaxTokenChars = 65'536;
constexpr size_t kMaxTransactionBytes = 1'048'576;
constexpr size_t kMaxCodecVector = 256;
constexpr size_t kMaxCodecElements = 1024;

struct ParsedToken {
    wallet::TxAddressType type;
    size_t voucherCount;
};

struct InspectedTransaction {
    Transaction::Ptr transaction;
    std::string serializedHash;
    std::string mainKernelId;
    HeightRange mainHeight;
    HeightRange validHeight;
    size_t ordinaryInputs, ordinaryOutputs, shieldedInputs, kernelCount;
};

ParsedToken parseToken(const std::string& token);
Rules supportedRules(int network);
// Caller supplies trusted supported rules; JNI only obtains these via supportedRules.
// This overload also lets synthetic tests use their explicitly isolated FakePoW rules.
// No definition of Rules::s_pInstance here: the JNI DSO/executable owns that symbol.
InspectedTransaction inspectTransaction(const ByteBuffer&, const Rules&, const std::string& expectedRules);
} // namespace beam::sdk
