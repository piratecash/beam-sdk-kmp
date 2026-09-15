#include "stateless_codec.h"
#include "core/serialization_adapters.h"
#include "core/shielded.h"
#include "utility/hex.h"
#include "wallet/core/base58.h"
#include "wallet/core/wallet_db.h"
#include <algorithm>
#include <stdexcept>
#include <type_traits>

namespace beam::sdk {
namespace {
void check(bool condition, const char* reason) {
    if (!condition) throw std::invalid_argument(reason);
}

template<class T> ByteBuffer encode(const T& value) {
    Serializer serializer;
    serializer & value;
    ByteBuffer bytes;
    serializer.swap_buf(bytes);
    return bytes;
}

// Keep the actual pinned YAS/Core adapters, including their pointer flags, compact
// integers, proof layouts and recursion guard. Dispatch with THIS archive type so
// nested vectors cannot silently fall back to an unbounded binary_iarchive.
class BoundedArchive : public yas::binary_iarchive<detail::SerializeIstream, SERIALIZE_OPTIONS> {
    using Base = yas::binary_iarchive<detail::SerializeIstream, SERIALIZE_OPTIONS>;
    detail::SerializeIstream& stream_;
    const Rules& rules_;
    size_t elements_ = 0;
    size_t byteAllowance_ = 0;
    bool token_;

    template<class T> void coreLoad(T& value) {
        using namespace yas::detail;
        serializer<type_properties<T>::value,
            serialization_method<T, BoundedArchive>::value, SERIALIZE_OPTIONS, T>::load(*this, value);
    }

public:
    BoundedArchive(detail::SerializeIstream& stream, const Rules& rules, bool token)
        : Base(stream), stream_(stream), rules_(rules), token_(token) {}

    using Base::read;
    void read(bool& value) {
        uint8_t encoded;
        Base::read(encoded);
        check(encoded <= 1, "Noncanonical boolean");
        value = encoded != 0;
    }

    void ensure_size(size_t count) {
        stream_.ensure_size(count);
        if (byteAllowance_) {
            check(count <= byteAllowance_, "Token field exceeds byte limit");
        } else {
            check(count <= kMaxCodecVector, "Codec vector count exceeds limit");
            check(count <= kMaxCodecElements - elements_, "Codec aggregate count exceeds limit");
            elements_ += count;
        }
    }

    template<class T> BoundedArchive& operator&(T& value) {
        coreLoad(value);
        return *this;
    }

    BoundedArchive& operator&(ByteBuffer& value) {
        // Raw token parameter buffers are bounded separately from element vectors.
        // Transaction contract byte buffers remain subject to the smaller bound.
        byteAllowance_ = token_ ? kMaxTokenChars : 0;
        coreLoad(value);
        byteAllowance_ = 0;
        return *this;
    }

    BoundedArchive& operator&(Lelantus::Proof& value) {
        const char* start = stream_.cur;
        Sigma::Cfg cfg;
        *this & cfg.n & cfg.M;
        check(cfg == rules_.Shielded.m_ProofMin || cfg == rules_.Shielded.m_ProofMax,
            "Unsupported shielded proof parameters");
        // Peek only the Core-encoded n/M; no proof allocation has happened yet.
        stream_.cur = start;
        coreLoad(value);
        return *this;
    }

    BoundedArchive& operator&(Asset::Proof& value) {
        // Core also hides native-BEAM asset identity. Its proof dimensions come
        // from trusted rules, not the payload; Core's adapter uses our vector gates.
        coreLoad(value);
        return *this;
    }

    BoundedArchive& operator&(std::pair<wallet::TxParameterID, ByteBuffer>& value);
};

template<class T> void decodeExact(const ByteBuffer& bytes, T& value, const Rules& rules, bool token) {
    check(!bytes.empty(), "Empty serialized value");
    detail::SerializeIstream stream;
    stream.reset(bytes.data(), bytes.size());
    BoundedArchive archive(stream, rules, token);
    archive & value;
    check(!stream.bytes_left(), "Trailing serialized bytes");
    check(encode(value) == bytes, "Noncanonical serialization");
}

BoundedArchive& BoundedArchive::operator&(std::pair<wallet::TxParameterID, ByteBuffer>& value) {
    coreLoad(value);
    // TxToken::IsValid parses each known public field with an ordinary Deserializer.
    // Preflight those SAME field types before handing the token back to Core.
    using namespace wallet;
    using namespace ECC;
    switch (value.first) {
#define MACRO(name, index, type) \
        case TxParameterID::name: { type field{}; decodeExact(value.second, field, rules_, true); break; }
        BEAM_TX_PUBLIC_PARAMETERS_MAP(MACRO)
#undef MACRO
        default: break; // Unknown opaque fields are byte-bounded, as in pinned Core.
    }
    return *this;
}

void shape(const Transaction& tx, const TxKernelStd*& main, size_t& shielded, size_t& kernels) {
    main = nullptr;
    shielded = 0;
    kernels = tx.m_vKernels.size();
    for (const auto& kernel : tx.m_vKernels) {
        if (kernel->get_Subtype() == TxKernel::Subtype::Std) {
            check(!main, "Ambiguous main standard kernel");
            main = &kernel->CastTo_Std();
            check(!main->m_pHashLock && !main->m_pRelativeLock && !main->m_CanEmbed,
                "Unsupported main kernel locks/embedding");
            check(main->m_vNested.size() == 1, "Expected one nested recipient output");
            const auto& nested = *main->m_vNested.front();
            check(nested.get_Subtype() == TxKernel::Subtype::ShieldedOutput && nested.m_vNested.empty(),
                "Expected leaf shielded recipient output");
            ++kernels;
        } else {
            check(kernel->get_Subtype() == TxKernel::Subtype::ShieldedInput && kernel->m_vNested.empty(),
                "Unsupported top-level kernel or nesting");
            ++shielded;
        }
    }
    check(main, "Missing main standard kernel");
    check(shielded <= Rules::get().Shielded.MaxIns, "Too many shielded inputs");
    for (const auto& output : tx.m_vOutputs)
        check(!output->m_Coinbase,
            "Unsupported ordinary output");
}
} // namespace

Rules supportedRules(int network) {
    check(network == 0 || network == 1, "Unsupported Beam network");
    Rules rules;
    rules.m_Network = network == 0 ? Rules::Network::mainnet : Rules::Network::testnet;
    rules.UpdateChecksum();
    return rules;
}

ParsedToken parseToken(const std::string& token) {
    check(!token.empty() && token.size() <= kMaxTokenChars, "Token length exceeds limit");
    check(std::all_of(token.begin(), token.end(), [](unsigned char c) {
        return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
    }), "Token must be ASCII hex or Base58");
    bool hex = true;
    auto bytes = from_hex(token, &hex);
    if (!hex) bytes = wallet::DecodeBase58(token);
    check(bytes.size() > 33 && (bytes.front() & wallet::TxToken::TokenFlag),
        "Only one-sided Beam tokens are supported");
    // Used only to preflight unrelated optional parameter types. Tokens do not
    // encode this network and this function neither detects nor returns a network.
    Rules rules;
    Rules::Scope scope(rules);
    wallet::TxToken decoded;
    decodeExact(bytes, decoded, rules, true);
    auto params = wallet::ParseParameters(token);
    check(!!params, "Invalid Beam token");
    const auto type = wallet::GetAddressType(token);
    using wallet::TxAddressType;
    check(type == TxAddressType::Offline || type == TxAddressType::PublicOffline ||
        type == TxAddressType::MaxPrivacy, "Interactive or unsupported Beam address");
    size_t count = 0;
    if (type == TxAddressType::Offline || type == TxAddressType::MaxPrivacy) {
        auto endpoint = params->GetParameter<PeerID>(wallet::TxParameterID::PeerEndpoint);
        check(!!endpoint, "Missing token endpoint");
        if (type == TxAddressType::Offline) {
            auto vouchers = params->GetParameter<wallet::ShieldedVoucherList>(wallet::TxParameterID::ShieldedVoucherList);
            check(!!vouchers && !vouchers->empty(), "Missing vouchers");
            for (const auto& voucher : *vouchers) check(voucher.IsValid(*endpoint), "Invalid voucher signature");
            count = vouchers->size();
        } else {
            auto voucher = params->GetParameter<ShieldedTxo::Voucher>(wallet::TxParameterID::Voucher);
            check(!!voucher && voucher->IsValid(*endpoint), "Invalid voucher signature");
            count = 1;
        }
    }
    return {type, count};
}

InspectedTransaction inspectTransaction(const ByteBuffer& bytes, const Rules& rules, const std::string& expectedRules) {
    check(!bytes.empty() && bytes.size() <= kMaxTransactionBytes, "Transaction byte length exceeds limit");
    check(expectedRules == rules.get_SignatureStr(), "Beam network/rules mismatch");
    Rules::Scope scope(rules);
    auto tx = std::make_shared<Transaction>();
    decodeExact(bytes, *tx, rules, false);
    const TxKernelStd* main;
    size_t shielded, kernels;
    shape(*tx, main, shielded, kernels);
    check(main->m_Height.m_Min && main->m_Height.m_Min <= main->m_Height.m_Max,
        "Invalid main height range");
    Transaction::Context context;
    context.m_Height = main->m_Height;
    check(tx->IsValid(context), "Transaction context-free validation failed");
    // Never Normalize before identity checks, and never choose the first kernel:
    // Core normalization puts shielded inputs ahead of the main standard kernel.
    main->CalculateID();
    ECC::Hash::Value hash;
    ECC::Hash::Processor() << Blob(bytes) >> hash;
    return {tx, hash.str(), main->get_ID().str(), main->m_Height, context.m_Height,
        tx->m_vInputs.size(), tx->m_vOutputs.size(), shielded, kernels};
}
} // namespace beam::sdk
