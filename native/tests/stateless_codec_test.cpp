// Isolated test-only reuse of the existing synthetic funding/signer/node helpers.
// This is a separate executable, never a source of the shipped JNI library.
#define main offlineSigningFixtureMain
#include "offline_signing_test.cpp"
#undef main
#include "stateless_codec.h"
#include "wallet/core/base58.h"

namespace {
template<class F> void rejectsAt(F&& action, const char* expected) {
    try { action(); }
    catch (const std::exception& error) {
        if (std::string(error.what()).find(expected) == std::string::npos)
            throw std::runtime_error(std::string("expected rejection ") + expected + ": " + error.what());
        return;
    }
    throw std::runtime_error("hostile bytes accepted");
}

void rejectsWith(const ByteBuffer& bytes, const Rules& rules, const char* expected) {
    rejectsAt([&] { beam::sdk::inspectTransaction(bytes, rules, rules.get_SignatureStr()); }, expected);
}

ByteBuffer replaceProofCfg(const ByteBuffer& bytes, const Transaction& tx, bool noncanonical) {
    for (const auto& kernel : tx.m_vKernels) {
        if (kernel->get_Subtype() != TxKernel::Subtype::ShieldedInput) continue;
        const auto& proof = kernel->CastTo_ShieldedInput().m_SpendProof;
        const auto encodedProof = encode(proof);
        const auto at = std::search(bytes.begin(), bytes.end(), encodedProof.begin(), encodedProof.end());
        require(at != bytes.end(), "proof bytes missing");
        ByteBuffer result(bytes.begin(), at);
        auto n = encode(proof.m_Cfg.n);
        auto m = encode(proof.m_Cfg.M);
        if (noncanonical) {
            // Pinned YAS compact unsigned: overlong one-byte payload instead of inline value.
            require(n.size() == 1 && proof.m_Cfg.n < 128, "unexpected fixture integer encoding");
            result.push_back(1);
            result.push_back(static_cast<uint8_t>(proof.m_Cfg.n));
            result.insert(result.end(), m.begin(), m.end());
        } else {
            auto hugeN = encode(uint32_t(0xffffffff));
            auto hugeM = encode(uint32_t(0xffffffff));
            result.insert(result.end(), hugeN.begin(), hugeN.end());
            result.insert(result.end(), hugeM.begin(), hugeM.end());
        }
        result.insert(result.end(), at + n.size() + m.size(), bytes.end());
        return result;
    }
    throw std::runtime_error("fixture has no shielded proof");
}

void hostile(const ByteBuffer& bytes, const Rules& rules) {
    auto trailing = bytes;
    trailing.push_back(0);
    rejectsWith(trailing, rules, "Trailing");
    auto truncated = bytes;
    truncated.pop_back();
    rejects([&] { beam::sdk::inspectTransaction(truncated, rules, rules.get_SignatureStr()); }, "truncation accepted");
    rejectsWith(ByteBuffer(beam::sdk::kMaxTransactionBytes + 1), rules, "byte length");
    rejects([&] { beam::sdk::inspectTransaction(bytes, rules, "wrong rules"); }, "wrong rules accepted");
    const auto testnet = beam::sdk::supportedRules(1);
    const auto mainnet = beam::sdk::supportedRules(0);
    rejects([&] { beam::sdk::inspectTransaction(bytes, testnet, mainnet.get_SignatureStr()); }, "wrong network accepted");

    // Core TxVectors counts are fixed uintBig32, independently of YAS compact integers.
    // Padding makes the remaining-byte heuristic succeed: our count gate must reject.
    auto count = encode(uintBigFrom(uint32_t(beam::sdk::kMaxCodecVector + 1)));
    count.resize(2048);
    rejectsWith(count, rules, "vector count");
    auto outputCount = encode(uintBigFrom(uint32_t(0)));
    outputCount.insert(outputCount.end(), count.begin(), count.end());
    rejectsWith(outputCount, rules, "vector count");
    auto kernelCount = encode(uintBigFrom(uint32_t(0)));
    auto noOutputs = kernelCount;
    kernelCount.insert(kernelCount.end(), noOutputs.begin(), noOutputs.end());
    kernelCount.insert(kernelCount.end(), count.begin(), count.end());
    rejectsWith(kernelCount, rules, "vector count");

    Transaction deep;
    auto root = std::make_unique<TxKernelStd>();
    TxKernel* cursor = root.get();
    for (unsigned i = 0; i < 4; ++i) {
        auto next = std::make_unique<TxKernelStd>();
        auto* nextPtr = next.get();
        cursor->m_vNested.push_back(std::move(next));
        cursor = nextPtr;
    }
    deep.m_vKernels.push_back(std::move(root));
    deep.m_Offset = Zero;
    rejectsWith(encode(deep), rules, "recursion too deep");

    Transaction aggregate;
    aggregate.m_Offset = Zero;
    for (unsigned i = 0; i < 4; ++i) {
        auto root = std::make_unique<TxKernelStd>();
        for (size_t j = 0; j < beam::sdk::kMaxCodecVector; ++j)
            root->m_vNested.push_back(std::make_unique<TxKernelStd>());
        aggregate.m_vKernels.push_back(std::move(root));
    }
    rejectsWith(encode(aggregate), rules, "aggregate count");

    // Unknown kernel tag with no body: Core must reject dispatch before any crypto.
    ByteBuffer futureKernel(8, 0); // no ordinary inputs or outputs
    auto taggedCount = encode(uintBigFrom(uint32_t(0x80000001)));
    futureKernel.insert(futureKernel.end(), taggedCount.begin(), taggedCount.end());
    futureKernel.push_back(255);
    rejectsWith(futureKernel, rules, "Bad kernel subtype");

    auto unsupported = decode(bytes);
    for (auto& kernel : unsupported->m_vKernels)
        if (kernel->get_Subtype() == TxKernel::Subtype::Std) kernel->m_vNested.clear();
    rejectsWith(encode(*unsupported), rules, "nested recipient output");

    auto tx = decode(bytes);
    if (shieldedInputs(*tx)) {
        rejectsWith(replaceProofCfg(bytes, *tx, false), rules, "proof parameters");
        rejectsWith(replaceProofCfg(bytes, *tx, true), rules, "Noncanonical");
        // Tiny FakePoW proof configurations must not become production acceptance.
        rejectsWith(bytes, mainnet, "proof parameters");
    }
}

void codecFixture(const std::filesystem::path& dir) {
    auto reactor = io::Reactor::create();
    io::Reactor::Scope reactorScope(*reactor);
    Rules rules;
    rules.m_Consensus = Rules::Consensus::FakePoW;
    rules.AllowPublicUtxos = true;
    rules.TreasuryChecksum = Zero;
    rules.Maturity.Coinbase = 10;
    for (size_t i = 1; i + 1 < std::size(rules.pForks); ++i) rules.pForks[i].m_Height = 5 + i;
    rules.Shielded.m_ProofMax = {2, 4};
    rules.Shielded.m_ProofMin = {2, 2};
    rules.Shielded.MaxIns = 2;
    rules.Shielded.MaxWindowBacklog = 32;
    rules.UpdateChecksum();
    Rules::Scope scope(rules);
    auto owner = createDb(dir / "funding.db", kSenderSeed);
    auto minerDb = createDb(dir / "miner.db", 90817);
    auto miner = minerDb->get_MasterKdf();
    NodeProcessor node;
    node.m_Horizon.SetInfinite();
    node.Initialize((dir / "node.db").string().c_str());
    Funding funds;
    while (node.m_Cursor.m_hh.m_Height < 29) mine(node, miner);
    mine(node, miner, fund(miner, owner->get_MasterKdf(), 1, 30, false, 0, &funds));
    for (unsigned i = 0; i < kInitialPool; ++i)
        mine(node, miner, fund(miner, i ? miner : owner->get_MasterKdf(), 2 + i, 31 + i,
            true, i, i ? nullptr : &funds));
    const Cache cache = priorSync(node);
    owner.reset();
    unsigned scenario = 0;
    for (Inputs inputs : {Inputs::Ordinary, Inputs::Shielded, Inputs::Mixed})
        for (TxAddressType type : {TxAddressType::Offline, TxAddressType::PublicOffline, TxAddressType::MaxPrivacy}) {
            ++scenario;
            auto recipient = chooseRecipient(dir, scenario, type);
            const auto token = beam::sdk::parseToken(recipient.token);
            require(token.type == type && token.voucherCount == (type == TxAddressType::PublicOffline ? 0 : 1),
                "one-sided token metadata mismatch");
            bool hex = true;
            auto tokenBytes = from_hex(recipient.token, &hex);
            if (!hex) tokenBytes = DecodeBase58(recipient.token);
            require(beam::sdk::parseToken(to_hex(tokenBytes.data(), tokenBytes.size())).type == type, "hex token rejected");
            auto parameters = *ParseParameters(recipient.token);
            if (type != TxAddressType::PublicOffline) {
                if (type == TxAddressType::Offline) {
                    auto vouchers = *parameters.GetParameter<ShieldedVoucherList>(TxParameterID::ShieldedVoucherList);
                    vouchers.front().m_Signature.m_k = Zero;
                    parameters.SetParameter(TxParameterID::ShieldedVoucherList, vouchers);
                } else {
                    auto voucher = *parameters.GetParameter<ShieldedTxo::Voucher>(TxParameterID::Voucher);
                    voucher.m_Signature.m_k = Zero;
                    parameters.SetParameter(TxParameterID::Voucher, voucher);
                }
                rejectsAt([&] { beam::sdk::parseToken(std::to_string(parameters)); }, "voucher signature");
            }
            parameters = *ParseParameters(recipient.token);
            parameters.SetParameter(TxParameterID::TransactionType, TxType::Simple);
            rejectsAt([&] { beam::sdk::parseToken(std::to_string(parameters)); }, "Interactive");
            tokenBytes.push_back(0);
            rejects([&] { beam::sdk::parseToken(EncodeToBase58(tokenBytes)); }, "trailing token bytes accepted");
            Captured captured;
            {
                Session session(dir / ("sender-" + std::to_string(scenario) + ".db"), funds, cache, inputs, Fault::None);
                session.prepare(recipient, inputs == Inputs::Mixed ? 60'000'000 : 20'000'000 + scenario, scenario);
                captured = successful(session, inputs, type, Fault::None);
            }
            const auto inspected = beam::sdk::inspectTransaction(captured.bytes, rules, rules.get_SignatureStr());
            require(inspected.mainKernelId == mainKernel(*decode(captured.bytes)).get_ID().str(), "wrong main kernel identity");
            require(inspected.shieldedInputs == (inputs == Inputs::Ordinary ? 0 : 1), "wrong shielded input count");
            require(encode(*inspected.transaction) == captured.bytes, "full transaction identity lost");
            require(inspected.serializedHash.size() == 64, "missing full transaction hash");
            if (inputs != Inputs::Ordinary)
                require(inspected.mainKernelId != inspected.transaction->m_vKernels.front()->get_ID().str(),
                    "fixture did not exercise normalized shielded-first ordering");
            require(&Rules::get() == &rules, "codec leaked thread-local rules scope");
            require(relay(node, captured.bytes, {"synthetic-fakepow", rules.get_SignatureStr()}) == proto::TxStatus::Ok,
                "fixture contextual validation failed");
            hostile(captured.bytes, rules);
            std::cout << "STATELESS_SCENARIO_OK " << scenario << " inputs="
                << (inputs == Inputs::Ordinary ? "ordinary" : inputs == Inputs::Shielded ? "shielded" : "mixed")
                << " token=" << (type == TxAddressType::Offline ? "Offline" :
                    type == TxAddressType::PublicOffline ? "PublicOffline" : "MaxPrivacy")
                << " canonical_fullbytes main_standard_kernel"
                << (inputs == Inputs::Ordinary ? "" : " normalized_shielded_first") << '\n';
        }
    {
        ++scenario;
        auto recipient = chooseRecipient(dir, scenario, TxAddressType::MaxPrivacy);
        Captured corrupt;
        {
            Session session(dir / "corrupt-context.db", funds, cache, Inputs::Shielded, Fault::State1);
            session.prepare(recipient, 20'000'000, scenario);
            corrupt = successful(session, Inputs::Shielded, recipient.type, Fault::State1);
        }
        // Deliberately demonstrates the public limitation: inspection cannot prove
        // the spend proof against the node's independent commitment/State1 context.
        beam::sdk::inspectTransaction(corrupt.bytes, rules, rules.get_SignatureStr());
        require(relay(node, corrupt.bytes, {"synthetic-fakepow", rules.get_SignatureStr()}) == proto::TxStatus::InvalidInput,
            "contextual node accepted corrupt shielded spend proof");
        std::cout << "STATELESS_CONTEXT_FREE_OK_NODE_REJECTS_BAD_SHIELDED_SPEND_PROOF\n";
    }
    rejects([] { beam::sdk::parseToken(std::string(beam::sdk::kMaxTokenChars + 1, '1')); }, "oversized token accepted");
    rejects([] { beam::sdk::parseToken("not a token"); }, "malformed token accepted");
    rejects([] { beam::sdk::parseToken(std::string(70, '0') + 'Z'); }, "malformed hex/Base58 accepted");
    rejects([] { beam::sdk::parseToken(std::string(70, 'O')); }, "invalid Base58 alphabet accepted");
    TxParameters oversizedField;
    auto badVouchers = encode(uint64_t(257)); // Core/YAS sequence size, with enough remaining bytes.
    badVouchers.resize(2048);
    oversizedField.SetParameter(TxParameterID::ShieldedVoucherList, std::move(badVouchers));
    rejectsAt([&] { beam::sdk::parseToken(std::to_string(oversizedField)); }, "vector count");
    TxParameters interactive;
    interactive.SetParameter(TxParameterID::TransactionType, TxType::Simple);
    rejects([&] { beam::sdk::parseToken(std::to_string(interactive)); }, "interactive token accepted");
    std::cout << "STATELESS_TOKEN_REJECTIONS_OK invalid_signature trailing malformed_hex_base58 length interactive voucher_count_before_alloc\n"
        << "STATELESS_RAW_REJECTIONS_OK vector256 aggregate1024 depth proof_configs rules_network trailing noncanonical unknown_kernel\n";
}
} // namespace

int main() {
    const auto dir = std::filesystem::temp_directory_path() / ("beam-codec-fixture-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    try {
        require(std::filesystem::create_directory(dir), "fixture directory collision");
        codecFixture(dir);
        std::filesystem::remove_all(dir);
        std::cout << "STATELESS_CODEC_OK (synthetic 16/4 proofs only)\n";
        return 0;
    } catch (const std::exception& error) {
        std::filesystem::remove_all(dir);
        std::cerr << "STATELESS_CODEC_FAILED: " << error.what() << '\n';
        return 1;
    }
}
