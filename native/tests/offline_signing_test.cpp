#include "wallet/core/wallet.h"
#include "wallet/core/common_utils.h"
#include "wallet/core/simple_transaction.h"
#include "wallet/transactions/lelantus/push_transaction.h"
#include "core/shielded.h"
#include "node/processor.h"
#include "utility/io/reactor.h"
#include "utility/io/timer.h"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <map>
#include <set>
#include <stdexcept>

#ifndef BEAM_SDK_REUSE_SIGNING_FIXTURE
thread_local const beam::Rules* beam::Rules::s_pInstance = nullptr;
#endif

namespace {
using namespace beam;
using namespace beam::wallet;
constexpr Amount kValue = 40'000'000, kFee = 1'000'000;
constexpr unsigned kInitialPool = 20;
constexpr uint64_t kSenderSeed = 10383;
constexpr size_t kByteLimit = 1024 * 1024;

void require(bool ok, const char* message)
{
    if (!ok) throw std::runtime_error(message);
}

template<class F> void rejects(F&& f, const char* message)
{
    bool rejected = false;
    try { f(); } catch (const std::exception&) { rejected = true; }
    require(rejected, message);
}

template<class T> ByteBuffer encode(const T& value)
{
    Serializer s;
    s & value;
    ByteBuffer result;
    s.swap_buf(result);
    return result;
}

Transaction::Ptr decode(const ByteBuffer& bytes)
{
    // Fixture bytes only. This is not the production hostile-input decoder.
    require(!bytes.empty() && bytes.size() <= kByteLimit, "transaction size outside fixture bound");
    auto tx = std::make_shared<Transaction>();
    Deserializer d;
    d.reset(bytes);
    d & *tx;
    require(!d.bytes_left(), "trailing transaction bytes");
    require(encode(*tx) == bytes, "transaction did not round trip canonically");
    return tx;
}

const TxKernelStd& mainKernel(const Transaction& tx)
{
    const TxKernelStd* main = nullptr;
    for (const auto& kernel : tx.m_vKernels)
    {
        if (kernel->get_Subtype() != TxKernel::Subtype::Std) continue;
        require(!main, "ambiguous main standard kernel");
        main = &kernel->CastTo_Std();
    }
    require(main && main->m_vNested.size() == 1 &&
        main->m_vNested.front()->get_Subtype() == TxKernel::Subtype::ShieldedOutput,
        "missing main standard kernel with recipient output");
    main->CalculateID();
    return *main;
}

unsigned shieldedInputs(const Transaction& tx)
{
    unsigned result = 0;
    for (const auto& kernel : tx.m_vKernels)
    {
        if (kernel->get_Subtype() == TxKernel::Subtype::ShieldedInput) ++result;
        else require(kernel->get_Subtype() == TxKernel::Subtype::Std, "unexpected top-level kernel");
    }
    return result;
}

void tick()
{
    auto timer = io::Timer::create(io::Reactor::get_Current());
    timer->start(1, false, [] { io::Reactor::get_Current().stop(); });
    io::Reactor::get_Current().run();
}

template<class F> void pump(F&& done)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
    while (!done())
    {
        require(std::chrono::steady_clock::now() < deadline, "real keykeeper timed out");
        tick();
    }
}

std::shared_ptr<WalletDB> createDb(const std::filesystem::path& path, uint64_t value)
{
    ECC::NoLeak<ECC::uintBig> seed;
    seed.V = value;
    auto db = std::dynamic_pointer_cast<WalletDB>(WalletDB::init(path.string(),
        SecString(std::string("synthetic-offline-probe")), seed));
    require(bool(db), "cannot create synthetic WalletDB");
    return db;
}

ByteBuffer parameters(const IWalletDB& db)
{
    Serializer s;
    for (const auto& p : db.getAllTxParameters())
        s & p.m_txID & p.m_subTxID & p.m_paramID & p.m_value;
    ByteBuffer result;
    s.swap_buf(result);
    return result;
}

// Same node entry points as snapshot_reorg_recovery_test.cpp; no socket or Node service.
void mine(NodeProcessor& node, const Key::IKdf::Ptr& miner, Transaction::Ptr tx = {})
{
    TxPool::Fluff pool;
    if (tx)
    {
        Transaction::Context context;
        context.m_Height.m_Min = node.m_Cursor.m_hh.m_Height + 1;
        require(tx->IsValid(context), "invalid synthetic funding transaction");
        uint32_t charge = 0;
        require(node.ValidateTxContextEx(*tx, context.m_Height, false, charge, nullptr, nullptr, nullptr) ==
            proto::TxStatus::Ok, "node rejected synthetic funding");
        Transaction::KeyType key;
        tx->get_Key(key);
        TxPool::Stats stats;
        stats.From(*tx, context, 0, 0);
        pool.AddValidTx(std::move(tx), stats, key, TxPool::Fluff::State::Fluffed);
    }
    NodeProcessor::BlockContext block(pool, 0, *miner, *miner);
    require(node.GenerateNewBlock(block), "cannot generate synthetic block");
    require(node.OnState(block.m_Hdr, PeerID()) == NodeProcessor::DataStatus::Accepted, "header rejected");
    Block::SystemState::ID id;
    block.m_Hdr.get_ID(id);
    require(node.OnBlock(id, block.m_Body.m_Perishable, block.m_Body.m_Eternal, PeerID()) ==
        NodeProcessor::DataStatus::Accepted, "block body rejected");
    node.TryGoUp();
    require(node.m_Cursor.m_hh.m_Height == block.m_Hdr.get_Height(), "node did not advance");
}

struct Funding
{
    Coin ordinary;
    ShieldedCoin shielded;
};

// Funding is independently mined before any signer cache exists.
Transaction::Ptr fund(const Key::IKdf::Ptr& miner, const Key::IKdf::Ptr& owner,
    Height coinHeight, Height height, bool shield, TxoID index, Funding* recovered)
{
    auto tx = std::make_shared<Transaction>();
    CoinID source(Rules::get().get_Emission(coinHeight), coinHeight, Key::Type::Coinbase);
    source.set_Subkey(0, CoinID::Scheme::V_Miner0);
    auto input = std::make_unique<Input>();
    ECC::Scalar::Native offset, changeKey;
    CoinID::Worker(source).Create(offset, input->m_Commitment, *miner);
    tx->m_vInputs.push_back(std::move(input));
    auto change = std::make_unique<Output>();
    CoinID changeId(source.m_Value - kValue - kFee, 1000 + height, Key::Type::Regular);
    change->Create(height, changeKey, *changeId.get_ChildKdf(miner), changeId, *miner, Output::OpCode::Public);
    offset += -changeKey;
    tx->m_vOutputs.push_back(std::move(change));
    if (shield)
    {
        auto kernel = std::make_unique<TxKernelShieldedOutput>();
        kernel->m_Height.m_Min = height;
        kernel->m_Fee = kFee;
        ShieldedTxo::Viewer viewer;
        viewer.FromOwner(*owner, 0);
        ShieldedTxo::Data::Params p;
        p.m_Ticket.Generate(kernel->m_Txo.m_Ticket, viewer, ECC::Hash::Value(2000 + index));
        p.m_Output.m_Value = kValue;
        p.m_Output.m_AssetID = 0;
        ZeroObject(p.m_Output.m_User);
        ECC::Oracle oracle;
        oracle << kernel->get_Msg();
        p.GenerateOutp(kernel->m_Txo, height, oracle);
        offset += -p.m_Output.m_k;
        if (recovered)
        {
            ShieldedTxo::Data::Params scan;
            ECC::Oracle scanOracle;
            scanOracle << kernel->get_Msg();
            require(scan.Recover(kernel->m_Txo, height, scanOracle, viewer), "cannot recognize shielded funding");
            recovered->shielded.m_CoinID.m_Key.m_nIdx = 0;
            scan.ToID(recovered->shielded.m_CoinID);
            recovered->shielded.m_TxoID = index;
            recovered->shielded.m_confirmHeight = height;
        }
        tx->m_vKernels.push_back(std::move(kernel));
    }
    else
    {
        // Ordinary funding uses the same synthetic owner secret, never the miner's key.
        auto output = std::make_unique<Output>();
        ECC::Scalar::Native outputKey, kernelKey;
        CoinID coin(kValue, 3000, Key::Type::Regular);
        // V3 regular coins use child KDF 0, as the real keykeeper does.
        auto coinKdf = coin.get_ChildKdf(owner);
        output->Create(height, outputKey, *coinKdf, coin, *owner, Output::OpCode::Public);
        offset += -outputKey;
        if (recovered)
        {
            require(output->Recover(height, *owner, recovered->ordinary.m_ID) &&
                output->VerifyRecovered(*coinKdf, recovered->ordinary.m_ID), "cannot recognize ordinary funding");
            recovered->ordinary.m_confirmHeight = height;
            recovered->ordinary.m_maturity = output->get_MinMaturity(height);
        }
        tx->m_vOutputs.push_back(std::move(output));
        auto kernel = std::make_unique<TxKernelStd>();
        kernel->m_Height.m_Min = height;
        kernel->m_Fee = kFee;
        miner->DeriveKey(kernelKey, Key::ID(height + 4000, Key::Type::Kernel));
        kernel->Sign(kernelKey);
        offset += -kernelKey;
        tx->m_vKernels.push_back(std::move(kernel));
    }
    tx->m_Offset = offset;
    tx->Normalize();
    return tx;
}

using Range = std::pair<TxoID, uint32_t>;
struct Cache
{
    Block::SystemState::Full tip;
    std::string rules;
    uint64_t generation = 1;
    TxoID count = 0;
    bool valid = true;
    std::map<Range, proto::ShieldedList> ranges;

    void admit(const std::string& expectedRules, uint64_t expectedGeneration,
        const Block::SystemState::Full& expectedTip) const
    {
        require(valid, "fixture context invalidated");
        require(rules == expectedRules, "fixture rules mismatch");
        require(generation == expectedGeneration, "fixture generation mismatch");
        require(encode(tip) == encode(expectedTip), "fixture checkpoint changed");
        require(count && !ranges.empty(), "fixture shielded context missing");
    }
};

proto::ShieldedList nodeRange(NodeProcessor& node, Range range)
{
    // Exact Node::Peer::OnMsg(GetShieldedList) response semantics.
    const auto count = std::min<TxoID>(range.second, node.m_Extra.m_ShieldedOutputs - range.first);
    require(count && range.second <= 2 * Rules::get().Shielded.m_ProofMax.get_N(), "bad fixture range");
    proto::ShieldedList result;
    result.m_Items.resize(static_cast<size_t>(count));
    node.get_DB().ShieldedRead(range.first, result.m_Items.data(), count);
    node.get_DB().ShieldedStateRead(range.first + count - 1, &result.m_State1, 1);
    return result;
}

void peerAgreement(const std::string& a, const std::string& b, Range ra, Range rb,
    const proto::ShieldedList& x, const proto::ShieldedList& y)
{
    // Fixture boundary evidence, not the production quorum transport.
    require(a != b && ra == rb && encode(x) == encode(y), "fixture peer/range disagreement");
}

Cache priorSync(NodeProcessor& node)
{
    Cache cache;
    cache.tip = node.m_Cursor.m_Full;
    cache.rules = Rules::get().get_SignatureStr();
    cache.count = node.m_Extra.m_ShieldedOutputs;
    // Exhaustive tiny synthetic coverage; each State1 is read at its exact end.
    for (TxoID start = 0; start < cache.count; ++start)
        for (uint32_t count = 1; count <= 2 * Rules::get().Shielded.m_ProofMax.get_N(); ++count)
        {
            Range range{start, count};
            cache.ranges.emplace(range, nodeRange(node, range));
        }
    return cache;
}

class NoNetwork : public proto::FlyClient::NetworkStd
{
public:
    explicit NoNetwork(Wallet& wallet) : NetworkStd(wallet) {}
    unsigned requests = 0;
    void PostRequestInternal(proto::FlyClient::Request&) override
    {
        ++requests;
        throw std::runtime_error("unexpected wallet network request");
    }
};

enum class Fault { None, State1, WindowItems, Hold };
struct Gateway : INegotiatorGateway
{
    Cache cache;
    IWalletDB::Ptr db;
    Fault fault;
    bool closed = false;
    unsigned forbidden = 0, registrations = 0, failures = 0, listRequests = 0, vouchers = 0;
    unsigned corruptions = 0;
    int async = 0;
    ByteBuffer bytes;
    TxID walletTxId{};
    Merkle::Hash kernelId;
    std::set<Range> requested;
    ShieldedListCallback delayed;
    proto::ShieldedList delayedResponse;
    Range delayedRange{};

    Gateway(Cache c, IWalletDB::Ptr d, Fault f) : cache(std::move(c)), db(std::move(d)), fault(f) {}
    void OnAsyncStarted() override { ++async; }
    void OnAsyncFinished() override { --async; }
    [[noreturn]] void deny()
    {
        ++forbidden;
        throw std::runtime_error("unexpected gateway network operation");
    }
    void on_tx_completed(const TxID&) override { deny(); }
    void on_tx_failed(const TxID&) override { ++failures; }
    void register_tx(const TxID& id, const Transaction::Ptr& tx, const Merkle::Hash* parent,
        SubTxID sub) override
    {
        require(!closed && cache.valid && !parent && sub == kDefaultSubTxID, "capture outside signing scope");
        require(!registrations++, "transaction captured more than once");
        bytes = encode(*tx);
        walletTxId = id;
        require(storage::getTxParameter(*db, id, TxParameterID::KernelID, kernelId), "missing saved kernel ID");
        auto parsed = decode(bytes);
        require(mainKernel(*parsed).get_ID() == kernelId, "saved main kernel differs from bytes");
    }
    bool get_tip(Block::SystemState::Full& tip) const override
    {
        require(!closed && cache.valid, "tip outside signing scope");
        tip = cache.tip;
        return true;
    }
    void get_shielded_list(const TxID&, TxoID start, uint32_t count, ShieldedListCallback&& cb) override
    {
        require(!closed && cache.valid, "list outside signing scope");
        ++listRequests;
        Range range{start, count};
        requested.insert(range);
        auto it = cache.ranges.find(range);
        require(it != cache.ranges.end() && !it->second.m_Items.empty(), "uncached builder window");
        auto response = it->second;
        if (fault == Fault::State1)
        {
            response.m_State1.m_pData[0] ^= 1;
            ++corruptions;
        }
        if (fault == Fault::WindowItems)
        {
            // Own output is ID 0. Swap only decoys inside the actual proof window.
            const size_t end = response.m_Items.size();
            require(end >= 3 && start == 0, "wrong-window fixture no longer covers own output");
            std::swap(response.m_Items[end - 1], response.m_Items[end - 2]);
            ++corruptions;
        }
        if (fault == Fault::Hold)
        {
            delayed = std::move(cb);
            delayedResponse = std::move(response);
            delayedRange = range;
        }
        else cb(start, count, response);
    }
    void get_UniqueVoucher(const WalletID& peer, const TxID&, boost::optional<ShieldedTxo::Voucher>& out) override
    {
        require(!closed && cache.valid, "voucher outside signing scope");
        ++vouchers;
        out = db->grabVoucher(peer);
        require(bool(out), "missing cached receiver voucher");
    }
    void confirm_kernel(const TxID&, const Merkle::Hash&, SubTxID) override { deny(); }
    void confirm_kernel_ex(const Merkle::Hash&, IConfirmCallback::Ptr&&) override { deny(); }
    void confirm_asset(const TxID&, const PeerID&, SubTxID) override { deny(); }
    void confirm_asset(const TxID&, Asset::ID, SubTxID) override { deny(); }
    void get_kernel(const TxID&, const Merkle::Hash&, SubTxID) override { deny(); }
    void send_tx_params(const WalletID&, const SetTxParameter&) override { deny(); }
    void get_proof_shielded_output(const TxID&, const ECC::Point&, ProofShildedOutputCallback&&) override { deny(); }
    void UpdateOnNextTip(const TxID&) override { deny(); }
    void Listen(const WalletID&, const ECC::Scalar::Native&, IHandler*) override { deny(); }
    void Unlisten(const WalletID&, IHandler*) override { deny(); }
    void Send(const WalletID&, const Blob&) override { deny(); }
    void HftSubscribe(bool) override { deny(); }
    const Merkle::Hash* get_DependentState(uint32_t&) override { deny(); }
};

enum class Inputs { Ordinary, Shielded, Mixed };
struct Recipient { std::string token; TxAddressType type; };

Recipient chooseRecipient(const std::filesystem::path& dir, unsigned scenario, TxAddressType type)
{
    auto db = createDb(dir / ("receiver-" + std::to_string(scenario) + ".db"), 80000 + scenario);
    WalletAddress address;
    db->createAddress(address);
    db->saveAddress(address);
    std::string token;
    if (type == TxAddressType::Offline) token = GenerateOfflineToken(address, *db, 0, 0, "", 1);
    else if (type == TxAddressType::PublicOffline) token = GeneratePublicToken(address, *db, "");
    else token = GenerateMaxPrivacyToken(address, *db, 0, 0, "");
    require(!token.empty(), "cannot create synthetic recipient token");
    return {token, type};
}

struct Session
{
    std::shared_ptr<WalletDB> db;
    std::unique_ptr<Wallet> wallet;
    std::shared_ptr<NoNetwork> network;
    Gateway gateway;
    BaseTransaction::Ptr tx;

    Session(const std::filesystem::path& path, const Funding& funds, const Cache& cache, Inputs inputs, Fault fault)
        : db(createDb(path, kSenderSeed)), gateway(cache, db, fault)
    {
        // Synthetic prior-sync import of recognized, independently mined outputs.
        db->get_History().AddStates(&cache.tip, 1);
        HeightHash hh;
        cache.tip.get_ID(hh);
        db->setSystemStateID(hh);
        db->set_ShieldedOuts(cache.count);
        if (inputs != Inputs::Shielded) db->saveCoin(funds.ordinary);
        if (inputs != Inputs::Ordinary) db->saveShieldedCoin(funds.shielded);
        db->FlushNow();
        wallet = std::make_unique<Wallet>(db);
        network = std::make_shared<NoNetwork>(*wallet);
        wallet->SetNodeEndpoint(network);
    }
    ~Session()
    {
        gateway.closed = true;
        if (tx) tx->Retire();
        tx.reset();
        // Gateway outlives every callback owner.
        wallet.reset();
        network.reset();
    }
    void prepare(const Recipient& recipient, Amount amount, unsigned scenario)
    {
        Block::SystemState::Full synced;
        require(db->get_History().get_Tip(synced), "prior-sync tip missing");
        gateway.cache.admit(Rules::get().get_SignatureStr(), 1, synced);
        require(gateway.cache.count == db->get_ShieldedOuts(), "prior-sync pool count changed");
        require(db->getTxHistory(TxType::ALL, 0, 1).empty(), "request existed before sync");
        auto parsed = ParseParameters(recipient.token);
        require(bool(parsed), "cannot parse receiver token");
        TxID id{};
        id.back() = static_cast<uint8_t>(scenario);
        auto params = lelantus::CreatePushTransactionParameters(id);
        require(LoadReceiverParams(*parsed, params, recipient.type), "receiver preparation failed");
        params.SetParameter(TxParameterID::Amount, amount).SetParameter(TxParameterID::Fee, kFee)
            .SetParameter(TxParameterID::AssetID, Asset::s_BeamID)
            .SetParameter(TxParameterID::Lifetime, uint32_t(1000))
            .SetParameter(TxParameterID::OriginalToken, recipient.token);
        BaseTransaction::Creator::Ptr creator = std::make_shared<lelantus::PushTransaction::Creator>([this] { return db; });
        auto completed = creator->CheckAndCompleteParameters(params);
        if (recipient.type == TxAddressType::Offline)
        {
            auto peer = completed.GetParameter<WalletID>(TxParameterID::PeerAddr);
            require(peer && db->getAddress(*peer) && db->getVoucherCount(*peer) == 1,
                "creator did not retain address/voucher semantics");
        }
        tx = creator->Create(BaseTransaction::TxContext(*wallet, gateway, id));
        // Wallet's parameter application helper is translation-unit private.
        SubTxID sub = kDefaultSubTxID;
        for (const auto& p : completed.Pack())
        {
            if (p.first == TxParameterID::SubTxIndex)
            {
                Deserializer d;
                d.reset(p.second);
                d & sub;
            }
            else db->setTxParameter(id, sub, p.first, p.second, true, true);
        }
        require(tx->CanProcess(), "scoped PushTransaction cannot reach Update");
    }
    void sign()
    {
        tx->Update();
        pump([&] { return gateway.registrations || gateway.failures || bool(gateway.delayed); });
        require(!gateway.forbidden && !network->requests, "SIGN attempted network access");
        require(!gateway.failures, "real PushTransaction failed");
    }
};

struct Captured
{
    ByteBuffer bytes;
    Merkle::Hash mainId;
    TxID walletId;
};

Captured successful(Session& session, Inputs inputs, TxAddressType recipient, Fault fault)
{
    session.sign();
    auto& g = session.gateway;
    require(g.registrations == 1 && g.async == 0, "capture did not finish async signing");
    auto tx = decode(g.bytes);
    require(tx->m_vInputs.size() == (inputs == Inputs::Shielded ? 0U : 1U), "wrong ordinary input selection");
    require(shieldedInputs(*tx) == (inputs == Inputs::Ordinary ? 0U : 1U), "wrong shielded input selection");
    require(g.listRequests == (inputs == Inputs::Ordinary ? 0U : 1U), "shielded signing did not use cached list");
    require(g.vouchers == (recipient == TxAddressType::Offline ? 1U : 0U), "wrong recipient voucher path");
    require(g.corruptions == (fault == Fault::None ? 0U : 1U), "adversarial signer context was not exercised");
    require(!g.bytes.empty(), "no complete transaction bytes");
    if (inputs != Inputs::Ordinary)
        require(tx->m_vKernels.front()->get_Subtype() == TxKernel::Subtype::ShieldedInput &&
            tx->m_vKernels.front()->get_ID() != g.kernelId, "fixture did not expose the first-kernel identity trap");
    for (const auto& r : g.requested)
        std::cout << "BUILDER_RANGE " << r.first << " " << r.second << '\n';
    const auto& main = mainKernel(*tx);
    TxKernel::Ptr savedMain;
    require(storage::getTxParameter(*session.db, g.walletTxId, TxParameterID::Kernel, savedMain) &&
        savedMain->get_Subtype() == TxKernel::Subtype::Std && savedMain->get_ID() == main.get_ID(),
        "wallet main kernel identity was replaced by normalized kernel position");
    auto& output = main.m_vNested.front()->CastTo_ShieldedOutput();
    if (recipient != TxAddressType::PublicOffline)
    {
        ShieldedTxo::Voucher voucher;
        PeerID endpoint;
        require(session.tx->GetParameter(TxParameterID::Voucher, voucher) &&
            session.tx->GetParameter(TxParameterID::PeerEndpoint, endpoint), "recipient voucher not persisted");
        voucher.m_Ticket = output.m_Txo.m_Ticket;
        require(voucher.IsValid(endpoint), "final recipient does not match prepared voucher");
        ShieldedTxo::Data::OutputParams recovered;
        ECC::Oracle oracle;
        oracle << output.get_Msg();
        require(recovered.Recover(output.m_Txo, voucher.m_SharedSecret, main.m_Height.m_Min, oracle),
            "cannot recover final recipient output");
        require(recovered.m_Value == session.tx->GetMandatoryParameter<Amount>(TxParameterID::Amount),
            "post-sync amount was not signed");
        if (recipient == TxAddressType::MaxPrivacy)
            require(ShieldedTxo::User::ToPackedMessage(recovered.m_User)->m_MaxPrivacyMinAnonymitySet == 64,
                "max privacy recipient flag lost");
    }
    session.tx->Retire();
    session.gateway.closed = true;
    session.db->FlushNow();
    auto baseline = parameters(*session.db);
    for (unsigned i = 0; i < 3; ++i) tick();
    require(parameters(*session.db) == baseline && g.registrations == 1 && !session.network->requests,
        "capture retirement leaked callback or registration");
    std::cout << "CAPTURE_BYTES " << g.bytes.size() << '\n';
    return {g.bytes, g.kernelId, g.walletTxId};
}

struct RelayConfig { std::string network, rules; };

// No sender DB, seed, keykeeper, TxID or signer cache enters this interface.
uint8_t relay(NodeProcessor& independentNode, const ByteBuffer& bytes, const RelayConfig& config)
{
    require(config.network == "synthetic-fakepow" && config.rules == Rules::get().get_SignatureStr(),
        "relay network/rules mismatch");
    auto tx = decode(bytes);
    mainKernel(*tx);
    Transaction::Context context;
    context.m_Height.m_Min = independentNode.m_Cursor.m_hh.m_Height + 1;
    require(tx->IsValid(context), "relay context-free validation rejected transaction");
    uint32_t charge = 0;
    // false is mandatory: TestValid on shielded input does not verify its spend proof.
    auto status = independentNode.ValidateTxContextEx(*tx, context.m_Height, false, charge, nullptr, nullptr, nullptr);
    std::cout << "CONTEXT_STATUS " << unsigned(status) << " HEIGHT " << context.m_Height.m_Min
        << " " << context.m_Height.m_Max << '\n';
    return status;
}

void cacheBoundaries(const Cache& good)
{
    good.admit(good.rules, 1, good.tip);
    auto bad = good;
    bad.ranges.clear();
    rejects([&] { bad.admit(good.rules, 1, good.tip); }, "missing cache admitted");
    bad = good;
    bad.rules += "-wrong";
    rejects([&] { bad.admit(good.rules, 1, good.tip); }, "wrong rules admitted");
    bad = good;
    ++bad.generation;
    rejects([&] { bad.admit(good.rules, 1, good.tip); }, "stale generation admitted");
    bad = good;
    bad.valid = false;
    rejects([&] { bad.admit(good.rules, 1, good.tip); }, "reorg invalidation admitted");
    bad = good;
    bad.tip.m_Prev.m_pData[0] ^= 1;
    rejects([&] { bad.admit(good.rules, 1, good.tip); }, "replacement checkpoint admitted");
    auto r = good.ranges.begin()->first;
    auto response = good.ranges.begin()->second;
    peerAgreement("peer-a", "peer-b", r, r, response, response);
    rejects([&] { peerAgreement("peer-a", "peer-a", r, r, response, response); }, "duplicate peer admitted");
    auto corrupt = response;
    corrupt.m_State1.m_pData[0] ^= 1;
    rejects([&] { peerAgreement("peer-a", "peer-b", r, r, response, corrupt); }, "peer State1 mismatch admitted");
    corrupt = response;
    corrupt.m_Items.clear();
    rejects([&] { peerAgreement("peer-a", "peer-b", r, r, response, corrupt); }, "peer length mismatch admitted");
    corrupt = response;
    corrupt.m_Items.front().m_X.m_pData[0] ^= 1;
    rejects([&] { peerAgreement("peer-a", "peer-b", r, r, response, corrupt); }, "peer commitment mismatch admitted");
    auto other = r;
    ++other.first;
    rejects([&] { peerAgreement("peer-a", "peer-b", r, other, response, response); }, "peer range mismatch admitted");
}

void delayedBoundary(Session& s, unsigned mode)
{
    s.sign();
    require(bool(s.gateway.delayed) && !s.gateway.registrations, "shielded callback was not held");
    // Retire fences actual Core callbacks; the fixture owns cancel/close policy.
    s.tx->Retire();
    if (mode == 0) s.tx->Cancel();
    if (mode == 1) { ++s.gateway.cache.generation; s.gateway.cache.valid = false; }
    if (mode == 2) { s.gateway.closed = true; s.tx.reset(); }
    s.db->FlushNow();
    auto before = parameters(*s.db);
    auto cb = std::move(s.gateway.delayed);
    auto range = s.gateway.delayedRange;
    cb(range.first, range.second, s.gateway.delayedResponse);
    for (unsigned i = 0; i < 3; ++i) tick();
    require(parameters(*s.db) == before && !s.gateway.registrations &&
        !s.gateway.forbidden && !s.network->requests, "retired callback mutated or registered");
}

void fixtureReopen(const std::filesystem::path& path, const Captured& captured)
{
    auto db = WalletDB::open(path.string(), SecString(std::string("synthetic-offline-probe")));
    require(bool(db), "cannot reopen synthetic signer");
    Merkle::Hash id;
    require(storage::getTxParameter(*db, captured.walletId, TxParameterID::KernelID, id) && id == captured.mainId,
        "captured transaction row missing on reopen");
    auto before = parameters(*db);
    Wallet wallet(db);
    auto network = std::make_shared<NoNetwork>(wallet);
    wallet.SetNodeEndpoint(network);
    // Deliberately leave PushTransaction unregistered in this isolated fixture owner.
    // Production must exclude offline rows even WITH its normal creator registry.
    wallet.ResumeAllTransactions();
    for (unsigned i = 0; i < 3; ++i) tick();
    require(!network->requests && parameters(*db) == before, "fixture reopen posted a request or changed transaction");
    std::vector<CoinID> ordinary;
    std::vector<IPrivateKeyKeeper2::ShieldedInput> shielded;
    storage::getTxParameter(*db, captured.walletId, TxParameterID::InputCoins, ordinary);
    storage::getTxParameter(*db, captured.walletId, TxParameterID::InputCoinsShielded, shielded);
    require(!ordinary.empty() || !shielded.empty(), "reopen lost input inventory");
    for (const auto& input : ordinary)
    {
        Coin coin;
        coin.m_ID = input;
        require(db->findCoin(coin) && coin.m_spentTxId && *coin.m_spentTxId == captured.walletId,
            "fixture reopen released ordinary reservation");
    }
    for (const auto& input : shielded)
    {
        auto coin = db->getShieldedCoin(input.m_Key);
        require(coin && coin->m_spentTxId && *coin->m_spentTxId == captured.walletId,
            "fixture reopen released shielded reservation");
    }
}

void run(const std::filesystem::path& dir)
{
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
    Rules::Scope rulesScope(rules);
    // Fixed seeds, amounts and chain schedule; Core entropy and cryptography remain unchanged.
    auto fundingOwner = createDb(dir / "funding-owner.db", kSenderSeed);
    auto minerDb = createDb(dir / "synthetic-miner.db", 90817);
    auto miner = minerDb->get_MasterKdf();
    NodeProcessor node;
    node.m_Horizon.SetInfinite();
    node.Initialize((dir / "canonical-node.db").string().c_str());
    Funding funds;
    while (node.m_Cursor.m_hh.m_Height < 29) mine(node, miner);
    mine(node, miner, fund(miner, fundingOwner->get_MasterKdf(), 1, 30, false, 0, &funds));
    for (unsigned i = 0; i < kInitialPool; ++i)
        mine(node, miner, fund(miner, i ? miner : fundingOwner->get_MasterKdf(), 2 + i, 31 + i,
            true, i, i ? nullptr : &funds));
    require(node.m_Extra.m_ShieldedOutputs == kInitialPool, "synthetic pool count mismatch");
    const Cache cache = priorSync(node);
    std::cout << "SYNTHETIC_SETTINGS pool=" << cache.count << " proofMax=" << rules.Shielded.m_ProofMax.get_N()
        << " proofMin=" << rules.Shielded.m_ProofMin.get_N() << " backlog=" << rules.Shielded.MaxWindowBacklog << '\n';
    fundingOwner.reset();
    cacheBoundaries(cache);
    std::cout << "CACHE_BOUNDARIES_OK missing/rules/generation/reorg/checkpoint/quorum\n";
    const RelayConfig relayConfig{"synthetic-fakepow", cache.rules};
    unsigned scenario = 0;
    Captured large;
    for (Inputs inputs : {Inputs::Ordinary, Inputs::Shielded, Inputs::Mixed})
        for (TxAddressType type : {TxAddressType::Offline, TxAddressType::PublicOffline, TxAddressType::MaxPrivacy})
        {
            ++scenario;
            const auto path = dir / ("signer-" + std::to_string(scenario) + ".db");
            Captured result;
            {
                Session s(path, funds, cache, inputs, Fault::None);
                // Both choices occur after prior sync and before the first Update.
                auto recipient = chooseRecipient(dir, scenario, type);
                const Amount amount = inputs == Inputs::Mixed ? 60'000'000 : 20'000'000 + scenario;
                s.prepare(recipient, amount, scenario);
                result = successful(s, inputs, type, Fault::None);
            }
            require(relay(node, result.bytes, relayConfig) == proto::TxStatus::Ok, "independent contextual relay rejected");
            fixtureReopen(path, result);
            if (inputs == Inputs::Shielded && type == TxAddressType::Offline) large = result;
            std::cout << "SCENARIO_ACCEPTED " << scenario << '\n';
        }
    for (Fault fault : {Fault::State1, Fault::WindowItems})
    {
        ++scenario;
        Captured bad;
        {
            Session s(dir / ("signer-" + std::to_string(scenario) + ".db"), funds, cache, Inputs::Shielded, fault);
            auto recipient = chooseRecipient(dir, scenario, TxAddressType::MaxPrivacy);
            s.prepare(recipient, 20'000'000, scenario);
            bad = successful(s, Inputs::Shielded, recipient.type, fault);
        }
        // relay requires context-free validity before checking the independent spend proof.
        require(relay(node, bad.bytes, relayConfig) == proto::TxStatus::InvalidInput,
            "independent node accepted corrupted signer State1/window");
        std::cout << "CONTEXT_REJECTED " << (fault == Fault::State1 ? "corruptState1" : "window") << '\n';
    }
    for (unsigned mode = 0; mode < 3; ++mode)
    {
        ++scenario;
        Session s(dir / ("delayed-" + std::to_string(mode) + ".db"), funds, cache, Inputs::Shielded, Fault::Hold);
        auto recipient = chooseRecipient(dir, scenario, TxAddressType::PublicOffline);
        s.prepare(recipient, 20'000'000, scenario);
        delayedBoundary(s, mode);
        std::cout << "CALLBACK_DRAIN_OK " << mode << '\n';
    }
    {
        ++scenario;
        auto missing = cache;
        missing.ranges.clear();
        Session s(dir / "missing-context.db", funds, missing, Inputs::Shielded, Fault::None);
        auto recipient = chooseRecipient(dir, scenario, TxAddressType::PublicOffline);
        rejects([&] { s.prepare(recipient, 20'000'000, scenario); }, "missing context reached the signer");
        require(!s.tx && s.db->getAllTxParameters().empty() && !s.gateway.failures && !s.network->requests,
            "missing context created a transaction or became a coin-selection failure");
        std::cout << "MISSING_CONTEXT_REJECTED_BEFORE_SIGN\n";
    }
    auto trailing = large.bytes;
    trailing.push_back(0);
    rejects([&] { decode(trailing); }, "trailing bytes accepted");
    auto truncated = large.bytes;
    truncated.pop_back();
    rejects([&] { decode(truncated); }, "truncated bytes accepted");
    rejects([&] { relay(node, large.bytes, {"wrong-network", cache.rules}); }, "wrong relay network admitted");
    rejects([&] { relay(node, large.bytes, {relayConfig.network, "wrong-rules"}); }, "wrong relay rules admitted");
    auto wrongWindow = decode(large.bytes);
    for (auto& kernel : wrongWindow->m_vKernels)
        if (kernel->get_Subtype() == TxKernel::Subtype::ShieldedInput)
        {
            auto& input = kernel->CastTo_ShieldedInput();
            input.m_WindowEnd = cache.count + 1;
            input.m_Lazy_Msg.Invalidate();
            input.m_Lazy_ID.Invalidate();
        }
    require(relay(node, encode(*wrongWindow), relayConfig) == proto::TxStatus::InvalidContext,
        "node accepted a window beyond its independent pool");
    std::cout << "CONTEXT_REJECTED outofrange\n";
    auto parsed = decode(large.bytes);
    TxoID end = 0;
    for (const auto& kernel : parsed->m_vKernels)
        if (kernel->get_Subtype() == TxKernel::Subtype::ShieldedInput)
        {
            const auto& input = kernel->CastTo_ShieldedInput();
            require(input.m_SpendProof.m_Cfg == rules.Shielded.m_ProofMax, "expiry case did not use a large proof");
            end = input.m_WindowEnd;
        }
    require(end != 0, "large proof window missing");
    while (node.m_Extra.m_ShieldedOutputs <= end + rules.Shielded.MaxWindowBacklog)
    {
        auto index = node.m_Extra.m_ShieldedOutputs;
        auto height = node.m_Cursor.m_hh.m_Height + 1;
        mine(node, miner, fund(miner, miner, index + 2, height, true, index, nullptr));
    }
    require(mainKernel(*parsed).m_Height.IsInRange(node.m_Cursor.m_hh.m_Height + 1), "expiry confused with kernel lifetime");
    require(relay(node, large.bytes, relayConfig) == proto::TxStatus::InvalidContext, "expired large proof accepted");
    std::cout << "CONTEXT_REJECTED expiredlargeproof pool=" << node.m_Extra.m_ShieldedOutputs
        << " windowEnd=" << end << '\n';
    {
        // A NEW post-sync operation exercises Core's historical small-window selection.
        const Cache refreshed = priorSync(node);
        ++scenario;
        Captured small;
        {
            Session s(dir / "historical-small.db", funds, refreshed, Inputs::Shielded, Fault::None);
            auto recipient = chooseRecipient(dir, scenario, TxAddressType::PublicOffline);
            s.prepare(recipient, 20'000'000, scenario);
            small = successful(s, Inputs::Shielded, recipient.type, Fault::None);
        }
        auto tx = decode(small.bytes);
        require(shieldedInputs(*tx) == 1, "small-window case lost shielded input");
        for (const auto& kernel : tx->m_vKernels)
            if (kernel->get_Subtype() == TxKernel::Subtype::ShieldedInput)
                require(kernel->CastTo_ShieldedInput().m_SpendProof.m_Cfg == rules.Shielded.m_ProofMin,
                    "Core did not select its historical small proof");
        require(relay(node, small.bytes, relayConfig) == proto::TxStatus::Ok, "historical small proof rejected");
        std::cout << "HISTORICAL_SMALL_PROOF_ACCEPTED\n";
    }
    std::cout << "PRODUCTION_GATES: durable cache/quorum; SDK recovery active membership; resume/export exclusions; "
        "reservation journal; real privacy windows/Max; hostile import bounds; network relay; platform matrix\n";
}
} // namespace

int main()
{
    try
    {
        const auto dir = std::filesystem::temp_directory_path() / ("beam-offline-probe-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        require(std::filesystem::create_directory(dir), "synthetic directory already exists");
        run(dir); // All node, DB, wallet and reactor owners close before cleanup.
        std::filesystem::remove_all(dir);
        std::cout << "SYNTHETIC_CLEANUP_OK\nOFFLINE_SIGNING_PROBE_OK\n";
        return 0;
    }
    catch (const std::exception& error)
    {
        std::cerr << "OFFLINE_SIGNING_PROBE_FAILED: " << error.what() << '\n';
        return 1;
    }
}
