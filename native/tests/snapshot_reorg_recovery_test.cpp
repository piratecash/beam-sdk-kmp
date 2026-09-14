#include "wallet/core/wallet.h"
#include "wallet/core/base_tx_builder.h"
#include "wallet/core/common_utils.h"
#include "wallet/core/simple_transaction.h"
#include "wallet/transactions/lelantus/push_transaction.h"
#include "core/block_rw.h"
#include "utility/io/reactor.h"
#include "utility/io/timer.h"
#include <filesystem>
#include <iostream>
#include <deque>
#include <chrono>
#ifdef BEAM_SDK_KMP_REORG_JNI
#include <jni.h>
#endif

thread_local const beam::Rules* beam::Rules::s_pInstance = nullptr;

namespace {
using namespace beam;
using namespace beam::wallet;
void require(bool value, const char* message) { if (!value) throw std::runtime_error(message); }

ByteBuffer parameters(const IWalletDB& db)
{
    Serializer s;
    for (const auto& p : db.getAllTxParameters()) s & p.m_txID & p.m_subTxID & p.m_paramID & p.m_value;
    ByteBuffer result;
    s.swap_buf(result);
    return result;
}

class TestNetwork : public proto::FlyClient::NetworkStd
{
public:
    explicit TestNetwork(Wallet& wallet) : NetworkStd(wallet) {}
    std::deque<proto::FlyClient::Request::Ptr> requests;
    unsigned registrations = 0;
    std::vector<Transaction::Ptr> registeredPayloads;
    std::string adversary;
    void PostRequestInternal(proto::FlyClient::Request& r) override
    {
        if (r.get_Type() == proto::FlyClient::Request::Type::Transaction)
        {
            ++registrations;
            registeredPayloads.push_back(r.As<proto::FlyClient::RequestTransaction>().m_Msg.m_Transaction);
        }
        requests.emplace_back(&r);
    }
};

// This uses the real node GetBlock pruning/compaction logic and the exact pinned
// Node::Peer::GetBlock Recovery1 serialization (node.cpp), including its trailing full body.
proto::BodyBuffers wireBody(NodeProcessor& node, Height height, Height low, Height high)
{
    NodeDB::StateID sid;
    sid.m_Number = Block::Number(height);
    sid.m_Row = node.FindActiveAtStrict(sid.m_Number);
    proto::BodyBuffers body;
    require(node.GetBlock(sid, &body.m_Eternal, &body.m_Perishable,
        Block::Number(0), Block::Number(low), Block::Number(high), true), "node pruned required history");
    Block::Body full;
    Deserializer d;
    d.reset(body.m_Perishable);
    d & Cast::Down<Block::BodyBase>(full) & Cast::Down<TxVectors::Perishable>(full);
    Serializer s;
    s & full.m_vInputs & full.m_vOutputs.size();
    for (const auto& output : full.m_vOutputs) yas::detail::saveRecovery(s, *output, height);
    s & Cast::Down<Block::BodyBase>(full) & Cast::Down<TxVectors::Perishable>(full);
    s.swap_buf(body.m_Perishable);
    return body;
}

size_t outputCount(const proto::BodyBuffers& body)
{
    Deserializer d;
    d.reset(body.m_Perishable);
    std::vector<Input::Ptr> inputs;
    size_t count = 0;
    d & inputs & count;
    return count;
}

Transaction::Ptr spend(const CoinID& coin, const Key::IKdf::Ptr& kdf, Height height)
{
    auto tx = std::make_shared<Transaction>();
    auto in = std::make_unique<Input>();
    ECC::Scalar::Native inputKey, outputKey, kernelKey;
    CoinID::Worker(coin).Create(inputKey, in->m_Commitment, *kdf);
    tx->m_vInputs.push_back(std::move(in));
    auto out = std::make_unique<Output>();
    constexpr Amount fee = 1'000'000;
    CoinID change(coin.m_Value - fee, height + 1000, Key::Type::Regular);
    out->Create(height, outputKey, *kdf, change, *kdf, Output::OpCode::Public);
    tx->m_vOutputs.push_back(std::move(out));
    auto kernel = std::make_unique<TxKernelStd>();
    kernel->m_Fee = fee;
    kernel->m_Height.m_Min = height;
    kdf->DeriveKey(kernelKey, Key::ID(height + 1000, Key::Type::Kernel));
    kernel->Sign(kernelKey);
    tx->m_vKernels.push_back(std::move(kernel));
    inputKey += -outputKey;
    inputKey += -kernelKey;
    tx->m_Offset = inputKey;
    tx->Normalize();
    Transaction::Context context;
    context.m_Height.m_Min = height;
    require(tx->IsValid(context), "fixture spend signature invalid");
    return tx;
}

Transaction::Ptr shield(const CoinID& coin, const Key::IKdf::Ptr& kdf, Height height)
{
    auto tx = std::make_shared<Transaction>();
    auto input = std::make_unique<Input>();
    ECC::Scalar::Native key;
    CoinID::Worker(coin).Create(key, input->m_Commitment, *kdf);
    tx->m_vInputs.push_back(std::move(input));
    auto kernel = std::make_unique<TxKernelShieldedOutput>();
    kernel->m_Height.m_Min = height;
    kernel->m_Fee = 1'000'000;
    ShieldedTxo::Viewer viewer;
    viewer.FromOwner(*kdf, 0);
    ShieldedTxo::Data::Params params;
    params.m_Ticket.Generate(kernel->m_Txo.m_Ticket, viewer, 13U);
    params.m_Output.m_Value = coin.m_Value - kernel->m_Fee;
    params.m_Output.m_AssetID = 0;
    ZeroObject(params.m_Output.m_User);
    ECC::Oracle oracle;
    oracle << kernel->get_Msg();
    params.GenerateOutp(kernel->m_Txo, height, oracle);
    require(kernel->IsValid(height), "shielded fixture output invalid");
    key += -params.m_Output.m_k;
    tx->m_Offset = key;
    tx->m_vKernels.push_back(std::move(kernel));
    tx->Normalize();
    return tx;
}

Block::SystemState::Full mine(NodeProcessor& node, const Key::IKdf::Ptr& kdf, Transaction::Ptr tx = {})
{
    TxPool::Fluff pool;
    if (tx)
    {
        Transaction::Context context;
        context.m_Height.m_Min = node.m_Cursor.m_hh.m_Height + 1;
        require(tx->IsValid(context), "spend validation failed");
        uint32_t charge = 0;
        require(node.ValidateTxContextEx(*tx, context.m_Height, false, charge, nullptr, nullptr, nullptr) ==
            proto::TxStatus::Ok, "real node rejected restored coin spend");
        Transaction::KeyType key;
        tx->get_Key(key);
        TxPool::Stats stats;
        stats.From(*tx, context, 0, 0);
        pool.AddValidTx(std::move(tx), stats, key, TxPool::Fluff::State::Fluffed);
    }
    NodeProcessor::BlockContext block(pool, 0, *kdf, *kdf);
    require(node.GenerateNewBlock(block), "cannot generate fixture block");
    require(node.OnState(block.m_Hdr, PeerID()) == NodeProcessor::DataStatus::Accepted, "header rejected");
    Block::SystemState::ID id;
    block.m_Hdr.get_ID(id);
    require(node.OnBlock(id, block.m_Body.m_Perishable, block.m_Body.m_Eternal, PeerID()) ==
        NodeProcessor::DataStatus::Accepted, "body rejected");
    node.TryGoUp();
    require(node.m_Cursor.m_hh.m_Height == block.m_Hdr.get_Height(), "node did not advance");
    return block.m_Hdr;
}

// Real snapshot writer/parser: only the live tree is exported, so spent U and V are absent.
void snapshot(NodeProcessor& node, const std::string& path)
{
    struct Source : Block::ChainWorkProof::ISource
    {
        NodeProcessor& node;
        explicit Source(NodeProcessor& n) : node(n) {}
        void get_StateAt(Block::SystemState::Full& state, const Difficulty::Raw& work) override
        { node.get_DB().get_State(node.get_DB().FindStateWorkGreater(work), state); }
        void get_Proof(Merkle::IProofBuilder& proof, Block::Number number) override
        { node.m_Mmr.m_States.get_Proof(proof, number.v ? number.v - 1 : 0); }
    } source(node);
    Block::ChainWorkProof cwp;
    cwp.Create(source, node.m_Cursor.m_Full);
    NodeProcessor::Evaluator(node).get_Live(cwp.m_hvRootLive);
    struct Traveler : RadixTree::ITraveler
    {
        RecoveryInfo::Writer writer;
        NodeDB* db = nullptr;
        void output(TxoID id, const UtxoTree::Key::Data& key)
        {
            NodeDB::WalkerTxo row;
            db->TxoGetValue(row, id);
            Output out;
            Deserializer d;
            d.reset(row.m_Value.p, row.m_Value.n);
            d & out;
            Height height = key.m_Maturity - out.get_MinMaturity(0);
            yas::binary_oarchive<std::FStream, SERIALIZE_OPTIONS> s(writer.m_Stream);
            s & height;
            yas::detail::saveRecovery(s, out, height);
        }
        bool OnLeaf(const RadixTree::Leaf& leaf) override
        {
            const auto& n = Cast::Up<UtxoTree::MyLeaf>(leaf);
            UtxoTree::Key::Data key;
            key = n.m_Key;
            if (n.IsExt())
                for (auto p = n.m_pIDs.get_Strict()->m_pTop.get_Strict(); p; p = p->m_pNext.get()) output(p->m_ID, key);
            else output(n.m_ID, key);
            return true;
        }
    } traveler;
    traveler.db = &node.get_DB();
    traveler.writer.Open(path.c_str(), cwp, node.m_Cursor.m_hh.m_Height);
    node.get_Utxos().Traverse(traveler);
    using Stream = yas::binary_oarchive<std::FStream, SERIALIZE_OPTIONS>;
    Stream stream(traveler.writer.m_Stream);
    stream & MaxHeight;
    struct ShieldedWriter : NodeProcessor::KrnWalkerShielded
    {
        Stream& stream;
        explicit ShieldedWriter(Stream& s) : stream(s) {}
        bool OnKrnEx(const TxKernelShieldedInput& kernel) override
        { uint8_t flags = 0; stream & m_Height & flags & kernel.m_SpendProof.m_SpendPk; return true; }
        bool OnKrnEx(const TxKernelShieldedOutput& kernel) override
        {
            uint8_t flags = RecoveryInfo::Flags::Output;
            ShieldedTxo txo;
            txo = kernel.m_Txo;
            if (txo.m_pAsset) flags |= RecoveryInfo::Flags::HadAsset;
            txo.m_pAsset.reset();
            stream & m_Height & flags & txo & kernel.get_Msg();
            return true;
        }
    } walker(stream);
    Block::NumberRange range;
    range.m_Min = node.FindAtivePastHeight(Rules::get().pForks[2].m_Height);
    range.m_Max = node.m_Cursor.m_Full.m_Number;
    node.EnumKernels(walker, range);
    stream & MaxHeight;
    stream & (Asset::s_MaxCount + 1);
}
}

namespace beam::wallet {
class SnapshotReorgRecoveryTestAccess
{
public:
    static void completeAtInitialFlush(Wallet& w, bool pack)
    {
        const auto height = w.GetEventsHeightNext();
        // The caller fails the initial flush before any body decoding; supply an already
        // quorum-accepted callback at the next cursor to isolate that entry fence.
        w.m_VerifiedRecoveryCursor.m_Height = height;
        if (pack)
        {
            Wallet::MyRequestBodyPack::Ptr request(new Wallet::MyRequestBodyPack);
            request->m_RecoveryGeneration = w.m_BodyRequestGeneration;
            request->m_QuorumAccepted = true;
            request->m_StartHeight = height;
            request->m_Msg.m_CountExtra = Block::Number(0);
            request->m_Res.m_Bodies.resize(1);
            w.OnRequestComplete(*request);
        }
        else
        {
            Wallet::MyRequestBody::Ptr request(new Wallet::MyRequestBody);
            request->m_RecoveryGeneration = w.m_BodyRequestGeneration;
            request->m_QuorumAccepted = true;
            request->m_Height = height;
            w.OnRequestComplete(*request);
        }
    }
    static void fork(Wallet& w, const Block::SystemState::Full& tip)
    {
        w.OnRollingBack(tip.get_Height() + 1);
        w.m_WalletDB->get_History().DeleteFrom(tip.get_Height());
        w.m_WalletDB->get_History().AddStates(&tip, 1);
        w.OnRolledBack();
    }
    static void tip(Wallet& w, const Block::SystemState::Full& tip)
    {
        w.m_WalletDB->get_History().AddStates(&tip, 1);
        w.OnNewTip();
    }
    static void fail(Wallet& w) { w.ReportBodyQuorumFailure("fixture quorum failure"); }
    static void event(Wallet& w, Height h, const proto::Event::Base& e) { w.ProcessRecoveryEvent(h, e); }
    static void activate(Wallet& w, const BaseTransaction::Ptr& tx) { w.MakeTransactionActive(tx); }
    static void queued(Wallet& w, const BaseTransaction::Ptr& tx) { w.UpdateOnSynced(tx); }
    static void update(Wallet& w, const BaseTransaction::Ptr& tx) { w.UpdateActiveTransaction(tx); }
    static RecognitionRecovery state(const Wallet& w) { return w.m_RecognitionRecovery; }
    static auto listCallback(proto::FlyClient::Request& r)
    { return static_cast<Wallet::MyRequestShieldedList&>(r).m_callback; }
    static void markBeforeHistory(Wallet& w, Height firstRemoved) { w.OnRollingBack(firstRemoved); }
    static void resync(Wallet& w) { w.OnTipUnchanged(); }
    static void proof(Wallet& w, const TxID& id, const ECC::Point& serial, INegotiatorGateway::ProofShildedOutputCallback&& callback)
    { w.get_proof_shielded_output(id, serial, std::move(callback)); }
};
}

namespace {
using Access = SnapshotReorgRecoveryTestAccess;

void drain(TestNetwork& network, NodeProcessor& node, bool duplicatePeer = false, size_t limit = 1000)
{
    size_t sequence = 0;
    while (!network.requests.empty() && sequence < limit)
    {
        auto r = network.requests.front();
        network.requests.pop_front();
        if (!r->m_pTrg) continue;
        ++sequence;
        r->m_RecoveryResponder = io::Address(duplicatePeer ? 1 : 1 + sequence % 2, 10000);
        using R = proto::FlyClient::Request;
        if (r->get_Type() == R::Type::Body)
        {
            auto& b = r->As<proto::FlyClient::RequestBody>();
            if (b.m_Msg.m_Top.m_Number.v)
                b.m_Res.m_Body = wireBody(node, b.m_Msg.m_Top.m_Number.v, 0, 0);
        }
        else if (r->get_Type() == R::Type::BodyPack)
        {
            auto& b = r->As<proto::FlyClient::RequestBodyPack>();
            // A short valid pack makes cursor/reopen and idempotency observable independently.
            Height start = b.m_Msg.m_Top.m_Number.v - b.m_Msg.m_CountExtra.v;
            Height end = std::min(start + 23, b.m_Msg.m_Top.m_Number.v);
            if (!network.m_RequestAdmission) // independent archive/full-scan reference
                b.m_Msg.m_HorizonLo1.v = b.m_Msg.m_HorizonHi1.v = 0;
            for (Height h = start; h <= end; ++h)
                b.m_Res.m_Bodies.push_back(wireBody(node, h, b.m_Msg.m_HorizonLo1.v, b.m_Msg.m_HorizonHi1.v));
            if (network.adversary == "pruned") b.m_Res.m_Bodies.clear();
            else if (network.adversary == "malformed" ||
                (network.adversary == "mismatch" && sequence % 2))
                b.m_Res.m_Bodies.back().m_Perishable.clear();
        }
        else if (r->get_Type() == R::Type::EnumHdrs)
        {
            auto& request = r->As<proto::FlyClient::RequestEnumHdrs>();
            Block::SystemState::Full state;
            node.get_DB().get_State(node.FindActiveAtStrict(Block::Number(request.m_Msg.m_Height.m_Min)), state);
            request.m_vStates.push_back(state);
        }
        else if (r->get_Type() != R::Type::StateSummary) continue;
        auto* handler = r->m_pTrg;
        handler->OnComplete(*r);
    }
}

struct ProbeTx : BaseTransaction
{
    unsigned updates = 0;
    explicit ProbeTx(const TxContext& context) : BaseTransaction(TxType::Simple, context) {}
    bool IsInSafety() const override { return false; }
    void UpdateImpl() override { ++updates; }
};

struct ProbeCreator : BaseTransaction::Creator
{
    BaseTransaction::Ptr Create(const BaseTransaction::TxContext& context) override
    { return std::make_shared<ProbeTx>(context); }
};

struct ProbeBuilder : BaseTxBuilder
{
    using BaseTxBuilder::BaseTxBuilder;
    using BaseTxBuilder::KeyKeeperHandler;
};

struct Signer : ProbeBuilder::KeyKeeperHandler
{
    unsigned& success;
    Signer(BaseTxBuilder& builder, unsigned& count) : KeyKeeperHandler(builder, builder.m_Signing), success(count) {}
    void OnSuccess(BaseTxBuilder& builder) override
    {
        ++success;
        builder.m_Tx.SetParameter(TxParameterID::KernelProofHeight, Height(999));
        Detach(builder, BaseTxBuilder::Stage::Done);
    }
};

void kernelResponseAdmission(WalletDB::Ptr db, NodeProcessor& node, const Merkle::Hash& kernelId)
{
    using Network = proto::FlyClient::NetworkStd;
    Wallet wallet(db);
    Network network(wallet);
    network.m_RequestAdmission = [](const proto::FlyClient::Request& r) { return r.m_WalletEpoch == 2; };
    Network::Connection connection(network);
    connection.m_Tip = node.m_Cursor.m_Full;
    db->get_History().AddStates(&connection.m_Tip, 1);
    proto::ProofKernel response;
    NodeDB::StateID sid;
    require(node.get_ProofKernel(&response.m_Proof.m_Inner, nullptr, sid, kernelId, nullptr) != 0,
        "fixture kernel missing");
    node.get_DB().get_State(sid.m_Row, response.m_Proof.m_State);
    Merkle::ProofBuilderHard builder;
    node.m_Mmr.m_States.get_Proof(builder, node.m_Mmr.m_States.N2I(response.m_Proof.m_State.m_Number));
    response.m_Proof.m_Outer.swap(builder.m_Proof);
    struct HistoryProof : NodeProcessor::ProofBuilderHard
    {
        using NodeProcessor::ProofBuilderHard::ProofBuilderHard;
        bool get_History(Merkle::Hash&) override { return false; }
    } historyProof(node, response.m_Proof.m_Outer);
    historyProof.GenerateProof();
    require(connection.m_Tip.IsValidProofKernel(kernelId, response.m_Proof), "fixture kernel proof invalid");
    struct Handler : proto::FlyClient::Request::IHandler
    {
        unsigned completions = 0;
        void OnComplete(proto::FlyClient::Request&) override { ++completions; }
    } handler;
    for (unsigned mode = 0; mode < 4; ++mode)
    {
        proto::FlyClient::RequestKernel::Ptr request(new proto::FlyClient::RequestKernel);
        request->m_WalletEpoch = mode == 3 ? 2 : 1;
        request->m_Msg.m_ID = kernelId;
        request->m_pTrg = mode == 2 ? nullptr : &handler;
        auto* entry = new Network::RequestNode;
        entry->m_pRequest = request;
        connection.m_lst.push_back(*entry);
        auto delivered = response;
        if (mode == 1) delivered.m_Proof.m_Inner.clear(); // stale malformed data must not be validated
        connection.OnMsg(std::move(delivered));
        require(connection.m_lst.empty() && network.m_lst.empty(), "kernel response leaked or retried request");
        Block::SystemState::Full found;
        const bool inserted = db->get_History().get_At(found, response.m_Proof.m_State.get_Height());
        require(inserted == (mode == 3) && handler.completions == (mode == 3 ? 1U : 0U),
            "stale kernel response mutated history/callback or current response was discarded");
        if (mode != 3) require(!request->m_pTrg && request->m_Res.m_Proof.empty(), "stale payload retained");
    }
    std::cout << "SNAPSHOT_REORG_KERNEL_RESPONSE_ADMISSION_OK" << std::endl;
}

void assetsResponseAdmission(WalletDB::Ptr db, const Block::SystemState::Full& tip)
{
    using Network = proto::FlyClient::NetworkStd;
    Wallet wallet(db);
    Network network(wallet);
    network.m_RequestAdmission = [](const proto::FlyClient::Request& r) { return r.m_WalletEpoch == 2; };
    Network::Connection connection(network);
    connection.m_Tip = tip;
    db->get_History().AddStates(&tip, 1);
    struct Handler : proto::FlyClient::Request::IHandler
    {
        unsigned completions = 0;
        void OnComplete(proto::FlyClient::Request&) override { ++completions; }
    } handler;
    for (unsigned mode = 0; mode < 4; ++mode)
    {
        proto::FlyClient::RequestAssetsListAt::Ptr request(new proto::FlyClient::RequestAssetsListAt);
        request->m_WalletEpoch = mode >= 2 ? 2 : 1;
        request->m_pTrg = mode == 2 ? nullptr : &handler;
        request->m_Msg.m_Aid0 = 2;
        request->m_Res.emplace_back(); // retain an earlier page
        request->m_Res.back().m_ID = 1;
        auto* entry = new Network::RequestNode;
        entry->m_pRequest = request;
        connection.m_lst.push_back(*entry);
        proto::AssetsListAt response;
        response.m_bMore = mode != 3;
        if (mode != 1) // stale empty page with bMore must not be validated
        {
            response.m_Assets.emplace_back();
            response.m_Assets.back().m_ID = 2;
        }
        connection.OnMsg(std::move(response));
        require(connection.m_lst.empty() && network.m_lst.empty(), "assets response leaked or retried request");
        require(handler.completions == (mode == 3 ? 1U : 0U), "assets response admission callback mismatch");
        require(request->m_Res.front().m_ID == 1 && request->m_Res.size() == (mode == 3 ? 2U : 1U) &&
            request->m_Msg.m_Aid0 == (mode == 3 ? 3U : 2U), "assets response admission mutated pagination");
        if (mode != 3) require(!request->m_pTrg, "stale assets request was not cancelled");
    }
    std::cout << "SNAPSHOT_REORG_ASSETS_RESPONSE_ADMISSION_OK" << std::endl;
}

void recoveryObservers(const std::shared_ptr<WalletDB>& db, const std::string& path, const SecString& password)
{
    TxDescription first(GenerateTxID(), TxType::Simple), second(GenerateTxID(), TxType::PushTransaction);
    first.m_status = TxStatus::Registering;
    second.m_status = TxStatus::Completed;
    db->saveTx(first);
    db->saveTx(second);
    db->FlushNow();
    struct ThrowingObserver : IWalletDbObserver
    {
        unsigned coins = 0, shielded = 0, transactions = 0;
        void onCoinsChanged(ChangeAction, const std::vector<Coin>&) override { ++coins; throw 1; }
        void onShieldedCoinsChanged(ChangeAction, const std::vector<ShieldedCoin>&) override
        { ++shielded; throw std::runtime_error("observer failure"); }
        void onTransactionChanged(ChangeAction, const std::vector<TxDescription>&) override
        { ++transactions; throw std::runtime_error("observer failure"); }
    } throwing;
    struct Observer : IWalletDbObserver
    {
        unsigned coins = 0, shielded = 0, transactions = 0;
        ChangeAction action = ChangeAction::Added;
        std::vector<TxDescription> rows;
        std::function<void()> committed;
        void onCoinsChanged(ChangeAction, const std::vector<Coin>&) override { ++coins; }
        void onShieldedCoinsChanged(ChangeAction, const std::vector<ShieldedCoin>&) override { ++shielded; }
        void onTransactionChanged(ChangeAction a, const std::vector<TxDescription>& items) override
        { ++transactions; action = a; rows = items; committed(); }
    } observer;
    bool durableAtNotification = false;
    observer.committed = [&] {
        auto reopened = WalletDB::open(path, password);
        auto tx = reopened->getTx(first.m_txId);
        durableAtNotification = tx && tx->m_status == TxStatus::Completed && reopened->getTx(second.m_txId);
    };
    db->Subscribe(&throwing);
    db->Subscribe(&observer);
    db->BeginRecoveryBatch();
    first.m_status = TxStatus::Completed;
    require(storage::setTxParameter(*db, first.m_txId, TxParameterID::Status, first.m_status, true),
        "notifying transaction parameter did not change");
    require(!observer.transactions && !throwing.transactions, "replay notified before commit");
    db->EndRecoveryBatch(true);
    require(durableAtNotification && observer.action == ChangeAction::Reset && observer.rows.size() == 2,
        "transaction reset was not authoritative/durable");
    require(std::all_of(observer.rows.begin(), observer.rows.end(), [](const auto& tx) {
        return tx.m_status == TxStatus::Completed;
    }), "transaction reset retained stale status");
    auto unchangedCounts = [&] {
        return observer.coins == 1 && observer.shielded == 1 && observer.transactions == 1 &&
            throwing.coins == 1 && throwing.shielded == 1 && throwing.transactions == 1;
    };
    require(unchangedCounts(), "throwing subscriber suppressed another reset/subscriber");
    for (bool failCommit : {false, true})
    {
        db->BeginRecoveryBatch();
        first.m_status = TxStatus::Failed;
        db->saveTx(first);
        if (failCommit)
        {
            db->FailCurrentTransactionForTests();
            bool failed = false;
            try { db->EndRecoveryBatch(true); } catch (const DatabaseException&) { failed = true; }
            require(failed && unchangedCounts(), "failed commit published recovery reset");
        }
        db->EndRecoveryBatch(false);
        require(unchangedCounts() && db->getTx(first.m_txId)->m_status == TxStatus::Completed,
            "rollback notified observers or retained transaction mutation");
    }
    db->BeginRecoveryBatch();
    db->EndRecoveryBatch(true);
    require(observer.transactions == 1 && throwing.transactions == 1 &&
        observer.coins == 2 && observer.shielded == 2 && throwing.coins == 2 && throwing.shielded == 2,
        "unchanged recovery batch emitted transaction reset or lost coin resets");
    db->Unsubscribe(&observer);
    db->Unsubscribe(&throwing);
    std::cout << "SNAPSHOT_REORG_TRANSACTION_RESET_DIRTY_COMMIT_ROLLBACK_OBSERVER_ISOLATION_OK" << std::endl;
}

void runFixture()
{
    auto reactor = io::Reactor::create();
    io::Reactor::Scope reactorScope(*reactor);
    Rules rules;
    rules.m_Consensus = Rules::Consensus::FakePoW;
    rules.AllowPublicUtxos = true;
    rules.TreasuryChecksum = Zero;
    rules.Maturity.Coinbase = 10;
    rules.pForks[1].m_Height = 16;
    rules.pForks[2].m_Height = 17;
    rules.UpdateChecksum();
    Rules::Scope rulesScope(rules);
    const auto dir = std::filesystem::temp_directory_path() / ("beam-reorg-fixture-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(dir);
    ECC::NoLeak<ECC::uintBig> seed;
    seed.V = 10383UL;
    SecString password(std::string("synthetic-reorg-fixture"));
    auto create = [&](const char* name, bool sdk) {
        return std::dynamic_pointer_cast<WalletDB>(WalletDB::init((dir / name).string(), password, seed, false,
            [sdk](IWalletDB& db) {
                if (sdk) storage::setVar(db, "beam.sdk.kmp.restore.v1", 4);
                storage::setVar(db, "beam.sdk.kmp.restore.initialized.v1", false);
            }));
    };
    recoveryObservers(create("observers.db", false), (dir / "observers.db").string(), password);
    auto db = create("snapshot.db", true);
    auto master = db->get_MasterKdf();
    NodeProcessor node;
    node.m_Horizon.SetInfinite();
    node.Initialize((dir / "node.db").string().c_str());
    constexpr Height S = 121, C = S - 100, F = S - 1;
    CoinID U(rules.get_Emission(C), C, Key::Type::Coinbase);
    CoinID V(rules.get_Emission(10), 10, Key::Type::Coinbase);
    U.set_Subkey(0, CoinID::Scheme::V_Miner0);
    V.set_Subkey(0, CoinID::Scheme::V_Miner0);
    CoinID shieldedFunding(rules.get_Emission(15), 15, Key::Type::Coinbase);
    shieldedFunding.set_Subkey(0, CoinID::Scheme::V_Miner0);
    std::vector<Block::SystemState::Full> headers;
    Merkle::Hash kernelId;
    for (Height h = 1; h <= S; ++h)
    {
        auto tx = h == 40 ? shield(shieldedFunding, master, h) :
            h == 60 ? spend(V, master, h) : h == S ? spend(U, master, h) : Transaction::Ptr{};
        if (h == 60) kernelId = tx->m_vKernels.front()->get_ID();
        headers.push_back(mine(node, master, tx));
    }
    kernelResponseAdmission(create("kernel.db", false), node, kernelId);
    assetsResponseAdmission(create("assets.db", false), node.m_Cursor.m_Full);
    require(outputCount(wireBody(node, 10, S - 20, S)) == 0, "compact horizon did not omit V");
    require(outputCount(wireBody(node, 10, 0, 0)) == 1, "complete horizon omitted V");
    const auto savedPruning = node.m_Extra.m_TxoHi;
    node.m_Extra.m_TxoHi = Block::Number(S);
    bool pruned = false;
    try { wireBody(node, 10, 0, 0); } catch (const std::exception&) { pruned = true; }
    node.m_Extra.m_TxoHi = savedPruning;
    require(pruned, "pruned history was accepted as complete");
    std::cout << "SNAPSHOT_REORG_HORIZON_COMPLETE_PRUNED_OK" << std::endl;

    auto wallet = std::make_shared<Wallet>(db);
    snapshot(node, (dir / "snapshot.bin").string());
    db->ImportRecovery((dir / "snapshot.bin").string(), *wallet);
    wallet->RecordSnapshotImport(headers.back());
    wallet->StartBodyRequestsAt(S + 1, db->get_ShieldedOuts(), S + 1, true);
    storage::setVar(*db, "beam.sdk.kmp.restore.initialized.v1", true);
    db->FlushNow();
    Coin absent;
    absent.m_ID = U;
    require(!db->findCoin(absent), "snapshot incorrectly contains spent U");
    const TxID registering = {{0x81}}, terminal = {{0x82}}, partial = {{0x83}};
    TxDescription existing(registering, TxType::PushTransaction);
    existing.m_status = TxStatus::Registering;
    existing.m_sender = true;
    db->saveTx(existing);
    existing.m_txId = terminal;
    existing.m_status = TxStatus::Completed;
    db->saveTx(existing);
    storage::setTxParameter(*db, registering, TxParameterID::KernelProofHeight, Height(0), false);
    storage::setTxParameter(*db, terminal, TxParameterID::KernelProofHeight, Height(C), false);
    storage::setTxParameter(*db, partial, TxParameterID::Message, ByteBuffer{1, 0, 255}, false);
    const ByteBuffer journal = {7, 4, 0, 9};
    db->setVarRaw("beam.sdk.kmp.send.v1.prepared", journal.data(), journal.size());
    db->setVarRaw("beam.sdk.kmp.send.v1.committing", journal.data(), journal.size());
    db->FlushNow();
    node.ManualRollbackTo(Block::Number(F));
    Access::fork(*wallet, headers[F - 1]);
    require(!wallet->IsRecoveryAdmissionOpen(), "rollback gate open");
    const auto raw = parameters(*db);
    auto replacement = mine(node, master);
    auto network = std::make_shared<TestNetwork>(*wallet);
    wallet->SetNodeEndpoint(network);
    wallet->ConfigureRecoveryQuorum();
    wallet->EnableBodyRequests(true);
    Access::tip(*wallet, replacement);
    drain(*network, node, true, 4);
    require(storage::getNextEventHeight(*db) == 0, "duplicate endpoint advanced cursor");
    drain(*network, node, false, 10);
    require(storage::getNextEventHeight(*db) > 1 && storage::getNextEventHeight(*db) <= S,
        "fixture failed to stop mid-replay");
    const Height cursor = storage::getNextEventHeight(*db);
    const auto generation = Access::state(*wallet).m_Generation;
    wallet.reset(); network.reset(); db.reset();
    db = std::dynamic_pointer_cast<WalletDB>(WalletDB::open((dir / "snapshot.db").string(), password));
    wallet = std::make_shared<Wallet>(db);
    require(!wallet->IsRecoveryAdmissionOpen() && !db->IsSelectionAllowed(), "reopen gate opened before quorum");
    require(storage::getNextEventHeight(*db) == cursor && Access::state(*wallet).m_Generation == generation,
        "reopen lost committed recovery progress");
    network = std::make_shared<TestNetwork>(*wallet);
    wallet->SetNodeEndpoint(network); wallet->ConfigureRecoveryQuorum(); wallet->EnableBodyRequests(true);
    Access::tip(*wallet, replacement);
    drain(*network, node);
    require(wallet->IsRecoveryAdmissionOpen(), "recovery never reached Ready");
    require(parameters(*db) == raw, "replay modified raw transaction parameters");
    ByteBuffer reopenedJournal;
    db->getBlob("beam.sdk.kmp.send.v1.prepared", reopenedJournal);
    require(reopenedJournal == journal, "Prepared journal changed");
    db->getBlob("beam.sdk.kmp.send.v1.committing", reopenedJournal);
    require(reopenedJournal == journal, "Committing journal changed");
    size_t eventsBefore = 0;
    db->visitEvents(0, [&](Height, ByteBuffer&&) { ++eventsBefore; return true; });
    Coin recovered; recovered.m_ID = U;
    const bool foundU = db->findCoin(recovered);
    if (!foundU || recovered.m_confirmHeight != C || recovered.m_spentHeight != MaxHeight)
    {
        std::cerr << "U diagnostic found=" << foundU << " confirm=" << recovered.m_confirmHeight
                  << " spent=" << recovered.m_spentHeight << " cursor=" << storage::getNextEventHeight(*db) << std::endl;
        db->visitCoins([](const Coin& c) { if (c.m_ID.m_Idx == C) std::cerr << "C coin=" << c.m_ID
            << " confirm=" << c.m_confirmHeight << " spent=" << c.m_spentHeight << std::endl; return true; });
    }
    require(foundU && recovered.m_confirmHeight == C && recovered.m_spentHeight == MaxHeight,
        "U missing or still spent after replay/reopen");
    unsigned copies = 0;
    db->visitCoins([&](const Coin& c) { if (c.m_ID == U) ++copies; return true; });
    require(copies == 1, "U duplicated");

    auto control = create("full.db", false);
    auto full = std::make_shared<Wallet>(control);
    auto fullNetwork = std::make_shared<TestNetwork>(*full);
    full->SetNodeEndpoint(fullNetwork); full->EnableBodyRequests(true);
    // Use complete requests for the independent full recognizer too.
    full->StartBodyRequestsAt(0, 0, 0);
    Access::tip(*full, replacement);
    drain(*fullNetwork, node);
    Coin expected; expected.m_ID = U;
    require(control->findCoin(expected) && expected.m_confirmHeight == recovered.m_confirmHeight &&
        expected.m_spentHeight == recovered.m_spentHeight, "independent full scan differs for U");
    require(db->get_ShieldedOuts() == control->get_ShieldedOuts() &&
        storage::getNextEventHeight(*db) == storage::getNextEventHeight(*control), "full scan cursor/count differ");
    require(db->get_ShieldedOuts() == 1, "fixture did not replay a real shielded output");
    auto available = [](const IWalletDB& ledger) {
        std::map<std::string, Amount> coins;
        ledger.visitCoins([&](const Coin& coin) {
            if (coin.m_status == Coin::Status::Available) coins.emplace(coin.toStringID(), coin.m_ID.m_Value);
            return true;
        });
        return coins;
    };
    const auto recoveredAvailable = available(*db);
    require(recoveredAvailable == available(*control) && recoveredAvailable.count(recovered.toStringID()) == 1,
        "recovered spendable identities/statuses/balance differ from full scan");
    Amount selectableBalance = 0;
    for (const auto& coin : recoveredAvailable) selectableBalance += coin.second;
    const auto selected = db->selectCoinsEx(selectableBalance, Asset::s_BeamID, false);
    require(std::count_if(selected.begin(), selected.end(), [&](const Coin& coin) { return coin.m_ID == U; }) == 1,
        "Core coin selection did not admit U exactly once");
    mine(node, master, spend(recovered.m_ID, db->get_MasterKdf(), S + 1));
    std::cout << "SNAPSHOT_REORG_U_REOPEN_FULL_SCAN_ACTUAL_SPEND_OK" << std::endl;

    // Start real builder handlers while Ready, then hold their success/failure/list callbacks
    // through a second fork. The old transaction stays strongly reachable like UpdateOnSynced.
    wallet->RegisterTransactionType(TxType::Simple, std::make_shared<ProbeCreator>());
    TxDescription send(GenerateTxID());
    send.m_status = TxStatus::Registering;
    send.m_sender = true;
    db->saveTx(send);
    storage::setTxParameter(*db, send.m_txId, TxParameterID::MinHeight, Height(1), false);
    auto old = std::make_shared<ProbeTx>(BaseTransaction::TxContext(*wallet, *wallet, send.m_txId));
    Access::activate(*wallet, old);
    auto signing = std::make_shared<ProbeBuilder>(*old, kDefaultSubTxID);
    auto failing = std::make_shared<ProbeBuilder>(*old, kDefaultSubTxID);
    unsigned signerSuccess = 0;
    auto successHandler = std::make_shared<Signer>(*signing, signerSuccess);
    auto failureHandler = std::make_shared<Signer>(*failing, signerSuccess);
    auto afterReadyBuilder = std::make_shared<ProbeBuilder>(*old, kDefaultSubTxID);
    auto afterReadyHandler = std::make_shared<Signer>(*afterReadyBuilder, signerSuccess);
    auto listBuilder = std::make_shared<ProbeBuilder>(*old, kDefaultSubTxID);
    ShieldedCoin shielded;
    shielded.m_CoinID = {};
    shielded.m_CoinID.m_Value = 50'000'000;
    shielded.m_TxoID = 0;
    shielded.m_confirmHeight = 10;
    shielded.m_spentTxId = send.m_txId;
    db->saveShieldedCoin(shielded);
    IPrivateKeyKeeper2::ShieldedInput shieldedInput;
    Cast::Down<ShieldedTxo::ID>(shieldedInput) = shielded.m_CoinID;
    shieldedInput.m_Fee = 1'000'000;
    listBuilder->m_Coins.m_InputShielded.push_back(shieldedInput);
    listBuilder->GenerateInOuts();
    auto listRequest = network->requests.back();
    require(listRequest->get_Type() == proto::FlyClient::Request::Type::ShieldedList, "real builder did not request shielded list");
    auto delayedList = Access::listCallback(*listRequest);
    unsigned proofCallbacks = 0;
    ECC::Point serial; serial = Zero;
    Access::proof(*wallet, send.m_txId, serial,
        [&](proto::ProofShieldedOutp&) { ++proofCallbacks; old->SetParameter(TxParameterID::Message, ByteBuffer{9}); });
    auto proofRequest = network->requests.back();
    Access::queued(*wallet, old);
    old->UpdateAsync();
    db->FlushNow();
    node.ManualRollbackTo(Block::Number(F));
    Access::fork(*wallet, headers[F - 1]);
    const auto callbackBaseline = parameters(*db);
    const auto reservation = db->getShieldedCoin(shielded.m_CoinID.m_Key)->m_spentTxId;
    successHandler->OnDone(IPrivateKeyKeeper2::Status::Success);
    failureHandler->OnDone(IPrivateKeyKeeper2::Status::Unspecified);
    proto::ShieldedList incompatible;
    incompatible.m_Items.resize(2); // requested list was clipped to the one existing index
    delayedList(0, 1, incompatible); // exercises the actual HandlerInputShielded callback
    old->Update();
    Access::update(*wallet, old);
    auto stopTimer = io::Timer::create(*reactor);
    stopTimer->start(1, false, [&] { reactor->stop(); });
    reactor->run();
    require(!listRequest->m_pTrg && !proofRequest->m_pTrg && !network->m_RequestAdmission(*proofRequest),
        "rollback retained transaction-bearing request");
    require(!signerSuccess && !proofCallbacks && !old->updates && parameters(*db) == callbackBaseline &&
        db->getShieldedCoin(shielded.m_CoinID.m_Key)->m_spentTxId == reservation,
        "stale callback changed parameters/reservation while recovering");
    // Recognition of shielded receipts must not restore or rewrite existing/partial TxIDs.
    for (const auto& id : {registering, terminal, partial})
    {
        proto::Event::Shielded event;
        event.m_CoinID = shielded.m_CoinID;
        event.m_CoinID.m_Key.m_nIdx = id[0];
        auto* message = ShieldedTxo::User::ToPackedMessage(event.m_CoinID.m_User);
        std::copy(id.begin(), id.end(), message->m_TxID.m_pData);
        event.m_TxoID = id[0];
        event.m_Flags = proto::Event::Flags::Add;
        Access::event(*wallet, C, event);
        Access::event(*wallet, C, event);
    }
    require(parameters(*db) == callbackBaseline, "shielded recovery rewrote raw Registering/terminal/partial parameters");
    replacement = mine(node, master);
    Access::tip(*wallet, replacement);
    drain(*network, node);
    require(wallet->IsRecoveryAdmissionOpen(), "repeated fork did not finish recovery");
    BaseTransaction::Ptr current;
    wallet->VisitActiveTransaction([&](const TxID& id, BaseTransaction::Ptr tx) { if (id == send.m_txId) current = tx; });
    require(current && current.get() != old.get() && current->GetTxID() == old->GetTxID(),
        "same TxID was not reconstructed as a new object");
    const auto readyBaseline = parameters(*db);
    afterReadyHandler->OnDone(IPrivateKeyKeeper2::Status::Success);
    delayedList(0, 1, incompatible);
    old->Update(); Access::update(*wallet, old); Access::queued(*wallet, old); Access::resync(*wallet);
    require(!old->updates && !signerSuccess && !proofCallbacks && parameters(*db) == readyBaseline,
        "old object revived after Ready");
    require(std::static_pointer_cast<ProbeTx>(current)->updates > 0, "replacement object never resumed");
    size_t eventsAfter = 0;
    db->visitEvents(0, [&](Height, ByteBuffer&&) { ++eventsAfter; return true; });
    require(eventsAfter == eventsBefore, "repeated replay duplicated common-prefix events");
    std::cout << "SNAPSHOT_REORG_RAW_PARAMETERS_JOURNAL_EVENTS_IDEMPOTENT_OK" << std::endl;
    std::cout << "SNAPSHOT_REORG_SIGNER_SUCCESS_FAILURE_ONLIST_PROOF_RETIRED_SAME_TXID_OK" << std::endl;
    Access::fail(*wallet);
    require(!wallet->IsRecoveryAdmissionOpen() && !db->IsSelectionAllowed() && network->registrations == 0,
        "quorum failure did not close idle admission");
    std::cout << "SNAPSHOT_REORG_QUORUM_IDLE_GATE_OK" << std::endl;

    const std::vector<std::string> crashPoints = {"before-marker", "after-marker", "reset-before-commit",
        "treasury-before-commit", "treasury-after-commit", "pack-before-commit", "pack-after-commit",
        "body-before-commit", "body-after-commit", "complete-before-commit", "complete-after-commit",
        "pack-commit-failure", "completion-commit-failure", "treasury-before-batch",
        "pack-before-batch", "body-before-batch"};
    unsigned crashIndex = 0;
    for (const auto& point : crashPoints)
    {
        const std::string name = "crash-" + std::to_string(crashIndex++) + ".db";
        auto crashDb = create(name.c_str(), true);
        auto engine = std::make_shared<Wallet>(crashDb);
        crashDb->ImportRecovery((dir / "snapshot.bin").string(), *engine);
        engine->RecordSnapshotImport(headers.back());
        engine->StartBodyRequestsAt(S + 1, crashDb->get_ShieldedOuts(), S + 1, true);
        storage::setVar(*crashDb, "beam.sdk.kmp.restore.initialized.v1", true);
        crashDb->FlushNow();
        auto transport = std::make_shared<TestNetwork>(*engine);
        engine->SetNodeEndpoint(transport); engine->ConfigureRecoveryQuorum(); engine->EnableBodyRequests(true);
        bool injected = false;
        Height committedCursor = 0;
        Wallet::SetRecoveryCheckpointForTests([&](const char* currentPoint) {
            if (injected) return;
            const bool commitFailure = (point == "pack-commit-failure" && std::string(currentPoint) == "pack-before-commit") ||
                (point == "completion-commit-failure" && std::string(currentPoint) == "complete-before-commit");
            if (point != currentPoint && !commitFailure) return;
            if ((point.find("pack") != std::string::npos) && crashDb->get_ShieldedOuts() == 0) return;
            injected = true;
            if (point.find("after-commit") != std::string::npos) committedCursor = storage::getNextEventHeight(*crashDb);
            if (commitFailure || point.find("before-batch") != std::string::npos)
            {
                storage::setVar(*crashDb, "fixture.pending.flush", 1);
                crashDb->FailCurrentTransactionForTests();
            }
            else throw std::runtime_error("injected recovery interruption");
        });
        try { Access::fork(*engine, headers[F - 1]); Access::tip(*engine, replacement); drain(*transport, node); }
        catch (const std::exception&) {
            require(injected, "unexpected failure before fault injection");
            require(point.find("before-batch") == std::string::npos, "initial recovery flush escaped failure handling");
        }
        Wallet::SetRecoveryCheckpointForTests({});
        require(injected, "crash fixture never reached requested fence");
        require(!engine->IsRecoveryAdmissionOpen(), "failed recovery opened admission");
        bool selectionDeferred = false;
        try { crashDb->selectCoinsEx(1, 0, false); } catch (const std::runtime_error&) { selectionDeferred = true; }
        require(selectionDeferred, "failed recovery allowed coin selection");
        engine.reset(); transport.reset(); crashDb.reset();
        crashDb = std::dynamic_pointer_cast<WalletDB>(WalletDB::open((dir / name).string(), password));
        engine = std::make_shared<Wallet>(crashDb);
        require(!engine->IsRecoveryAdmissionOpen(), "restart admitted before live validation");
        if (committedCursor) require(storage::getNextEventHeight(*crashDb) == committedCursor,
            "post-commit exception rolled back durable progress");
        transport = std::make_shared<TestNetwork>(*engine);
        engine->SetNodeEndpoint(transport); engine->ConfigureRecoveryQuorum(); engine->ResumeAllTransactions();
        engine->EnableBodyRequests(true);
        if (point == "before-marker") Access::fork(*engine, headers[F - 1]);
        Access::tip(*engine, replacement);
        if (point == "complete-after-commit")
        {
            drain(*transport, node, true, 4);
            require(!engine->IsRecoveryAdmissionOpen(), "completed reopen admitted with one live peer and no pending bodies");
        }
        drain(*transport, node);
        require(engine->IsRecoveryAdmissionOpen(), "restart did not complete recovery");
        require(crashDb->get_ShieldedOuts() == 1, "crash/restart lost shielded count");
        Coin coin; coin.m_ID = U;
        require(crashDb->findCoin(coin) && coin.m_spentHeight == MaxHeight, "crash/restart lost U");
        require(!transport->registrations, "crash/restart broadcast a new transaction");
        std::cout << "SNAPSHOT_REORG_CRASH_OK " << point << std::endl;
    }

    struct Observer : IWalletDbObserver
    {
        unsigned notifications = 0;
        void onCoinsChanged(ChangeAction, const std::vector<Coin>&) override { ++notifications; }
    };
    for (const auto* adversary : {"mismatch", "malformed", "pruned"})
    {
        auto brokenDb = create((std::string(adversary) + ".db").c_str(), true);
        auto engine = std::make_shared<Wallet>(brokenDb);
        brokenDb->ImportRecovery((dir / "snapshot.bin").string(), *engine);
        engine->RecordSnapshotImport(headers.back());
        engine->StartBodyRequestsAt(S + 1, brokenDb->get_ShieldedOuts(), S + 1, true);
        Access::fork(*engine, headers[F - 1]);
        const auto rawBefore = parameters(*brokenDb);
        Observer observer;
        brokenDb->Subscribe(&observer);
        auto transport = std::make_shared<TestNetwork>(*engine);
        transport->adversary = adversary;
        engine->SetNodeEndpoint(transport); engine->ConfigureRecoveryQuorum(); engine->EnableBodyRequests(true);
        Access::tip(*engine, replacement); drain(*transport, node);
        require(!engine->IsRecoveryAdmissionOpen() && !brokenDb->IsSelectionAllowed(), "adversarial body opened gate");
        Coin coin; coin.m_ID = U;
        require(storage::getNextEventHeight(*brokenDb) == 1 && brokenDb->get_ShieldedOuts() == 0 &&
            !brokenDb->findCoin(coin) && parameters(*brokenDb) == rawBefore, "failed pack leaked data/cursor/count");
        require(observer.notifications == 1, "failed pack leaked observer notifications"); // committed empty treasury only
        brokenDb->Unsubscribe(&observer);
        std::cout << "SNAPSHOT_REORG_ADVERSARY_OK " << adversary << std::endl;
    }

    for (int source : {2, 3}) // Height and Date keep the resolved original H independently.
    {
        auto birthdayDb = create(source == 2 ? "height.db" : "date.db", true);
        storage::setVar(*birthdayDb, "beam.sdk.kmp.restore.v1", source);
        auto engine = std::make_shared<Wallet>(birthdayDb);
        engine->StartBodyRequestsAt(100, 1, 100, false);
        birthdayDb->get_History().AddStates(&headers[99], 1);
        Access::fork(*engine, headers[49]);
        require(Access::state(*engine).m_OriginalBirthday == 100 &&
            engine->BodyRecognitionBoundaryForTests() == 51, "height/date original or effective boundary changed incorrectly");
        auto transport = std::make_shared<TestNetwork>(*engine);
        engine->SetNodeEndpoint(transport); engine->ConfigureRecoveryQuorum(); engine->EnableBodyRequests(true);
        Access::tip(*engine, replacement); drain(*transport, node);
        require(engine->IsRecoveryAdmissionOpen() && birthdayDb->get_ShieldedOuts() == 1, "height/date replay/count incomplete");
        Coin beforeBoundary; beforeBoundary.m_ID = U;
        require(!birthdayDb->findCoin(beforeBoundary), "height/date recognized before effective boundary");
        Coin replacementCoin;
        replacementCoin.m_ID = CoinID(rules.get_Emission(75), 75, Key::Type::Coinbase);
        replacementCoin.m_ID.set_Subkey(0, CoinID::Scheme::V_Miner0);
        require(birthdayDb->findCoin(replacementCoin), "height/date skipped newly included replacement output");
        std::cout << "SNAPSHOT_REORG_BIRTHDAY_OK " << source << std::endl;
    }

    auto sendDb = create("push-resume.db", true);
    auto sendEngine = std::make_shared<Wallet>(sendDb);
    sendDb->ImportRecovery((dir / "snapshot.bin").string(), *sendEngine);
    sendEngine->RecordSnapshotImport(headers.back());
    sendEngine->StartBodyRequestsAt(S + 1, sendDb->get_ShieldedOuts(), S + 1, true);
    Access::fork(*sendEngine, headers[F - 1]);
    auto sendNetwork = std::make_shared<TestNetwork>(*sendEngine);
    sendEngine->SetNodeEndpoint(sendNetwork); sendEngine->ConfigureRecoveryQuorum(); sendEngine->EnableBodyRequests(true);
    Access::tip(*sendEngine, replacement); drain(*sendNetwork, node);
    sendEngine->RegisterTransactionType(TxType::PushTransaction,
        std::make_shared<lelantus::PushTransaction::Creator>([sendDb] { return sendDb; }));
    const auto token = GenerateTokenDefaultAddr(TokenType::Offline, control, 1);
    auto receiver = ParseParameters(token);
    require(bool(receiver), "real send receiver token invalid");
    const auto operationTxId = GenerateTxID();
    auto sendParameters = CreateSimpleTransactionParameters(operationTxId);
    require(LoadReceiverParams(*receiver, sendParameters, TxAddressType::Offline), "real send receiver parameters invalid");
    sendParameters.SetParameter(TxParameterID::Amount, Amount(2'000'000))
        .SetParameter(TxParameterID::Fee, Amount(1'000'000)).SetParameter(TxParameterID::AssetID, Asset::s_BeamID)
        .SetParameter(TxParameterID::OriginalToken, token);
    require(sendEngine->StartTransaction(sendParameters) == operationTxId, "real send changed TxID");
    BaseTransaction::Ptr oldSend;
    sendEngine->VisitActiveTransaction([&](const TxID& id, BaseTransaction::Ptr tx) { if (id == operationTxId) oldSend = tx; });
    require(bool(oldSend) && !sendNetwork->registrations, "fixture did not pause real send before signer completion");
    Access::fork(*sendEngine, headers[F - 1]);
    const auto signingBaseline = parameters(*sendDb);
    stopTimer->start(10, false, [&] { reactor->stop(); }); reactor->run();
    require(parameters(*sendDb) == signingBaseline && !sendNetwork->registrations,
        "real async signer mutated/broadcast during recovery");
    Access::tip(*sendEngine, replacement);
    drain(*sendNetwork, node, false, 6); // fork once more while recognition is in progress
    const auto oldGeneration = Access::state(*sendEngine).m_Generation;
    Access::fork(*sendEngine, headers[F - 2]);
    require(Access::state(*sendEngine).m_Generation > oldGeneration && storage::getNextEventHeight(*sendDb) == 0,
        "fork during replay did not restart generation/cursor");
    Access::tip(*sendEngine, replacement); drain(*sendNetwork, node);
    for (unsigned attempt = 0; attempt < 10 && !sendNetwork->registrations; ++attempt)
    { stopTimer->start(20, false, [&] { reactor->stop(); }); reactor->run(); }
    BaseTransaction::Ptr resumedSend;
    sendEngine->VisitActiveTransaction([&](const TxID& id, BaseTransaction::Ptr tx) { if (id == operationTxId) resumedSend = tx; });
    require(resumedSend && resumedSend.get() != oldSend.get() && !oldSend->CanProcess(),
        "real PushTransaction object was not replaced under the same TxID");
    require(sendNetwork->registrations == 1 && sendNetwork->registeredPayloads.size() == 1,
        "real same-TxID resumption did not emit exactly one registration");
    unsigned txRows = 0;
    for (const auto& tx : sendDb->getTxHistory(TxType::ALL, 0, std::numeric_limits<int>::max()))
        if (tx.m_txId == operationTxId) ++txRows;
    require(txRows == 1, "real resumption duplicated Core transaction row");
    mine(node, master, sendNetwork->registeredPayloads.front());
    std::cout << "SNAPSHOT_REORG_REAL_PUSH_SAME_TXID_SINGLE_REGISTRATION_REPEATED_FORK_OK" << std::endl;
    for (bool pack : {false, true})
    {
        const auto name = pack ? "pack-entry.db" : "body-entry.db";
        auto entryDb = create(name, true);
        auto engine = std::make_shared<Wallet>(entryDb);
        entryDb->ImportRecovery((dir / "snapshot.bin").string(), *engine);
        engine->RecordSnapshotImport(headers.back());
        engine->StartBodyRequestsAt(S + 1, entryDb->get_ShieldedOuts(), S + 1, true);
        Access::fork(*engine, headers[F - 1]);
        auto transport = std::make_shared<TestNetwork>(*engine);
        engine->SetNodeEndpoint(transport); engine->ConfigureRecoveryQuorum(); engine->EnableBodyRequests(true);
        Access::tip(*engine, node.m_Cursor.m_Full); drain(*transport, node);
        require(engine->IsRecoveryAdmissionOpen(), "initial-flush fixture never opened admission");
        auto pending = std::make_shared<ProbeTx>(BaseTransaction::TxContext(*engine, *engine, GenerateTxID()));
        Access::activate(*engine, pending);
        auto builder = std::make_shared<ProbeBuilder>(*pending, kDefaultSubTxID);
        unsigned successes = 0, proofs = 0;
        auto callback = std::make_shared<Signer>(*builder, successes);
        Access::proof(*engine, pending->GetTxID(), serial, [&](proto::ProofShieldedOutp&) { ++proofs; });
        auto heldRequest = transport->requests.back();
        entryDb->FlushNow();
        const auto before = parameters(*entryDb);
        const auto cursorBefore = storage::getNextEventHeight(*entryDb);
        storage::setVar(*entryDb, "fixture.pending.flush", 1);
        entryDb->FailCurrentTransactionForTests();
        Access::completeAtInitialFlush(*engine, pack);
        require(!engine->IsRecoveryAdmissionOpen() && !entryDb->IsSelectionAllowed() && !pending->CanProcess() &&
            !heldRequest->m_pTrg && !transport->m_RequestAdmission(*heldRequest),
            "initial flush failure left admission or old callbacks live");
        callback->OnDone(IPrivateKeyKeeper2::Status::Success);
        pending->Update();
        require(!successes && !proofs && !pending->updates && parameters(*entryDb) == before &&
            storage::getNextEventHeight(*entryDb) == cursorBefore, "initial flush failure leaked state/callback effects");
        callback.reset(); builder.reset(); pending.reset(); heldRequest.reset();
        engine.reset(); transport.reset(); entryDb.reset();
        entryDb = std::dynamic_pointer_cast<WalletDB>(WalletDB::open((dir / name).string(), password));
        engine = std::make_shared<Wallet>(entryDb);
        require(!engine->IsRecoveryAdmissionOpen(), "initial-flush reopen admitted before validation");
        transport = std::make_shared<TestNetwork>(*engine);
        engine->SetNodeEndpoint(transport); engine->ConfigureRecoveryQuorum(); engine->ResumeAllTransactions();
        engine->EnableBodyRequests(true); Access::tip(*engine, node.m_Cursor.m_Full); drain(*transport, node);
        require(engine->IsRecoveryAdmissionOpen() && storage::getNextEventHeight(*entryDb) == cursorBefore,
            "initial-flush failure did not remain reopen-recoverable");
        std::cout << "SNAPSHOT_REORG_INITIAL_FLUSH_CLOSE_RETIRE_REOPEN_OK " << name << std::endl;
    }
    // Paths are synthetic and intentionally retained on failure for diagnosis.
}
}

#ifdef BEAM_SDK_KMP_REORG_JNI
extern "C" JNIEXPORT jstring JNICALL
Java_cash_p_beam_internal_SnapshotReorgRecoveryNativeTest_runNativeFixture(JNIEnv* env, jobject)
{
    try { runFixture(); return env->NewStringUTF("SNAPSHOT_REORG_JNI_OK"); }
    catch (const std::exception& error)
    {
        env->ThrowNew(env->FindClass("java/lang/IllegalStateException"), error.what());
        return nullptr;
    }
}
#else
int main()
{
    try { runFixture(); return 0; }
    catch (const std::exception& error) { std::cerr << "SNAPSHOT_REORG_FAILED: " << error.what() << std::endl; return 1; }
}
#endif
