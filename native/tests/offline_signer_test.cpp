#include "../src/beam_jni.cpp"
#define BEAM_SDK_REUSE_SIGNING_FIXTURE
#define Session ProbeSession
#define main unusedOfflineFeasibilityMain
#include "offline_signing_test.cpp"
#undef main
#undef Session
#ifndef _WIN32
#include <spawn.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
extern char** environ;
#endif

namespace beam::wallet {
class SnapshotReorgRecoveryTestAccess {
public:
    static void observe(Wallet& owner, const TxID& id, const Merkle::Hash& kernel,
        const proto::ProofKernel& proof) {
        Wallet::MyRequestKernel request;
        request.m_TxID = id; request.m_Msg.m_ID = kernel; request.m_Res = proof;
        owner.OnRequestComplete(request);
    }
    static void fork(Wallet& owner, const Block::SystemState::Full& tip) {
        owner.OnRollingBack(tip.get_Height() + 1);
        owner.get_History().DeleteFrom(tip.get_Height() + 1);
        owner.get_History().AddStates(&tip, 1);
        owner.OnRolledBack();
    }
    // What a completed recovery opens: admission and coin selection.
    // Body recognition of the transaction's own outputs, i.e. the change coming back from chain.
    static void receive(Wallet& owner, Height h, const Transaction& tx) {
        const auto kdf = owner.m_WalletDB->get_MasterKdf();
        for (const auto& output : tx.m_vOutputs) {
            proto::Event::Utxo event;
            if (!output->Recover(h, *kdf, event.m_Cid)) continue;
            event.m_Commitment = output->m_Commitment;
            event.m_Maturity = output->get_MinMaturity(h);
            event.m_Flags = proto::Event::Flags::Add;
            owner.ProcessRecoveryEvent(h, event);
        }
    }
    static void admit(Wallet& owner) {
        owner.m_RecoveryAdmission = true;
        std::dynamic_pointer_cast<WalletDB>(owner.m_WalletDB)->SetSelectionAllowed(true);
    }
    // What body recognition records when the block carrying the transaction spends its inputs.
    static void spend(Wallet& owner, Height h, const std::vector<CoinID>& ordinary,
        const std::vector<IPrivateKeyKeeper2::ShieldedInput>& shielded) {
        for (const auto& id : ordinary) {
            proto::Event::Utxo event;
            event.m_Cid = id; event.m_Flags = 0;
            require(owner.m_WalletDB->get_CommitmentSafe(event.m_Commitment, id), "missing input commitment");
            owner.ProcessRecoveryEvent(h, event);
        }
        for (const auto& input : shielded) {
            const auto coin = owner.m_WalletDB->getShieldedCoin(input.m_Key);
            require(bool(coin), "missing shielded input");
            proto::Event::Shielded event;
            event.m_CoinID = coin->m_CoinID; event.m_TxoID = coin->m_TxoID; event.m_Flags = 0;
            owner.ProcessRecoveryEvent(h, event);
        }
    }
};
}

namespace {
#include "offline_context_network_fixture.h"

class OfflineSignerFixture {
public:
    Session session;
    io::Reactor::Ptr retainedReactor;
    Rules::Scope rulesScope;
    io::Reactor::Scope reactorScope;
    std::shared_ptr<WalletDB> db;
    std::filesystem::path path;
    std::string contextId;
    OfflineSignerFixture(const std::filesystem::path& directory, const Rules& rules)
        : session(1, directory.string(), false, 0), retainedReactor(session.reactor_),
          rulesScope((session.rules_ = rules, session.rules_)), reactorScope(*session.reactor_),
          path(directory / "wallet.db") {
        reopen();
        contextId = session.offlineContext_->state().contextId;
    }
    OfflineSignerFixture(const std::filesystem::path& directory, const Rules& rules,
        NodeProcessor& node, const Funding& funds, Inputs inputs)
        : session(1, directory.string(), false, 0), retainedReactor(session.reactor_),
          rulesScope((session.rules_ = rules, session.rules_)),
          reactorScope(*session.reactor_), path(directory / "wallet.db") {
        std::filesystem::create_directory(directory);
        db = createDb(path, kSenderSeed);
        session.database_ = db;
        storage::setVar(*db, "beam.sdk.kmp.restore.v1", 0); // real production recovery policy remains enabled
        auto tip = node.m_Cursor.m_Full;
        HeightHash id; tip.get_ID(id);
        db->get_History().AddStates(&tip, 1); db->setSystemStateID(id);
        db->set_ShieldedOuts(node.m_Extra.m_ShieldedOutputs);
        if (inputs != Inputs::Shielded) db->saveCoin(funds.ordinary);
        if (inputs != Inputs::Ordinary) db->saveShieldedCoin(funds.shielded);
        RecognitionRecovery r;
        r.m_Phase = RecognitionRecovery::Complete; r.m_Generation = 1;
        r.m_Cursor = r.m_Target = id;
        storage::setBlobVar(*db, RecognitionRecovery::Key, r);
        db->FlushNow();
        session.offlineContext_ = std::make_shared<beam::sdk::OfflineContext>(db, 1, [](const auto&) {});
        {
            Wallet owner(db);
            require(owner.RecoveryPolicyEnabled(), "fixture omitted production recovery policy");
            // Fixture imports a completed prior sync; no transaction exists yet. The signer
            // itself must keep recovery enabled and pass the production membership capability.
            db->SetSelectionAllowed(true);
            ContextNetwork network(owner, node);
            session.offlineContext_->prepare(network); network.finish();
        }
        require(session.offlineContext_->state().phase == beam::sdk::OfflineContext::Phase::Ready,
            "prior-sync durable context failed");
        contextId = session.offlineContext_->state().contextId;
    }
    ~OfflineSignerFixture() { session.close(); db.reset(); }
    Json quote(const std::string& receiver, Amount amount, bool maximum = false) {
        return Json::parse(session.quoteSend(receiver, amount, maximum, "", contextId));
    }
    Json sign(const std::string& operation, const std::string& receiver, const Json& q, bool maximum = false) {
        return Json::parse(session.signOffline(operation, receiver, q.at("amount").get<Amount>(),
            maximum, "", contextId, q.at("version").get<std::string>()));
    }
    std::string selfReceiver(bool maxPrivacy) {
        WalletAddress address;
        db->createAddress(address);
        address.m_Token = maxPrivacy ? GenerateMaxPrivacyToken(address, *db, 0, 0, "") :
            GenerateOfflineToken(address, *db, 0, 0, "", 1);
        db->saveAddress(address); db->FlushNow();
        require(!address.m_Token.empty(), "missing self receiver");
        return address.m_Token;
    }
    std::vector<ShieldedCoin> pendingOutputs(const TxID& id) {
        std::vector<ShieldedCoin> outputs;
        db->visitShieldedCoins([&](const ShieldedCoin& c) {
            if (c.m_createTxId == id) {
                require(c.m_TxoID == ShieldedCoin::kTxoInvalidID && c.m_confirmHeight == MaxHeight,
                    "fixture did not exercise a pending self output");
                outputs.push_back(c);
            }
            return true;
        });
        return outputs;
    }
    SendRecord record(const std::string& operation) {
        SendRecord r; require(loadSendRecord(*db, operation, r), "missing durable operation"); return r;
    }
    bool reserved(const TxID& id) {
        bool found = false;
        db->visitCoins([&](const Coin& c) { found |= c.m_spentTxId == id; return true; });
        db->visitShieldedCoins([&](const ShieldedCoin& c) { found |= c.m_spentTxId == id; return true; });
        return found;
    }
    void requireReleased(const TxID& id) {
        db->visitCoins([&](const Coin& c) {
            require(c.m_spentTxId != id && c.m_createTxId != id, "offline transaction held ordinary coin state");
            return true;
        });
        db->visitShieldedCoins([&](const ShieldedCoin& c) {
            require(c.m_spentTxId != id && c.m_createTxId != id, "offline transaction held shielded coin state");
            return true;
        });
    }
    Amount spendable() {
        Amount total = 0;
        db->visitCoins([&](const Coin& c) {
            if (c.m_confirmHeight != MaxHeight && c.m_spentHeight == MaxHeight && !c.m_spentTxId) total += c.m_ID.m_Value;
            return true;
        });
        db->visitShieldedCoins([&](const ShieldedCoin& c) {
            if (c.m_confirmHeight != MaxHeight && c.m_spentHeight == MaxHeight && !c.m_spentTxId) total += c.m_CoinID.m_Value;
            return true;
        });
        return total;
    }
    void syncTo(const Block::SystemState::Full& tip) {
        HeightHash id; tip.get_ID(id);
        db->get_History().AddStates(&tip, 1); db->setSystemStateID(id);
    }
    Json operation(const std::string& operationId) {
        for (const auto& item : Json::parse(session.sendOperations()))
            if (item.at("operationId") == operationId) return item;
        throw std::runtime_error("missing send operation");
    }
    // What open(), start() and signOffline() run first.
    void sweep() { session.sweepOfflineLeftovers(nullptr); }
    const TransactionSnapshot* shown(const TxID& id) {
        for (const auto& item : session.snapshot_.transactions)
            if (item.transaction.m_txId == id) return &item;
        return nullptr;
    }
    void fault(const std::string& point) {
        session.sendBoundaryForTests_ = [point](const char* current) {
            if (point == current) throw std::runtime_error("injected offline durability boundary");
        };
    }
    void clearFault() { session.sendBoundaryForTests_ = {}; }
    void invalidateContext() { session.offlineContext_->invalidate(); }
    beam::sdk::SigningContext captureContext() {
        return beam::sdk::SigningContext::capture(db, *session.offlineContext_, contextId);
    }
    void cancelAfterKeykeeperDispatch() {
        session.sendBoundaryForTests_ = [&](const char* current) {
            if (std::string(current) == "offline-updated") session.cancelRequested_.store(true);
        };
    }
    void reopen() {
        // Model process loss, not orderly close: discard every write after the last FlushNow.
        // Fault tests roll back here; the child-process tests open after SIGKILL with no cleanup.
        if (db) db->RollbackNow(false);
        session.offlineContext_.reset(); session.database_.reset(); db.reset();
        db = std::dynamic_pointer_cast<WalletDB>(WalletDB::open(path.string(), SecString(std::string("synthetic-offline-probe"))));
        require(bool(db), "production operation reopen failed"); session.database_ = db;
        session.offlineContext_ = std::make_shared<beam::sdk::OfflineContext>(db, 1, [](const auto&) {});
        session.offlineContext_->load();
    }
    void killAt(const std::string& point) {
#ifndef _WIN32
        session.sendBoundaryForTests_ = [point](const char* current) {
            if (point == current) { ::kill(::getpid(), SIGKILL); ::_exit(90); }
        };
#endif
    }
    void assertNoSdkRecovery() {
        const auto before = parameters(*db);
        auto wallet = std::make_shared<Wallet>(db);
        require(wallet->RecoveryPolicyEnabled(), "SDK recovery fixture omitted policy");
        db->SetSelectionAllowed(true); // synthetic completed sync, with production policy enabled
        auto inventory = Json::parse(session.recoverSendOperationsOnWalletThread(wallet));
        require(inventory.size() == 1 && !session.startTransactionCallsForTests_ && parameters(*db) == before,
            "SDK recovery submitted or mutated offline operation");
    }
    void assertCloseDuringCallback(const std::string& operation, const std::string& receiver, const Json& q) {
        std::future<void> closing;
        session.sendBoundaryForTests_ = [&](const char* current) {
            if (std::string(current) != "offline-updated") return;
            closing = std::async(std::launch::async, [&] { session.close(); });
            while (!session.cancelRequested_.load()) std::this_thread::yield();
        };
        rejects([&] { sign(operation, receiver, q); }, "close did not interrupt pending keykeeper callback");
        closing.get();
        const auto before = parameters(*db);
        // close drained/detached every signer capability; retained fixture DB is read-only here.
        require(record(operation).state == SendState::Signing && record(operation).rawHex.empty(),
            "close returned signed bytes or lost the durable intent");
        for (unsigned i = 0; i != 3; ++i) tick();
        require(parameters(*db) == before, "close changed reservations after drain");
    }
    void assertNoResume(const SendRecord& record) {
        const auto before = parameters(*db);
        {
            Wallet owner(db);
            require(owner.RecoveryPolicyEnabled(), "reopen omitted recovery policy");
            owner.RegisterTransactionType(TxType::PushTransaction,
                std::make_shared<lelantus::PushTransaction::Creator>([&] { return db; }));
            auto network = std::make_shared<NoNetwork>(owner);
            owner.SetNodeEndpoint(network);
            owner.ResumeAllTransactions();
            owner.CancelTransaction(parseTxId(record.txId));
            owner.DeleteTransaction(parseTxId(record.txId));
            for (unsigned i = 0; i != 3; ++i) tick();
            require(!network->requests && parameters(*db) == before,
                "production reopen resumed/broadcast/released offline transaction");
            require(owner.IsOfflineTransaction(parseTxId(record.txId)), "offline exclusion missing");
        }
        db->SetSelectionAllowed(true); // return to the fixture's already imported prior-sync boundary
    }
};

// Forwards Core row changes into the session's history view, as the running client does.
struct HistoryFeed : IWalletDbObserver {
    WalletDB& db;
    Session& session;
    HistoryFeed(WalletDB& database, Session& owner) : db(database), session(owner) {
        db.Subscribe(this);
        session.onTransactions(ChangeAction::Reset, db.getTxHistory(TxType::ALL, 0, std::numeric_limits<int>::max()));
    }
    ~HistoryFeed() { db.Unsubscribe(this); }
    void onTransactionChanged(ChangeAction action, const std::vector<TxDescription>& items) override {
        session.onTransactions(action, items);
    }
};

class KernelNetwork : public proto::FlyClient::NetworkStd {
public:
    explicit KernelNetwork(Wallet& wallet) : NetworkStd(wallet) {}
    unsigned kernels = 0, other = 0;
    void PostRequestInternal(proto::FlyClient::Request& r) override {
        ++(r.get_Type() == proto::FlyClient::Request::Kernel ? kernels : other);
    }
};

proto::ProofKernel kernelProof(NodeProcessor& node, const Merkle::Hash& kernel) {
    proto::ProofKernel proof;
    NodeDB::StateID sid;
    require(node.get_ProofKernel(&proof.m_Proof.m_Inner, nullptr, sid, kernel, nullptr) != 0,
        "independent node lost the exported kernel");
    node.get_DB().get_State(sid.m_Row, proof.m_Proof.m_State);
    Merkle::ProofBuilderHard builder;
    node.m_Mmr.m_States.get_Proof(builder, node.m_Mmr.m_States.N2I(proof.m_Proof.m_State.m_Number));
    proof.m_Proof.m_Outer.swap(builder.m_Proof);
    struct HistoryProof : NodeProcessor::ProofBuilderHard {
        using NodeProcessor::ProofBuilderHard::ProofBuilderHard;
        bool get_History(Merkle::Hash&) override { return false; }
    } historyProof(node, proof.m_Proof.m_Outer);
    historyProof.GenerateProof();
    require(node.m_Cursor.m_Full.IsValidProofKernel(kernel, proof.m_Proof), "exported kernel proof invalid");
    return proof;
}

// Stop inside the real builder's output notification, before keykeeper completion.
// Flush models the DB timer committing this row while the operation is still Signing.
struct KillAfterPendingOutput : IWalletDbObserver {
    WalletDB& db;
    explicit KillAfterPendingOutput(WalletDB& value) : db(value) { db.Subscribe(this); }
    ~KillAfterPendingOutput() { db.Unsubscribe(this); }
    void onShieldedCoinsChanged(ChangeAction action, const std::vector<ShieldedCoin>& coins) override {
#ifndef _WIN32
        for (const auto& c : coins) {
            if (action != ChangeAction::Added || !c.m_createTxId || c.m_TxoID != ShieldedCoin::kTxoInvalidID)
                continue;
            SendRecord record;
            require(loadSendRecord(db, "process-kill", record) && record.state == SendState::Signing &&
                record.rawHex.empty() && parseTxId(record.txId) == *c.m_createTxId,
                "pending-output interruption missed Signing ownership");
            db.FlushNow();
            ::kill(::getpid(), SIGKILL); ::_exit(90);
        }
#endif
    }
};

void signerCases(const std::filesystem::path& dir, bool production, bool historical,
    const std::string& executable, const std::string& killPoint = {}) {
    auto reactor = io::Reactor::create(); io::Reactor::Scope reactorScope(*reactor);
    Rules rules;
    rules.m_Consensus = Rules::Consensus::FakePoW;
    rules.AllowPublicUtxos = true; rules.TreasuryChecksum = Zero; rules.Maturity.Coinbase = 10;
    for (size_t i = 1; i + 1 < std::size(rules.pForks); ++i) rules.pForks[i].m_Height = 5 + i;
    if (!production) {
        rules.Shielded.m_ProofMax = {2, 4}; rules.Shielded.m_ProofMin = {2, 2};
        rules.Shielded.MaxWindowBacklog = 32;
    } else {
        require(rules.Shielded.m_ProofMax.get_N() == 65536 && rules.Shielded.m_ProofMin.get_N() == 1024,
            "production proof configuration changed");
        // This FakePoW chain mines >131k blocks faster than wall time. Its moving median
        // advances timestamps; allow that fixture clock without changing proof/recovery rules.
        rules.DA.MaxAhead_s = 24 * 60 * 60;
    }
    rules.UpdateChecksum(); Rules::Scope scope(rules);
    auto funder = createDb(dir / "funding.db", kSenderSeed);
    auto minerDb = createDb(dir / "miner.db", 90817); auto miner = minerDb->get_MasterKdf();
    NodeProcessor node; node.m_Horizon.SetInfinite(); node.Initialize((dir / "node.db").string().c_str());
    Funding funds;
    while (node.m_Cursor.m_hh.m_Height < 29) mine(node, miner);
    mine(node, miner, fund(miner, funder->get_MasterKdf(), 1, 30, false, 0, &funds));
    const unsigned pool = production ? rules.Shielded.m_ProofMax.get_N() +
        (historical ? rules.Shielded.MaxWindowBacklog : 0) + 4 : kInitialPool;
    // Opt-in production fixtures mine genuine commitments and exact State1 into an independent
    // NodeProcessor, then complete full proofs and contextual acceptance. This deliberately
    // costs real work; zero-filled commitments/get_N request coverage are not substitutes.
    for (unsigned i = 0; i != pool; ++i) {
        mine(node, miner, fund(miner, i ? miner : funder->get_MasterKdf(), i + 2,
            i + 31, true, i, i ? nullptr : &funds));
        if (production && ((i + 1) % 4096 == 0 || i + 1 == pool))
            std::cout << "PRODUCTION_COMMITMENTS " << i + 1 << '/' << pool << std::endl;
    }
    funder.reset();
    if (!killPoint.empty()) {
        // SIGKILL also terminates this synthetic node; persist its prior-sync chain
        // before interrupting the wallet so reopen validates against the same checkpoint.
        node.CommitDB();
        OfflineSignerFixture f(dir / "killed", rules, node, funds, Inputs::Mixed);
        const bool pending = killPoint.find("pending-") == 0;
        auto receiver = pending ? Recipient{f.selfReceiver(killPoint.find("pending-max-privacy") == 0), TxAddressType::Offline} :
            chooseRecipient(dir, 1000, TxAddressType::PublicOffline);
        auto q = f.quote(receiver.token, 60'000'000);
        if (pending) {
            KillAfterPendingOutput boundary(*f.db);
            f.sign("process-kill", receiver.token, q);
            throw std::runtime_error("self output was not created before signing completed");
        }
        const bool exporting = killPoint.find("exported") != std::string::npos;
        const bool aborting = killPoint.find("abort") != std::string::npos;
        if (exporting || aborting) f.sign("process-kill", receiver.token, q);
        f.killAt(killPoint);
        if (exporting) f.session.exportSignedTransaction("process-kill");
        else if (aborting) f.session.abortPrepared("process-kill");
        else f.sign("process-kill", receiver.token, q);
        throw std::runtime_error("process-kill boundary was not reached");
    }
    unsigned scenario = 0;
    for (bool maximum : {false, true})
    for (auto inputs : {Inputs::Ordinary, Inputs::Shielded, Inputs::Mixed})
        for (auto type : {TxAddressType::Offline, TxAddressType::PublicOffline, TxAddressType::MaxPrivacy}) {
            ++scenario;
            OfflineSignerFixture f(dir / ("sdk-" + std::to_string(scenario)), rules, node, funds, inputs);
            auto receiver = chooseRecipient(dir, scenario + 1000, type); // chosen after normal sync
            auto before = parameters(*f.db); auto version = beam::sdk::walletSelectionVersion(*f.db);
            auto q = f.quote(receiver.token, inputs == Inputs::Mixed ? 60'000'000 : 20'000'000, maximum);
            require(parameters(*f.db) == before && version == beam::sdk::walletSelectionVersion(*f.db), "quote mutated wallet");
            auto signedResult = f.sign("full-" + std::to_string(scenario), receiver.token, q, maximum);
            require(signedResult.at("state") == "Signed", "SDK did not persist Signed");
            auto record = f.record("full-" + std::to_string(scenario));
            require(record.inputs.size() && record.shieldedInputs.size(), "Signed omitted atomic input inventories");
            f.requireReleased(parseTxId(record.txId));
            f.fault("offline-exported");
            rejects([&] { f.session.exportSignedTransaction(record.operationId); }, "lost export response missing");
            f.clearFault();
            require(f.record(record.operationId).state == SendState::Exported, "lost response was not an export");
            require(!f.session.abortPrepared(record.operationId), "exported inputs were canceled");
            auto bytes = f.session.exportSignedTransaction(record.operationId);
            // Foreign relay fixture receives only bytes and rules; no other wallet DB is opened.
            require(relay(node, bytes, {"synthetic-fakepow", rules.get_SignatureStr()}) == proto::TxStatus::Ok,
                "production SDK bytes failed independent contextual validation");
            if (production && inputs != Inputs::Ordinary) {
                auto decoded = decode(bytes);
                unsigned spends = 0;
                for (const auto& kernel : decoded->m_vKernels) if (kernel->get_Subtype() == TxKernel::Subtype::ShieldedInput) {
                    ++spends;
                    require(kernel->CastTo_ShieldedInput().m_SpendProof.m_Cfg.get_N() == (historical ? 1024 : 65536),
                        "full signed proof used the wrong production window");
                }
                require(spends != 0, "production proof fixture omitted shielded signing");
            }
            f.reopen(); f.assertNoResume(record); f.assertNoSdkRecovery();
            require(f.session.exportSignedTransaction(record.operationId) == bytes, "reopen re-export changed bytes");
            require(f.sign(record.operationId, receiver.token, q, maximum).at("state") == "Exported", "retry regenerated payment");
            const auto inventory = Json::parse(f.session.sendOperations());
            require(inventory.size() == 1 && inventory[0].at("deliveryMode") == "Offline", "stopped inventory lost own signed send");
            require(inventory[0].at("createdAtEpochSeconds") == f.db->getTx(parseTxId(record.txId))->m_createTime,
                "inventory lost the Core creation time");
            if (production) std::cout << "PRODUCTION_SIGNER_CASE_OK " << scenario
                << " proof=" << (historical ? 1024 : 65536) << std::endl;
        }
    if (production) return;
#ifndef _WIN32
    for (const std::string point : {"offline-intent-record", "offline-intent", "offline-core-created",
        "offline-signed-record", "offline-signed", "offline-release-record", "offline-released",
        "offline-exported-record", "offline-exported",
        "offline-abort-record", "offline-aborted", "pending-self-offline", "pending-max-privacy",
        "pending-self-offline-abort", "pending-max-privacy-abort"}) {
        const auto childDir = dir / ("process-" + point);
        std::filesystem::create_directory(childDir);
        std::string path = childDir.string();
        char* args[] = {const_cast<char*>(executable.c_str()), const_cast<char*>("--kill-boundary"),
            const_cast<char*>(point.c_str()), path.data(), nullptr};
        pid_t child;
        require(posix_spawn(&child, executable.c_str(), nullptr, nullptr, args, environ) == 0,
            "cannot spawn synthetic durability child");
        int status = 0;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
        while (waitpid(child, &status, WNOHANG) == 0) {
            if (std::chrono::steady_clock::now() >= deadline) {
                kill(child, SIGKILL); waitpid(child, &status, 0);
                throw std::runtime_error("synthetic durability child timed out");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        require(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL,
            "durability child did not die at the requested boundary");
        OfflineSignerFixture f(childDir / "killed", rules);
        SendRecord record;
        const bool present = loadSendRecord(*f.db, "process-kill", record);
        if (point == "offline-intent-record" || point == "offline-aborted") {
            require(!present && f.db->getTxHistory(TxType::ALL, 0, std::numeric_limits<int>::max()).empty(),
                "kill left partial intent/abort evidence");
            f.db->visitCoins([](const Coin& c) { require(!c.m_spentTxId, "kill left ordinary reserve"); return true; });
            f.db->visitShieldedCoins([](const ShieldedCoin& c) { require(!c.m_spentTxId, "kill left shielded reserve"); return true; });
        } else {
            const auto expected = point == "offline-exported" ? SendState::Exported :
                (point == "offline-signed" || point == "offline-release-record" || point == "offline-released" ||
                    point == "offline-exported-record" || point == "offline-abort-record") ? SendState::Signed :
                SendState::Signing;
            require(present && record.state == expected, "kill crossed a durable state fence");
            if (point.find("pending-") == 0) {
                require(expected == SendState::Signing && !f.pendingOutputs(parseTxId(record.txId)).empty(),
                    "process interruption did not persist the builder's pending self output");
                require(!f.contextId.empty(), "pending self output poisoned context on reopen");
                rejects([&] { f.session.exportSignedTransaction(record.operationId); }, "Signing exported before retry");
                if (point.find("-abort") != std::string::npos) {
                    const auto id = parseTxId(record.txId);
                    require(f.session.abortPrepared(record.operationId), "interrupted self Signing could not abort");
                    f.reopen();
                    require(f.pendingOutputs(id).empty() && !f.db->getTx(id) &&
                        Json::parse(f.session.sendOperations()).empty(), "interrupted abort left self output/intent");
                    require(!f.quote(record.receiver, record.amount).empty(), "interrupted abort/reopen poisoned context");
                    f.requireReleased(id);
                    std::cout << "OFFLINE_SIGNER_PROCESS_KILL_OK " << point << std::endl;
                    continue;
                }
            }
            if (point == "offline-intent") require(f.db->getTxHistory(TxType::ALL, 0, std::numeric_limits<int>::max()).empty(), "Core preceded intent");
            if (expected != SendState::Signing) {
                require(!record.rawHex.empty(), "durable Signed lost bytes");
                const auto id = parseTxId(record.txId);
                // Only a kill between the Signed flush and its release leaves the signing reservation.
                if (point == "offline-signed" || point == "offline-release-record") {
                    Coin ordinary = funds.ordinary;
                    require(f.db->findCoin(ordinary) && ordinary.m_spentTxId == id,
                        "Signed bytes preceded durable ordinary reserve");
                    bool reserved = false;
                    f.db->visitShieldedCoins([&](const ShieldedCoin& c) { reserved |= c.m_spentTxId == id; return true; });
                    require(reserved, "Signed bytes preceded durable shielded reserve");
                } else {
                    f.requireReleased(id);
                }
            }
            f.assertNoResume(record); f.assertNoSdkRecovery();
            Json q = {{"amount", record.amount}, {"version", record.quoteVersion}};
            if (expected == SendState::Signing && point.find("pending-") != 0) {
                // What open() runs first: the interrupted intent goes and a retry signs anew.
                const auto old = parseTxId(record.txId);
                f.session.sweepOfflineLeftoversOnOpen();
                SendRecord gone;
                require(!loadSendRecord(*f.db, record.operationId, gone) && !f.db->getTx(old),
                    "open sweep kept an interrupted signing");
                f.requireReleased(old);
                q = f.quote(record.receiver, record.amount);
                f.sign(record.operationId, record.receiver, q);
                record = f.record(record.operationId);
                require(parseTxId(record.txId) != old && !f.db->getTx(old), "retry resurrected the swept transaction");
            }
            f.sign(record.operationId, record.receiver, q);
            const auto bytes = f.session.exportSignedTransaction(record.operationId);
            require(bytes == f.session.exportSignedTransaction(record.operationId), "kill retry changed bytes");
            if (!record.rawHex.empty()) require(bytes == from_hex(record.rawHex), "kill regenerated Signed bytes");
            require(!f.session.abortPrepared(record.operationId), "kill/reopen aborted an exported operation");
            f.requireReleased(parseTxId(record.txId));
            if (point.find("pending-") == 0) {
                NodeProcessor childNode;
                childNode.m_Horizon.SetInfinite();
                childNode.Initialize((childDir / "node.db").string().c_str());
                Block::SystemState::Full savedTip;
                require(f.db->get_History().get_Tip(savedTip) && childNode.m_Cursor.m_Full == savedTip,
                    "killed fixture node lost the wallet's prior-sync checkpoint");
                require(relay(childNode, bytes, {"synthetic-fakepow", rules.get_SignatureStr()}) == proto::TxStatus::Ok,
                    "pending-output retry failed independent contextual validation");
                // The chain recreates a self output once it lands; until then it is not a coin.
                f.reopen();
                require(f.pendingOutputs(parseTxId(record.txId)).empty() &&
                    f.session.exportSignedTransaction(record.operationId) == bytes &&
                    !f.session.abortPrepared(record.operationId), "pending-output export/reopen lost ownership");
            }
        }
        std::cout << "OFFLINE_SIGNER_PROCESS_KILL_OK " << point << std::endl;
    }
#endif
    for (bool maxPrivacy : {false, true})
    for (const std::string point : {"offline-abort-record", "offline-aborted"}) {
        const auto name = std::string(maxPrivacy ? "self-max-" : "self-offline-") + point;
        OfflineSignerFixture f(dir / name, rules, node, funds, Inputs::Mixed);
        const auto receiver = f.selfReceiver(maxPrivacy);
        const auto q = f.quote(receiver, 60'000'000);
        // A crash before the release, or an SDK 0.1.5 record, keeps the reservation and self output.
        f.fault("offline-release-record");
        rejects([&] { f.sign(name, receiver, q); }, "release boundary fault missing");
        f.clearFault();
        const auto id = parseTxId(f.record(name).txId);
        const auto outputs = f.pendingOutputs(id);
        require(!outputs.empty(), "self abort regression omitted pending shielded output");
        // Known self output is excluded from coverage, but partial/unknown metadata is not.
        for (unsigned fault = 0; fault != 7; ++fault) {
            auto damaged = outputs.front();
            if (fault == 0) damaged.m_createTxId.reset();
            if (fault == 1) damaged.m_createTxId = GenerateTxID();
            if (fault == 2) damaged.m_CoinID.m_User.m_pMessage[0].m_pData[0] ^= 1;
            if (fault == 3) damaged.m_confirmHeight = 0;
            if (fault == 4) damaged.m_TxoID = 0;
            if (fault == 5) damaged.m_confirmHeight = f.db->getCurrentHeight();
            if (fault == 6) { damaged.m_TxoID = 0; damaged.m_confirmHeight = f.db->getCurrentHeight(); }
            f.db->saveShieldedCoin(damaged);
            rejects([&] { f.captureContext(); },
                "unknown/partially confirmed self output bypassed coverage");
            f.db->saveShieldedCoin(outputs.front());
        }
        f.captureContext();
        f.db->FlushNow();
        f.fault(point);
        rejects([&] { f.session.abortPrepared(name); }, "self abort boundary fault missing");
        f.clearFault(); f.reopen();
        if (point == "offline-abort-record") {
            require(f.record(name).state == SendState::Signed && f.pendingOutputs(id).size() == outputs.size(),
                "abort rollback lost pending self output or Signed");
            require(f.session.abortPrepared(name), "self abort retry failed");
            f.reopen();
        }
        require(f.pendingOutputs(id).empty() && !f.db->getTx(id) && Json::parse(f.session.sendOperations()).empty(),
            "self abort/reopen left transaction-created state");
        Coin ordinary = funds.ordinary;
        require(f.db->findCoin(ordinary) && !ordinary.m_spentTxId &&
            !f.db->getShieldedCoin(funds.shielded.m_CoinID.m_Key)->m_spentTxId,
            "self abort did not restore ordinary/shielded reserves");
        f.db->visitCoins([&](const Coin& c) { require(c.m_createTxId != id, "self abort left change"); return true; });
        rejects([&] { f.session.exportSignedTransaction(name); }, "aborted self bytes escaped");
        require(!f.quote(receiver, 60'000'000).empty(), "self abort/reopen poisoned subsequent offline quote");
        std::cout << "OFFLINE_SELF_ABORT_OK " << name << std::endl;
    }
    // Every fence includes rollback before commit and lost response after commit, with reopen.
    for (const std::string point : {"offline-intent-record", "offline-intent", "offline-core-created",
        "offline-signed-record", "offline-signed", "offline-release-record", "offline-released",
        "offline-exported-record", "offline-exported"}) {
        OfflineSignerFixture f(dir / ("boundary-" + point), rules, node, funds, Inputs::Ordinary);
        auto receiver = chooseRecipient(dir, ++scenario + 1000, TxAddressType::PublicOffline);
        auto q = f.quote(receiver.token, 20'000'000);
        const bool exporting = point.find("exported") != std::string::npos;
        if (exporting) f.sign(point, receiver.token, q);
        f.fault(point);
        rejects([&] { if (exporting) f.session.exportSignedTransaction(point); else f.sign(point, receiver.token, q); },
            "durable boundary fault did not trigger");
        f.clearFault(); f.reopen();
        if (point == "offline-intent-record") {
            require(f.db->getTxHistory(TxType::ALL, 0, std::numeric_limits<int>::max()).empty(), "Core row preceded durable offline intent");
            q = f.quote(receiver.token, 20'000'000);
        }
        if (point == "offline-release-record") {
            // Exported while still holding coins: what SDK 0.1.5 left on devices.
            const auto id = parseTxId(f.record(point).txId);
            const auto bytes = f.session.exportSignedTransaction(point);
            require(f.reserved(id) && f.record(point).state == SendState::Exported,
                "release fault did not leave an exported reservation");
            f.sweep();
            require(f.record(point).state == SendState::Exported && f.session.exportSignedTransaction(point) == bytes,
                "sweep changed exported bytes");
            f.requireReleased(id);
        }
        const auto retry = f.sign(point, receiver.token, q);
        require(retry.at("state") != "Signing", "same operation did not recover after boundary");
        auto record = f.record(point); f.assertNoResume(record);
        f.requireReleased(parseTxId(record.txId));
        auto bytes = f.session.exportSignedTransaction(point);
        require(bytes == f.session.exportSignedTransaction(point), "idempotent export changed bytes");
    }
    for (const std::string point : {"offline-abort-record", "offline-aborted"}) {
        OfflineSignerFixture f(dir / point, rules, node, funds, Inputs::Mixed);
        auto receiver = chooseRecipient(dir, ++scenario + 1000, TxAddressType::PublicOffline);
        const auto q = f.quote(receiver.token, 60'000'000);
        f.sign(point, receiver.token, q); f.fault(point);
        rejects([&] { f.session.abortPrepared(point); }, "abort boundary fault missing");
        f.clearFault(); f.reopen();
        if (point == "offline-abort-record") {
            require(f.record(point).state == SendState::Signed, "abort rollback lost Signed");
            require(f.session.abortPrepared(point), "pre-export abort could not retry");
        }
        require(f.db->getTxHistory(TxType::ALL, 0, std::numeric_limits<int>::max()).empty(), "aborted Core row remained resumable");
        require(Json::parse(f.session.sendOperations()).empty(), "aborted record remained exportable");
        rejects([&] { f.session.exportSignedTransaction(point); }, "aborted bytes escaped");
    }
    {
        OfflineSignerFixture f(dir / "interrupted-sweep", rules, node, funds, Inputs::Mixed);
        auto receiver = chooseRecipient(dir, ++scenario + 1000, TxAddressType::PublicOffline);
        const auto q = f.quote(receiver.token, 60'000'000);
        f.fault("offline-core-created");
        rejects([&] { f.sign("interrupted", receiver.token, q); }, "core-created fault missing");
        f.clearFault(); f.reopen();
        const auto id = parseTxId(f.record("interrupted").txId);
        require(f.record("interrupted").state == SendState::Signing && f.db->getTx(id),
            "fixture did not leave an interrupted Signing row");
        Json next;
        try {
            next = f.sign("next", receiver.token, q);
        } catch (const beam::sdk::SendError& e) {
            require(std::string(e.code) == "STALE_QUOTE", "leftover sweep failed the next operation");
            next = f.sign("next", receiver.token, f.quote(receiver.token, 60'000'000));
        }
        SendRecord gone;
        require(!loadSendRecord(*f.db, "interrupted", gone) && !f.db->getTx(id),
            "interrupted Signing survived another operation");
        f.requireReleased(id);
        require(next.at("state") == "Signed", "operation after the sweep was not signed");
        std::cout << "OFFLINE_SIGNER_INTERRUPTED_SWEEP_OK" << std::endl;
    }
    {
        OfflineSignerFixture f(dir / "cancel", rules, node, funds, Inputs::Mixed);
        auto receiver = chooseRecipient(dir, ++scenario + 1000, TxAddressType::PublicOffline);
        auto q = f.quote(receiver.token, 60'000'000);
        f.cancelAfterKeykeeperDispatch();
        rejects([&] { f.sign("cancel", receiver.token, q); }, "delayed keykeeper cancellation was ignored");
        f.clearFault();
        auto before = parameters(*f.db);
        for (int i = 0; i != 4; ++i) tick();
        require(parameters(*f.db) == before && f.record("cancel").rawHex.empty(), "retired callback mutated or exported");
        f.reopen(); f.sign("cancel", receiver.token, q);
    }
    {
        OfflineSignerFixture f(dir / "close-callback", rules, node, funds, Inputs::Ordinary);
        auto receiver = chooseRecipient(dir, ++scenario + 1000, TxAddressType::PublicOffline);
        const auto q = f.quote(receiver.token, 20'000'000);
        f.assertCloseDuringCallback("close", receiver.token, q);
    }
    {
        OfflineSignerFixture f(dir / "dust-maturity-reservations", rules, node, funds, Inputs::Ordinary);
        auto receiver = chooseRecipient(dir, ++scenario + 1000, TxAddressType::PublicOffline);
        const auto h = f.db->getCurrentHeight();
        Coin matureLater(9'000'000); matureLater.m_confirmHeight = h; matureLater.m_maturity = h + 100;
        f.db->storeCoin(matureLater);
        TxDescription pending;
        pending.m_txId = GenerateTxID(); pending.m_sender = true;
        pending.m_txType = TxType::PushTransaction; pending.m_status = TxStatus::InProgress;
        pending.m_amount = 1; pending.m_fee = 1; f.db->saveTx(pending);
        Coin reserved(8'000'000); reserved.m_confirmHeight = 1; reserved.m_maturity = 1;
        reserved.m_spentTxId = pending.m_txId; f.db->storeCoin(reserved);
        f.db->FlushNow();
        // These extra coins cannot make a request beyond the original confirmed coin affordable.
        rejects([&] { f.quote(receiver.token, kValue + 1); }, "maturing/reserved funds were selectable");
        const auto max = f.quote(receiver.token, 0, true);
        const Amount amount = max.at("amount").get<Amount>();
        const auto noChange = f.quote(receiver.token, amount);
        const auto change = f.quote(receiver.token, amount - 1);
        require(noChange.at("change") == 0 && change.at("change").get<Amount>() > 0,
            "one-atomic change boundary lost");
        require(max.at("fee") == noChange.at("fee"), "Max changed actual Core fee");
        const auto before = f.quote(receiver.token, 1);
        Coin changed = funds.ordinary;
        require(f.db->findCoin(changed), "missing reservation mutation input");
        changed.m_spentTxId = GenerateTxID(); f.db->saveCoin(changed); f.db->FlushNow();
        rejects([&] { f.sign("reservation-stale", receiver.token, before); }, "reservation mutation admitted stale quote");
        require(f.db->getTxHistory(TxType::ALL, 0, std::numeric_limits<int>::max()).size() == 1, "reservation stale quote created a Core row");
    }
    {
        OfflineSignerFixture f(dir / "quote-boundaries", rules, node, funds, Inputs::Mixed);
        auto receiver = chooseRecipient(dir, ++scenario + 1000, TxAddressType::MaxPrivacy);
        auto q = f.quote(receiver.token, 0, true);
        auto exactQuote = f.quote(receiver.token, q.at("amount").get<Amount>());
        require(q.at("fee") == exactQuote.at("fee") && q.at("change") == exactQuote.at("change"), "Max/exact Core disagreement");
        auto stale = f.quote(receiver.token, 10'000'000);
        Coin c(1); c.m_confirmHeight = 0; c.m_maturity = 0; f.db->storeCoin(c); f.db->FlushNow();
        rejects([&] { f.sign("stale", receiver.token, stale); }, "wallet mutation did not stale quote");
        require(f.db->getTxHistory(TxType::ALL, 0, std::numeric_limits<int>::max()).empty(), "stale quote created Core row");
        rejects([&] { f.quote(receiver.token, 100'000'000); }, "insufficient exact quote passed");
        f.invalidateContext();
        // Persisted checkpoint mismatch must fail ContextUnavailable, never InsufficientFunds.
        auto id = HeightHash{Zero, 1}; f.db->setSystemStateID(id); f.db->FlushNow();
        try { f.quote(receiver.token, 100'000'000); throw std::runtime_error("stale context admitted"); }
        catch (const beam::sdk::SendError& e) { require(std::string(e.code) == "CONTEXT_UNAVAILABLE", "missing context reported as funds"); }
    }
    {
        OfflineSignerFixture f(dir / "signing-snapshot", rules, node, funds, Inputs::Shielded);
        const auto context = f.captureContext();
        BaseTxBuilder::ShieldedWindow w;
        require(BaseTxBuilder::SelectShieldedWindow(funds.shielded, f.db->get_ShieldedOuts(), w),
            "fixture coin has no builder window");
        const beam::sdk::OfflineContext::Range range{w.m_Start, w.m_Count};
        const auto items = std::min<TxoID>(w.m_Count, f.db->get_ShieldedOuts() - w.m_Start);
        // Capture owns only the builder's own deduplicated windows, never expanded lists.
        require(context.windows && context.windows->windows() == 1 && context.windows->items() == items,
            "signing capture stored more than the builder's own window");
        proto::ShieldedList served;
        require(context.windows->materialize(range, served) && encode(served) == encode(nodeRange(node, range)),
            "materialised window is not byte-identical to the independent node response");
        proto::ShieldedList rejected;
        require(!context.windows->materialize({range.first + 1, range.second}, rejected) &&
            !context.windows->materialize({range.first, range.second + 1}, rejected),
            "signing capture served an unsaved range");
        f.invalidateContext(); // reservation notifications clear the cache during signing
        proto::ShieldedList afterInvalidate;
        require(context.windows->materialize(range, afterInvalidate) && encode(afterInvalidate) == encode(served),
            "captured window did not survive cache invalidation");
        require(context.current(*f.db), "unchanged database dropped the signing fence");
        f.db->set_ShieldedOuts(f.db->get_ShieldedOuts() + 1);
        require(!context.current(*f.db), "changed shielded count did not fence the captured scope");
        f.db->set_ShieldedOuts(f.db->get_ShieldedOuts() - 1);
        require(context.current(*f.db) && context.windows->materialize(range, afterInvalidate) &&
            encode(afterInvalidate) == encode(served), "restored database lost the captured scope");
        auto receiver = chooseRecipient(dir, ++scenario + 1000, TxAddressType::PublicOffline);
        const auto q = f.quote(receiver.token, 20'000'000);
        require(f.sign("snapshot", receiver.token, q).at("state") == "Signed",
            "shielded signing failed through the bounded window path");
        std::cout << "OFFLINE_SIGNER_SNAPSHOT_OK windows=" << context.windows->windows()
            << " items=" << context.windows->items() << " bytes=" << context.windows->bytes() << std::endl;
    }
    // Reach the chain: whoever spends first wins, history follows the kernel, one kernel round per start.
    for (auto inputs : {Inputs::Ordinary, Inputs::Shielded, Inputs::Mixed}) {
        const bool mixed = inputs == Inputs::Mixed;
        const auto name = "chain-" + std::to_string(int(inputs));
        OfflineSignerFixture f(dir / name, rules, node, funds, inputs);
        auto receiver = chooseRecipient(dir, ++scenario + 1000, TxAddressType::PublicOffline);
        const Amount amount = mixed ? 60'000'000 : 20'000'000;
        const auto before = f.spendable();
        const auto q = f.quote(receiver.token, amount);
        f.sign("first", receiver.token, q);
        const auto first = parseTxId(f.record("first").txId);
        const auto firstBytes = f.session.exportSignedTransaction("first");
        f.requireReleased(first);
        const auto again = f.quote(receiver.token, amount);
        require(f.spendable() == before && again.at("fee") == q.at("fee") && again.at("change") == q.at("change") &&
            again.at("ordinaryInputs") == q.at("ordinaryInputs") && again.at("shieldedInputs") == q.at("shieldedInputs"),
            "exported send kept its coins from selection");
        std::string winner = "first";
        if (mixed) {
            // Exported bytes hold nothing: a rival over the same coins is admitted and reaches the chain first.
            require(f.sign("rival", receiver.token, again).at("state") == "Signed", "exported send deferred another offline send");
            winner = "rival";
        }
        const auto record = f.record(winner);
        const auto id = parseTxId(record.txId);
        const auto bytes = f.session.exportSignedTransaction(winner);
        std::vector<CoinID> ordinaryInputs;
        std::vector<IPrivateKeyKeeper2::ShieldedInput> shieldedInputs;
        require(fromByteBuffer(record.inputs, ordinaryInputs) && fromByteBuffer(record.shieldedInputs, shieldedInputs) &&
            ordinaryInputs.empty() == (inputs == Inputs::Shielded) && shieldedInputs.empty() == (inputs == Inputs::Ordinary),
            "fixture did not spend the requested inputs");
        const auto oldTip = node.m_Cursor.m_Full;
        const auto decoded = decode(bytes);
        const auto kernel = mainKernel(*decoded).get_ID();
        const unsigned unobserved = mixed ? 2 : 1;
        {
            HistoryFeed feed(*f.db, f.session);
            require(!f.shown(id) && !f.shown(first), "unobserved offline send appeared in history");
            {
                // A start whose round runs before mining sees nothing and asks once, whatever the tips.
                Wallet owner(f.db);
                auto network = std::make_shared<KernelNetwork>(owner); owner.SetNodeEndpoint(network);
                SnapshotReorgRecoveryTestAccess::admit(owner);
                owner.RequestOfflineKernelProofs(); owner.RequestOfflineKernelProofs();
                require(network->kernels == unobserved && !network->other, "offline kernel check was not one round per start");
            }
            mine(node, miner, decode(bytes));
            if (mixed) require(relay(node, firstBytes, {"synthetic-fakepow", rules.get_SignatureStr()}) != proto::TxStatus::Ok,
                "node accepted the losing double spend");
            const auto h = node.m_Cursor.m_hh.m_Height;
            const auto proof = kernelProof(node, kernel);
            Wallet owner(f.db);
            auto network = std::make_shared<KernelNetwork>(owner); owner.SetNodeEndpoint(network);
            owner.RegisterTransactionType(TxType::PushTransaction,
                std::make_shared<lelantus::PushTransaction::Creator>([&] { return f.db; }));
            SnapshotReorgRecoveryTestAccess::admit(owner);
            owner.RequestOfflineKernelProofs();
            require(network->kernels == unobserved, "the next start did not ask about unobserved rows");
            f.syncTo(node.m_Cursor.m_Full);
            const auto status = f.db->getTx(id)->m_status;
            // Both orders of arrival: the kernel proof before or after body recognition spends the inputs.
            if (inputs == Inputs::Shielded) SnapshotReorgRecoveryTestAccess::spend(owner, h, ordinaryInputs, shieldedInputs);
            SnapshotReorgRecoveryTestAccess::observe(owner, id, kernel, proof);
            if (inputs != Inputs::Shielded) SnapshotReorgRecoveryTestAccess::spend(owner, h, ordinaryInputs, shieldedInputs);
            SnapshotReorgRecoveryTestAccess::receive(owner, h, *decoded);
            Height observed = 0; storage::getTxParameter(*f.db, id, TxParameterID::KernelProofHeight, observed);
            require(observed == h && f.db->getTx(id)->m_status == status, "kernel observation rewrote the Core row");
            const auto* shown = f.shown(id);
            require(shown && shown->transaction.m_status == TxStatus::Completed && shown->proofHeight == h,
                "observed offline send did not appear as Completed");
            require(f.operation(winner).at("observedProofHeight") == h, "spent inputs did not confirm the observed kernel");
            require(f.spendable() == before - amount - again.at("fee").get<Amount>(),
                "balance after the chain is not the pre-sign balance minus amount and fee");
            {
                HistoryFeed reset(*f.db, f.session);
                require(f.shown(id) && f.shown(id)->transaction.m_status == TxStatus::Completed, "Reset lost the observed send");
            }
            if (mixed) {
                require(!f.shown(first) && f.operation("first").at("observedProofHeight") == 0 &&
                    f.record("first").state == SendState::Exported, "the losing double spend looked confirmed");
                {
                    Wallet restarted(f.db);
                    auto next = std::make_shared<KernelNetwork>(restarted); restarted.SetNodeEndpoint(next);
                    SnapshotReorgRecoveryTestAccess::admit(restarted);
                    restarted.RequestOfflineKernelProofs();
                    require(next->kernels == 1, "a later start asked about the observed row again");
                }
                node.ManualRollbackTo(oldTip.m_Number);
                SnapshotReorgRecoveryTestAccess::fork(owner, oldTip);
                storage::getTxParameter(*f.db, id, TxParameterID::KernelProofHeight, observed);
                require(!observed && f.db->getTx(id)->m_status == status && !network->other,
                    "offline reorg rewrote history or registered transaction");
                require(!f.shown(id) && f.operation(winner).at("observedProofHeight") == 0,
                    "reorged offline send stayed in history or confirmed");
                f.requireReleased(id);
            }
        }
        if (mixed) {
            // Re-mined on the new branch, it reappears after the next start.
            mine(node, miner);
            mine(node, miner, decode(bytes));
            const auto proof = kernelProof(node, kernel);
            HistoryFeed feed(*f.db, f.session);
            Wallet owner(f.db);
            auto network = std::make_shared<KernelNetwork>(owner); owner.SetNodeEndpoint(network);
            SnapshotReorgRecoveryTestAccess::admit(owner);
            owner.RequestOfflineKernelProofs();
            require(network->kernels == 2, "the next start skipped the reorged row");
            f.syncTo(node.m_Cursor.m_Full);
            SnapshotReorgRecoveryTestAccess::observe(owner, id, kernel, proof);
            require(f.shown(id) && f.shown(id)->proofHeight == node.m_Cursor.m_hh.m_Height, "re-mined offline send stayed hidden");
        }
        node.ManualRollbackTo(oldTip.m_Number); // later fixtures start from the shared prior-sync chain
        f.reopen();
        require(f.session.exportSignedTransaction(winner) == bytes && !f.session.abortPrepared(winner) &&
            f.record(winner).state == SendState::Exported, "chain/reopen changed exported bytes or state");
        std::cout << "OFFLINE_SIGNER_CHAIN_OK " << name << std::endl;
        if (mixed) std::cout << "OFFLINE_SIGNER_ORIGINAL_KERNEL_REORG_OK" << std::endl;
    }
}
} // namespace

int main(int argc, char** argv) {
    try {
        if (argc == 4 && std::string(argv[1]) == "--kill-boundary") {
            signerCases(argv[3], false, false, argv[0], argv[2]);
            return 3;
        }
        const bool production = argc == 2;
        const bool historical = production && std::string(argv[1]) == "--production-small-proofs";
        if (production && !historical && std::string(argv[1]) != "--production-proofs") return 2;
        const auto dir = std::filesystem::temp_directory_path() / ("beam-sdk-offline-signer-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directory(dir);
        signerCases(dir, production, historical, std::filesystem::absolute(argv[0]).string());
        std::filesystem::remove_all(dir);
        std::cout << "OFFLINE_SIGNER_OK\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "OFFLINE_SIGNER_FAILED: " << e.what() << '\n'; return 1;
    }
}
