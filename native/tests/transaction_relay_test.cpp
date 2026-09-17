// Synthetic signing/funding helpers only. No remote node, real wallet or funded network.
// Compile separately with transaction_relay.cpp + stateless_codec.cpp and TESTS defined.
#define main offlineSigningFixtureMain
#include "offline_signing_test.cpp"
#undef main
#include "transaction_relay.h"
#include <fstream>
#include <future>
#include <thread>

namespace {
using beam::sdk::TransactionRelay;
using beam::sdk::RelayOutcome;
using beam::sdk::RelayResult;

enum class WireMode { Accept, Reject, Drop, HoldLogin, CancelBeforeLogin, LateReply, CancelAtReceive };

struct WireNode : proto::NodeConnection::Server {
    struct Peer final : proto::NodeConnection {
        WireNode& owner;
        io::Timer::Ptr late;
        explicit Peer(WireNode& node) : owner(node) {}
        void SetupLogin(proto::Login& message) override {
            if (owner.spreading) message.m_Flags |= proto::LoginFlags::SpreadingTransactions;
            else message.m_Flags &= ~proto::LoginFlags::SpreadingTransactions;
        }
        void OnConnectedSecure() override {
            if (owner.mode == WireMode::HoldLogin) return;
            if (owner.mode == WireMode::CancelBeforeLogin) { owner.onReceive(); return; }
            SendLogin();
            ECC::Scalar::Native identity;
            identity = 421U; // Synthetic node authentication, never a wallet key.
            ProveID(identity, proto::IDType::Node);
            // Exercise relay isolation from unsolicited headers; no chain sync/history
            // is required to register a transaction through this connection.
            proto::NewTip tip;
            tip.m_Description = owner.node.m_Cursor.m_Full;
            Send(tip);
        }
        void OnMsg(proto::NewTransaction&& message) override {
            ++owner.received;
            owner.bytes.push_back(encode(*message.m_Transaction));
            require(!message.m_Context && message.m_Fluff, "unexpected relay envelope");
            Transaction::Context context;
            context.m_Height.m_Min = owner.node.m_Cursor.m_hh.m_Height + 1;
            uint32_t charge = 0;
            const auto status = message.m_Transaction->IsValid(context)
                ? owner.node.ValidateTxContextEx(*message.m_Transaction, context.m_Height,
                    false, charge, nullptr, nullptr, nullptr)
                : proto::TxStatus::Invalid;
            owner.contextStatus = status;
            if (owner.mode == WireMode::Drop) { Reset(); return; }
            if (owner.mode == WireMode::CancelAtReceive) owner.onReceive();
            const auto replyStatus = owner.mode == WireMode::Reject ? proto::TxStatus::LowFee : status;
            if (owner.mode == WireMode::LateReply || owner.mode == WireMode::CancelAtReceive) {
                late = io::Timer::create(io::Reactor::get_Current());
                late->start(owner.mode == WireMode::LateReply ? 1500 : 100, false, [this, replyStatus] {
                    ++owner.lateCallbacks;
                    proto::Status response;
                    response.m_Value = replyStatus;
                    Send(response); // May already be closed; must not resurrect the relay.
                });
            } else {
                proto::Status response;
                response.m_Value = replyStatus;
                Send(response);
            }
        }
        void OnMsg(proto::GetShieldedList&& message) override {
            ++owner.lists;
            Send(nodeRange(owner.node, {message.m_Id0, message.m_Count}));
        }
        void OnDisconnect(const DisconnectReason&) override { Reset(); }
    };

    NodeProcessor& node;
    WireMode mode;
    std::vector<std::unique_ptr<Peer>> peers;
    std::vector<ByteBuffer> bytes;
    std::function<void()> onReceive;
    unsigned acceptedConnections = 0, received = 0, lateCallbacks = 0;
    unsigned lists = 0;
    bool spreading = true;
    uint8_t contextStatus = proto::TxStatus::Unspecified;
    uint16_t port = 0;
    WireNode(NodeProcessor& processor, WireMode behavior) : node(processor), mode(behavior) {
        // Reserve a loopback listener before advertising its address; avoid fixed-port tests.
        for (unsigned i = 0; i < 128; ++i) {
            const auto candidate = static_cast<uint16_t>(32000 +
                (std::chrono::steady_clock::now().time_since_epoch().count() + i) % 20000);
            try { Listen(io::Address::localhost().port(candidate)); port = candidate; break; }
            catch (const std::exception&) {}
        }
        require(port != 0, "cannot bind synthetic loopback node");
    }
    std::string endpoint() const { return "127.0.0.1:" + std::to_string(port); }
    void OnAccepted(io::TcpStream::Ptr&& stream, int error) override {
        if (error || !stream) return;
        ++acceptedConnections;
        auto peer = std::make_unique<Peer>(*this);
        auto* value = peer.get();
        peers.push_back(std::move(peer));
        value->Accept(std::move(stream));
        value->SecureConnect();
    }
    ~WireNode() { m_pServer.reset(); peers.clear(); }
};


// The recovery-quorum switch, on the only harness that can reach the park: TestNetwork in
// snapshot_reorg_recovery_test.cpp overrides PostRequestInternal, so requests there never reach
// AssignRequests and never see the peer-count gate at all.
void recoveryQuorumSwitchCases(NodeProcessor& node) {
    using Fly = proto::FlyClient;
    struct Client : Fly {
        Block::SystemState::HistoryMap history;
        Block::SystemState::IHistory& get_History() override { return history; }
    } client;
    client.history.AddStates(&node.m_Cursor.m_Full, 1);
    struct Handler : Fly::Request::IHandler {
        unsigned completed = 0;
        void OnComplete(Fly::Request& r) override { ++completed; r.m_pTrg = nullptr; }
    } contexts;

    WireNode only(node, WireMode::Accept);
    only.spreading = false;
    Fly::NetworkStd network(client);
    network.m_Cfg.m_vNodes = {io::Address::localhost().port(only.port)};
    auto ready = [](const auto& c) {
        return c.IsLive() && c.IsSecureOut() && c.IsAtTip() && (c.m_Flags & Fly::NetworkStd::Connection::Flags::Node);
    };
    // An offline-context request. IsRecoveryRequest accepts it through the m_OfflineContext
    // FLAG, which is how OfflineContext drives its own two-peer replay - not how the wallet's
    // scan traffic is classified. The type-classified shape is exercised separately below.
    auto recovery = [&](Fly::RequestShieldedList::Ptr& request) {
        request = new Fly::RequestShieldedList;
        request->m_OfflineContext = true;
        request->m_Msg.m_Id0 = 0; request->m_Msg.m_Count = 1;
    };
    // The wallet's own recovery traffic. Wallet::PostReq sets no flag at all, so a body pack is a
    // recovery request purely by get_Type(); testing only the flag above would leave the type
    // disjunction in IsRecoveryRequest unprotected, and deleting BodyPack from it would silently
    // stop gating every body request the scan posts.
    auto bodyPack = [&](Fly::RequestBodyPack::Ptr& request) {
        request = new Fly::RequestBodyPack;
        // Shaped like Wallet::RequestBodies builds it, so the gate sees the real thing.
        node.m_Cursor.m_Full.get_ID(request->m_Msg.m_Top);
        request->m_Msg.m_FlagP = proto::BodyBuffers::Recovery1;
        request->m_Msg.m_FlagE = proto::BodyBuffers::Full;
        request->m_Msg.m_CountExtra.v = 1;
    };

    network.Connect();
    pump([&] { return ready(network.m_Connections.front()); });

    network.SetRecoveryQuorum(false);

    // ---- quorum OFF, but the request excludes the only peer. The exclusion is a property of the
    // REQUEST - OfflineContext sets it when replaying the second half, and handing the same peer
    // the replay could only invalidate the context - so it must be honoured whatever the policy
    // says. It must also NOT be reported: ReportBodyQuorumFailure is terminal, and an exclusion
    // bounce is normal failover, not a stall.
    unsigned bounceReports = 0;
    network.m_RecoveryParkReport = [&](const char*) { ++bounceReports; };
    Fly::NetworkStd::SetRecoveryParkReportThresholdForTests(0);
    Fly::RequestShieldedList::Ptr contextual;
    recovery(contextual);
    contextual->m_RecoveryExcludedAddress = io::Address::localhost().port(only.port);
    network.PostRequest(*contextual, contexts);
    for (unsigned i = 0; i != 5; ++i) tick();
    network.OnNewRequests();
    require(network.m_lst.size() == 1 && only.lists == 0,
        "the quorum switch disabled the request-level peer exclusion");
    require(bounceReports == 0,
        "an exclusion bounce was reported as a quorum stall, which fails the whole session");
    Fly::NetworkStd::SetRecoveryParkReportThresholdForTests(120000);
    network.m_RecoveryParkReport = nullptr;
    contextual->m_pTrg = nullptr;
    network.m_lst.Clear();

    // ---- quorum ON with the same single peer: parked, and reported once it stays parked.
    // Nothing has been served yet on purpose: with one peer neither a body pack under the armed
    // quorum nor an offline-context request in any configuration can be assigned, so both
    // counters are still zero and the drain phase below is what proves service resumes.
    unsigned reports = 0;
    network.m_RecoveryParkReport = [&](const char*) { ++reports; };
    network.SetRecoveryQuorum(true);
    Fly::RequestShieldedList::Ptr parked;
    recovery(parked);
    network.PostRequest(*parked, contexts);
    for (unsigned i = 0; i != 5; ++i) tick();
    require(contexts.completed == 0 && only.lists == 0 && network.m_lst.size() == 1,
        "quorum on served a recovery request from a single peer");
    require(reports == 0, "a fresh park reported before the threshold elapsed");

    // The threshold is process-global storage; lower it, re-drive the gate, restore it.
    Fly::NetworkStd::SetRecoveryParkReportThresholdForTests(0);
    network.OnNewRequests();
    require(reports == 1, "a park past the threshold went unreported");
    network.OnNewRequests();
    network.OnNewRequests();
    require(reports == 1, "the park report is not latched and fired repeatedly");

    // ---- a second eligible peer drains it, and that clears the streak. The switch goes back OFF
    // here on purpose: an offline-context request is gated by its own flag either way, so the
    // drain still happens, and it lets the responder assertion below run in the shipped
    // configuration rather than the armed one.
    network.SetRecoveryQuorum(false);
    WireNode alternate(node, WireMode::Accept);
    alternate.spreading = false;
    auto* other = new Fly::NetworkStd::Connection(network);
    other->m_Addr = io::Address::localhost().port(alternate.port);
    other->Connect(other->m_Addr);
    pump([&] { return ready(*other) && contexts.completed == 1; });
    require(network.m_lst.empty() && reports == 1, "quorum drain reported a stall");
    // The responder must be recorded even with the quorum off. OfflineContext requires it
    // non-empty on every response and compares its two replay halves by it, so gating the
    // recording on the quorum - as this once did - invalidates every offline context the moment
    // the switch ships off.
    require(parked->m_RecoveryResponder != io::Address(),
        "a served recovery request carried no responder, which invalidates every offline context");

    // Re-armed for the reset phase: with the quorum off a request carrying no exclusion is simply
    // served, so there would be nothing to park and nothing to report.
    network.SetRecoveryQuorum(true);

    // Streak reset. Asserting "no new report" here would be vacuous: the latch is already set
    // from the report above, so it holds whether or not the assignment cleared anything. The
    // reset is only observable as a SECOND report after parking again.
    other->Reset();
    delete other;
    Fly::RequestShieldedList::Ptr again;
    recovery(again);
    network.PostRequest(*again, contexts);
    for (unsigned i = 0; i != 5; ++i) tick();
    require(network.m_lst.size() == 1 && contexts.completed == 1, "single peer served a recovery request");
    network.OnNewRequests();
    require(reports == 2, "a successful assignment did not clear the latch, so the next stall was silent");
    network.OnNewRequests();
    require(reports == 2, "the second park report is not latched either");
    Fly::NetworkStd::SetRecoveryParkReportThresholdForTests(120000);

    // ---- the wallet's own recovery traffic, classified purely by get_Type(). Deliberately last:
    // the harness node answers GetShieldedList and nothing else, so an assigned body pack stays
    // in flight forever and eventually costs the connection - harmless here, fatal to any phase
    // that ran afterwards. Parking is decided before anything is sent, so the distinction shows
    // as the request leaving (assigned) or staying in (parked) the network-wide list.
    network.m_lst.Clear();
    network.SetRecoveryQuorum(true);
    Fly::RequestBodyPack::Ptr heldBack;
    bodyPack(heldBack);
    network.PostRequest(*heldBack, contexts);
    require(network.m_lst.size() == 1, "quorum on served a body pack from a single peer");
    heldBack->m_pTrg = nullptr;
    network.m_lst.Clear();

    network.SetRecoveryQuorum(false);
    Fly::RequestBodyPack::Ptr assignedPack;
    bodyPack(assignedPack);
    network.PostRequest(*assignedPack, contexts);
    require(network.m_lst.empty(), "quorum off parked a body pack behind a single peer");
    assignedPack->m_pTrg = nullptr;

    network.m_RecoveryParkReport = nullptr;
    network.Disconnect();
    std::cout << "RECOVERY_QUORUM_SWITCH_OK off_serves on_parks reports_once latched resets\n";
}

// Real secure loopback peers and NetworkStd dispatch. No scheduler/request overrides.
void schedulerCases(NodeProcessor& node, const ByteBuffer& bytes) {
    using Fly = proto::FlyClient;
    struct Client : Fly {
        Block::SystemState::HistoryMap history;
        Block::SystemState::IHistory& get_History() override { return history; }
    } client;
    client.history.AddStates(&node.m_Cursor.m_Full, 1);
    struct Handler : Fly::Request::IHandler {
        unsigned completed = 0;
        void OnComplete(Fly::Request& r) override { ++completed; r.m_pTrg = nullptr; }
    } transactions, contexts, ordinary;
    WireNode first(node, WireMode::Accept), alternate(node, WireMode::Accept);
    first.spreading = alternate.spreading = false;
    Fly::NetworkStd network(client);
    network.SetRecoveryQuorum(true);
    network.m_Cfg.m_vNodes = {io::Address::localhost().port(first.port)};
    network.Connect();
    auto ready = [](const auto& c) {
        return c.IsLive() && c.IsSecureOut() && c.IsAtTip() && (c.m_Flags & Fly::NetworkStd::Connection::Flags::Node);
    };
    pump([&] { return ready(network.m_Connections.front()); });
    Fly::RequestTransaction::Ptr tx = new Fly::RequestTransaction;
    tx->m_Msg.m_Transaction = decode(bytes); tx->m_Msg.m_Fluff = true;
    network.PostRequest(*tx, transactions); // Previously recursed indefinitely here.
    require(network.m_lst.size() == 1 && !first.received, "unsupported peer consumed registration");
    Fly::RequestShieldedList::Ptr context = new Fly::RequestShieldedList;
    context->m_OfflineContext = true;
    context->m_Msg.m_Id0 = 0; context->m_Msg.m_Count = 1;
    context->m_RecoveryExcludedAddress = io::Address::localhost().port(first.port);
    network.PostRequest(*context, contexts);
    Fly::RequestEnsureSync::Ptr sync = new Fly::RequestEnsureSync;
    sync->m_IsDependent = false;
    network.PostRequest(*sync, ordinary);
    require(ordinary.completed == 1 && network.m_lst.size() == 2,
        "unsupported/quorum-blocked requests starved ordinary work");
    for (unsigned i = 0; i != 5; ++i) tick();
    require(!first.received && !first.lists && !contexts.completed, "single peer bypassed context quorum");

    auto* other = new Fly::NetworkStd::Connection(network);
    other->m_Addr = io::Address::localhost().port(alternate.port);
    other->Connect(other->m_Addr);
    pump([&] { return ready(*other) && contexts.completed == 1; });
    require(first.lists == 0 && alternate.lists == 1 && network.m_lst.size() == 1,
        "excluded context peer or unsupported registration blocked eligible context");
    require(context->m_RecoveryResponder == other->m_Addr && context->m_ResponseCheckpoint == node.m_Cursor.m_Full,
        "actual scheduler lost context responder/checkpoint");
    alternate.spreading = true;
    alternate.peers.front()->SendLogin(); // Actual post-sync capability change.
    pump([&] { return transactions.completed == 1; });
    require(!first.received && alternate.received == 1 && network.m_lst.empty(),
        "capability change did not drain registration to eligible alternate");

    // Fix the priority order, then queue context behind ordinary work in one batch.
    auto& preferred = *std::find_if(network.m_Connections.begin(), network.m_Connections.end(),
        [&](const auto& c) { return c.m_Addr.port() == first.port; });
    network.m_Connections.erase(Fly::NetworkStd::ConnectionList::s_iterator_to(preferred));
    network.m_Connections.push_front(preferred);
    auto enqueue = [&](Fly::Request& r, Handler& h) {
        r.m_pTrg = &h; network.m_lst.Create_back()->m_pRequest = &r;
    };
    enqueue(*tx, transactions);
    enqueue(*context, contexts);
    enqueue(*sync, ordinary);
    network.OnNewRequests();
    pump([&] { return transactions.completed == 2 && contexts.completed == 2; });
    require(ordinary.completed == 2 && !first.received && !first.lists && alternate.received == 2 &&
        alternate.lists == 2 && network.m_lst.empty(), "eligible alternate/context exclusion batch starved or duplicated");
    first.spreading = true;
    first.peers.front()->SendLogin();
    pump([&] { return (preferred.m_LoginFlags & proto::LoginFlags::SpreadingTransactions) != 0; });
    enqueue(*tx, transactions);
    enqueue(*context, contexts);
    network.OnNewRequests();
    pump([&] { return transactions.completed == 3 && contexts.completed == 3; });
    require(first.received == 1 && !first.lists && alternate.received == 2 && alternate.lists == 3,
        "context behind accepted ordinary work reached its excluded peer");
    const auto address = other->m_Addr;
    other->m_Addr = preferred.m_Addr; // Two connections to one endpoint do not form a quorum.
    enqueue(*context, contexts);
    enqueue(*sync, ordinary);
    network.OnNewRequests();
    require(contexts.completed == 3 && ordinary.completed == 3 && network.m_lst.size() == 1,
        "duplicate endpoint formed quorum or starved ordinary work");
    other->m_Addr = address;
    other->AssignRequests();
    pump([&] { return contexts.completed == 4; });
    require(first.lists == 0 && alternate.lists == 4 && network.m_lst.empty(), "restored quorum failed to drain context");
    network.Disconnect();
    std::cout << "CORE_SCHEDULER_ELIGIBILITY_OK\n";
}

RelayResult attempt(const ByteBuffer& bytes, const Rules& rules, WireNode& node,
    uint32_t timeout = 3000, bool cancelBefore = false) {
    auto operation = std::make_unique<TransactionRelay>(bytes, rules, rules.get_SignatureStr(),
        std::vector<std::string>{node.endpoint()});
    require(operation->inspected().mainKernelId == mainKernel(*decode(bytes)).get_ID().str(),
        "relay selected the first shielded kernel instead of main standard kernel");
    node.onReceive = [&] { operation->cancel(); };
    if (cancelBefore) operation->cancel();
    std::packaged_task<RelayResult()> task([&] { return operation->run(timeout); });
    auto future = task.get_future();
    std::thread worker(std::move(task));
    try {
        pump([&] { return future.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready; });
    } catch (...) {
        operation->cancel();
        worker.join();
        throw;
    }
    worker.join();
    const auto result = future.get();
    operation.reset(); // Every subsequent node callback runs after the relay handle is gone.
    node.onReceive = [] { throw std::runtime_error("send after relay drain"); };
    return result;
}

void runLateCallbacks(WireNode& node) {
    pump([&] { return node.lateCallbacks == 1; });
    for (unsigned i = 0; i < 5; ++i) tick();
    require(node.received == 1, "late callback caused a second send");
}

void relayFixture(const std::filesystem::path& dir) {
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
    auto funding = createDb(dir / "funding.db", kSenderSeed);
    auto minerDb = createDb(dir / "miner.db", 90817);
    auto miner = minerDb->get_MasterKdf();
    NodeProcessor node;
    node.m_Horizon.SetInfinite();
    node.Initialize((dir / "node.db").string().c_str());
    Funding funds;
    while (node.m_Cursor.m_hh.m_Height < 29) mine(node, miner);
    mine(node, miner, fund(miner, funding->get_MasterKdf(), 1, 30, false, 0, &funds));
    for (unsigned i = 0; i < kInitialPool; ++i)
        mine(node, miner, fund(miner, i ? miner : funding->get_MasterKdf(), 2 + i, 31 + i,
            true, i, i ? nullptr : &funds));
    const auto cache = priorSync(node);
    funding.reset();
    auto recipient = chooseRecipient(dir, 1, TxAddressType::MaxPrivacy);
    Captured captured, corrupt;
    {
        Session signer(dir / "signer.db", funds, cache, Inputs::Mixed, Fault::None);
        signer.prepare(recipient, 60'000'000, 1);
        captured = successful(signer, Inputs::Mixed, recipient.type, Fault::None);
    }
    {
        Session signer(dir / "corrupt.db", funds, cache, Inputs::Shielded, Fault::State1);
        signer.prepare(recipient, 20'000'000, 2);
        corrupt = successful(signer, Inputs::Shielded, recipient.type, Fault::State1);
    }
    schedulerCases(node, captured.bytes);
    recoveryQuorumSwitchCases(node);
    auto directoryEntries = [&] {
        std::map<std::filesystem::path, ByteBuffer> entries;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(dir)) {
            if (!entry.is_regular_file()) continue;
            std::ifstream file(entry.path(), std::ios::binary);
            require(file.good(), "cannot inspect synthetic fixture files");
            entries.emplace(entry.path(), ByteBuffer(std::istreambuf_iterator<char>(file), {}));
        }
        return entries;
    };
    const auto before = directoryEntries();
    {
        WireNode server(node, WireMode::Accept);
        for (unsigned i = 0; i < 2; ++i)
            require(attempt(captured.bytes, rules, server).outcome == RelayOutcome::Accepted, "wire acceptance failed");
        require(server.received == 2 && server.bytes[0] == captured.bytes && server.bytes[1] == captured.bytes,
            "retry changed canonical full bytes");
        require(server.contextStatus == proto::TxStatus::Ok, "node contextual validation failed");
    }
    {
        WireNode server(node, WireMode::Accept);
        const auto result = attempt(corrupt.bytes, rules, server);
        require(result.outcome == RelayOutcome::Rejected && result.nodeStatus == proto::TxStatus::InvalidInput,
            "node did not reject corrupt shielded proof over wire");
    }
    {
        WireNode server(node, WireMode::Reject);
        const auto result = attempt(captured.bytes, rules, server);
        require(result.outcome == RelayOutcome::Rejected && result.nodeStatus == proto::TxStatus::LowFee,
            "typed node rejection lost");
    }
    {
        WireNode server(node, WireMode::Drop);
        require(attempt(captured.bytes, rules, server).outcome == RelayOutcome::UnknownAcceptance,
            "post-send disconnect claimed unsent");
        require(server.received == 1, "post-send disconnect fixture never received bytes");
        server.mode = WireMode::Accept;
        require(attempt(captured.bytes, rules, server).outcome == RelayOutcome::Accepted,
            "identical retry after unknown acceptance failed");
        require(server.bytes.size() == 2 && server.bytes[0] == server.bytes[1],
            "ambiguous retry changed bytes");
    }
    {
        WireNode server(node, WireMode::HoldLogin);
        require(attempt(captured.bytes, rules, server, 100).outcome == RelayOutcome::TimeoutBeforeSend,
            "pre-send timeout misclassified");
        require(server.received == 0, "sent before authenticated login");
    }
    {
        WireNode server(node, WireMode::CancelBeforeLogin);
        require(attempt(captured.bytes, rules, server).outcome == RelayOutcome::CancelledBeforeSend,
            "connected pre-send cancel misclassified");
        require(server.acceptedConnections == 1 && server.received == 0, "cancel before login sent bytes");
    }
    {
        WireNode server(node, WireMode::LateReply);
        require(attempt(captured.bytes, rules, server, 1000).outcome == RelayOutcome::TimeoutUnknownAcceptance,
            "post-send timeout claimed unsent");
        require(server.received == 1, "timeout fixture did not reach send boundary");
        runLateCallbacks(server);
    }
    {
        WireNode server(node, WireMode::CancelAtReceive);
        require(attempt(captured.bytes, rules, server).outcome == RelayOutcome::CancelledUnknownAcceptance,
            "post-send cancel claimed unsent");
        runLateCallbacks(server);
    }
    {
        WireNode server(node, WireMode::Accept);
        require(attempt(captured.bytes, rules, server, 1000, true).outcome == RelayOutcome::CancelledBeforeSend,
            "pre-cancelled relay ran");
        require(server.acceptedConnections == 0, "pre-cancelled relay opened connection");
        rejects([&] { TransactionRelay bad(captured.bytes, rules, "wrong", {server.endpoint()}); }, "rules mismatch admitted");
        rejects([&] { TransactionRelay bad(ByteBuffer(1'048'577), rules, rules.get_SignatureStr(),
            {server.endpoint()}); }, "oversized bytes admitted");
        auto trailing = captured.bytes;
        trailing.push_back(0);
        rejects([&] { TransactionRelay bad(trailing, rules, rules.get_SignatureStr(), {server.endpoint()}); },
            "trailing bytes admitted");
        for (unsigned i = 0; i < 5; ++i) tick();
        require(server.acceptedConnections == 0, "invalid bytes/rules reached network");
    }
    {
        WireNode server(node, WireMode::Accept);
        server.m_pServer.reset(); // Known closed loopback endpoint; real connect refusal.
        require(attempt(captured.bytes, rules, server).outcome == RelayOutcome::NetworkUnavailable,
            "pre-send unavailable misclassified");
        require(server.received == 0, "unavailable fixture received transaction");
    }
    require(directoryEntries() == before, "foreign relay created or mutated wallet/history/storage files");
    std::cout << "TRANSACTION_RELAY_WIRE_OK accepted contextual_reject identical_retries unavailable unknown timeout cancel late_drain no_wallet\n";
}
}

int main() {
    const auto dir = std::filesystem::temp_directory_path() / ("beam-relay-fixture-" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    try {
        require(std::filesystem::create_directory(dir), "fixture directory collision");
        relayFixture(dir);
        std::filesystem::remove_all(dir);
        return 0;
    } catch (const std::exception& failure) {
        std::filesystem::remove_all(dir);
        std::cerr << "TRANSACTION_RELAY_WIRE_FAILED: " << failure.what() << '\n';
        return 1;
    }
}
