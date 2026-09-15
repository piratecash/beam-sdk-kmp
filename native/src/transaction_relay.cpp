#include "transaction_relay.h"
#include "core/fly_client.h"
#include "wallet/core/default_peers.h"
#include "utility/io/timer.h"
#include <uv.h>
#include <chrono>
#include <stdexcept>

namespace beam::sdk {
namespace {
using Clock = std::chrono::steady_clock;
using Fly = proto::FlyClient;

// Core's general FlyClient scheduler requires chain history even for submission.
// The relay uses its Connection/RequestTransaction serializer and secure transport,
// with isolated login/status callbacks and no scheduler/header synchronization.
struct RelayClient final : Fly {
    Block::SystemState::IHistory& get_History() override {
        throw std::logic_error("History is unavailable in transaction relay");
    }
};

class Runner final : public Fly::Request::IHandler {
    struct Connection final : Fly::NetworkStd::Connection {
        Runner& owner;
        bool login = false, submitted = false, failed = false;
        Connection(Runner& value) : Fly::NetworkStd::Connection(value.network_), owner(value) {}
        void SetupLogin(proto::Login&) override {} // No peers, BBS, mining or owner requests.
        void OnLogin(proto::Login&&, uint32_t) override { login = true; maybeSend(); }
        void OnMsg(proto::Authentication&& msg) override {
            Fly::NetworkStd::Connection::OnMsg(std::move(msg));
            maybeSend();
        }
        void OnMsg(proto::NewTip&&) override {} // Node performs contextual tx validation.
        void OnMsg(proto::PbftStamp&&) override {}
        void OnMsg(proto::Status&& msg) override {
            if (owner.done_ || !submitted) return;
            if (owner.stopping()) return;
            if (m_lst.empty()) ThrowUnexpected();
            owner.request_->m_Res = std::move(msg);
            // Finish directly: Core's generic OnDone consults history and may retry.
            m_lst.Finish(m_lst.front());
        }
        void OnDisconnect(const DisconnectReason&) override {
            if (failed) return;
            failed = true;
            ResetAll(); // Cancels Core pending connect/async failure callbacks; no reconnect.
            ++owner.failed_;
            owner.checkUnavailable();
        }
        void maybeSend() {
            if (failed || submitted || owner.done_ || owner.sent_ || !login ||
                !IsLive() || !IsSecureOut() || !(m_Flags & Flags::Node)) return;
            if (owner.stopping()) return;
            if (!IsSupported(*owner.request_)) return;
            auto& entry = *m_lst.Create_back();
            entry.m_pRequest = owner.request_;
            submitted = true;
            // Conservative boundary before Core serialization/write: an exception or
            // disconnect beyond here can NEVER be reported as definitely unsent.
            owner.sent_ = true;
            SendRequest(*owner.request_);
        }
    };

    struct Lookup {
        uv_getaddrinfo_t request{};
        Runner* owner;
        std::string host, port;
        bool pending = false;
    };

    const std::atomic<bool>& cancelled_;
    Clock::time_point deadline_;
    io::Reactor::Ptr reactor_ = io::Reactor::create();
    RelayClient client_;
    Fly::NetworkStd network_{client_};
    Fly::RequestTransaction::Ptr request_{new Fly::RequestTransaction};
    io::Timer::Ptr timer_;
    std::vector<std::unique_ptr<Lookup>> lookups_;
    size_t pending_ = 0, connections_ = 0, failed_ = 0;
    bool done_ = false, sent_ = false;
    RelayResult result_{RelayOutcome::NetworkUnavailable};

    void finish(RelayResult value) {
        if (done_) return;
        done_ = true;
        result_ = value;
        reactor_->stop(); // Cleanup occurs after the active Core callback unwinds.
    }
    bool stopping() {
        if (done_) return true;
        if (cancelled_.load()) {
            finish({sent_ ? RelayOutcome::CancelledUnknownAcceptance : RelayOutcome::CancelledBeforeSend});
        } else if (Clock::now() >= deadline_) {
            finish({sent_ ? RelayOutcome::TimeoutUnknownAcceptance : RelayOutcome::TimeoutBeforeSend});
        }
        return done_;
    }
    void checkUnavailable() {
        if (stopping()) return;
        if (sent_ && failed_) {
            // A losing parallel connection may fail while the submitting one is live.
            for (auto& connection : network_.m_Connections) {
                const auto& relay = static_cast<const Connection&>(connection);
                if (relay.submitted && !relay.failed) return;
            }
            finish({RelayOutcome::UnknownAcceptance});
        } else if (!pending_ && failed_ == connections_) {
            finish({RelayOutcome::NetworkUnavailable});
        }
    }
    void connect(const io::Address& address) {
        if (stopping()) return;
        auto* connection = new Connection(*this); // Core intrusive list owns deletion.
        ++connections_;
        connection->m_Addr = address;
        try { connection->Connect(address); }
        catch (...) {
            connection->failed = true;
            connection->ResetAll();
            ++failed_;
            checkUnavailable();
        }
    }
    static void resolved(uv_getaddrinfo_t* request, int status, addrinfo* addresses) noexcept {
        auto& lookup = *static_cast<Lookup*>(request->data);
        auto& self = *lookup.owner;
        lookup.pending = false;
        --self.pending_;
        try {
            if (!self.done_ && !status && addresses && addresses->ai_family == AF_INET) {
                const auto* address = reinterpret_cast<const sockaddr_in*>(addresses->ai_addr);
                self.connect(io::Address(*address));
            }
            self.checkUnavailable();
        } catch (...) {
            self.finish({self.sent_ ? RelayOutcome::UnknownAcceptance : RelayOutcome::NetworkUnavailable});
        }
        if (addresses) uv_freeaddrinfo(addresses);
    }
    void resolve(const std::vector<std::string>& peers) {
        // Queue all lookups before pumping callbacks; no blocking Address::resolve.
        lookups_.reserve(peers.size());
        for (const auto& peer : peers) {
            if (stopping()) return;
            auto lookup = std::make_unique<Lookup>();
            lookup->owner = this;
            const auto colon = peer.rfind(':');
            if (colon == std::string::npos) throw std::logic_error("Invalid default node endpoint");
            lookup->host = peer.substr(0, colon);
            lookup->port = peer.substr(colon + 1);
            lookup->request.data = lookup.get();
            // Establish ownership before registering an asynchronous request.
            lookups_.push_back(std::move(lookup));
            auto& owned = *lookups_.back();
            addrinfo hints{};
            hints.ai_family = AF_INET;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_protocol = IPPROTO_TCP;
            if (!uv_getaddrinfo(&reactor_->get_UvLoop(), &owned.request, resolved,
                owned.host.c_str(), owned.port.c_str(), &hints)) {
                owned.pending = true;
                ++pending_;
            }
        }
        checkUnavailable();
    }
    void drain() {
        done_ = true;
        request_->m_pTrg = nullptr;
        if (timer_) timer_->cancel();
        timer_.reset();
        network_.Disconnect(); // Deletes connections, cancels their timers and I/O.
        network_.m_lst.Clear(); // Release requests requeued by Connection destruction.
        for (const auto& lookup : lookups_)
            if (lookup->pending) uv_cancel(reinterpret_cast<uv_req_t*>(&lookup->request));
        // uv_cancel cannot interrupt an OS resolver already running. Keep every lookup
        // and this owner alive until its completion callback; never detach DNS work.
        while (pending_) uv_run(&reactor_->get_UvLoop(), UV_RUN_ONCE);
        lookups_.clear();
        uv_run(&reactor_->get_UvLoop(), UV_RUN_NOWAIT);
    }
    void OnComplete(Fly::Request& request) override {
        const auto status = request.As<Fly::RequestTransaction>().m_Res.m_Value;
        finish(status == proto::TxStatus::Ok ? RelayResult{RelayOutcome::Accepted} :
            RelayResult{RelayOutcome::Rejected, status});
    }

public:
    Runner(const InspectedTransaction& inspected, const std::atomic<bool>& cancelled, uint32_t timeout)
        : cancelled_(cancelled), deadline_(Clock::now() + std::chrono::milliseconds(timeout)) {
        request_->m_Msg.m_Transaction = inspected.transaction;
        request_->m_Msg.m_Fluff = true; // Same direct registration mode as pinned Wallet.
        request_->m_pTrg = this;
    }
    RelayResult run(const std::vector<std::string>& peers) {
        io::Reactor::Scope scope(*reactor_);
        try {
            timer_ = io::Timer::create(*reactor_);
            timer_->start(10, true, [this] { stopping(); });
            resolve(peers);
            if (!done_) reactor_->run();
        } catch (...) {
            finish({sent_ ? RelayOutcome::UnknownAcceptance : RelayOutcome::NetworkUnavailable});
        }
        drain();
        return result_;
    }
};
} // namespace

TransactionRelay::TransactionRelay(const ByteBuffer& bytes, int network, const std::string& expected)
    : rules_(supportedRules(network)), inspected_(inspectTransaction(bytes, rules_, expected)) {
    Rules::Scope scope(rules_);
    peers_ = getDefaultPeers();
}

#ifdef BEAM_SDK_KMP_TESTS
TransactionRelay::TransactionRelay(const ByteBuffer& bytes, const Rules& rules, const std::string& expected,
    const std::vector<std::string>& peers)
    : rules_(rules), inspected_(inspectTransaction(bytes, rules_, expected)), peers_(peers) {
    for (const auto& peer : peers_)
        if (peer.rfind("127.0.0.1:", 0) != 0) throw std::invalid_argument("Relay fixture requires loopback");
}
#endif

TransactionRelay::~TransactionRelay() = default;

RelayResult TransactionRelay::run(uint32_t timeoutMillis) {
    if (!timeoutMillis || timeoutMillis > 120'000) throw std::invalid_argument("Invalid relay timeout");
    if (started_.exchange(true)) throw std::logic_error("Relay operation already run");
    Rules::Scope scope(rules_);
    std::unique_ptr<Runner> runner;
    try { runner = std::make_unique<Runner>(inspected_, cancelled_, timeoutMillis); }
    catch (const std::bad_alloc&) { throw; }
    catch (...) { return {RelayOutcome::NetworkUnavailable}; } // No transport exists yet.
    return runner->run(peers_);
}
} // namespace beam::sdk
