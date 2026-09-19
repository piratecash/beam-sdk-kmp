#pragma once

#include "wallet/core/wallet.h"
#include "wallet/core/base_tx_builder.h"
#include <map>
#include <atomic>
#include <deque>

namespace beam::sdk {

// All methods except destruction after reactor join run on the WalletDB owner thread.
// A context authorizes local proof lookup only. It never authorizes a transaction or resume.
class OfflineContext final : public proto::FlyClient::Request::IHandler, private wallet::IWalletDbObserver {
public:
    enum class Phase { Unavailable, Preparing, Ready, Invalidated };
    struct State {
        Phase phase = Phase::Unavailable;
        std::string contextId;
        Height height = 0;
        TxoID shieldedCount = 0;
        // Diagnostics only: why this state was reported and how far preparation got.
        std::string reason;
        bool boundaryDone = false, awaitingSecondPeer = false;
        size_t downloadsDone = 0, downloadsTotal = 0, proofsDone = 0, proofsTotal = 0;
        bool operator==(const State&) const;
    };
    using Range = std::pair<TxoID, uint32_t>;
    using Changed = std::function<void(const State&)>;
    static constexpr size_t MaxBytes = 64 * 1024 * 1024;
    static constexpr size_t MaxWindows = 4096;

    OfflineContext(std::shared_ptr<wallet::WalletDB>, int network, Changed);
    ~OfflineContext();
    void load();
    void prepare(proto::FlyClient::INetwork&);
    void invalidate(std::string reason = "invalidated");
    void note(std::string reason); // a fact about the current phase, e.g. a skipped prepare
    void requestStop() { stopRequested_.store(true); } // thread-safe dispatch fence, before join
    void resume() { stopRequested_.store(false); } // owner acquired, before restarting reactor
    void stop();
    void close(); // detach requests before network/DB destruction, after reactor drain
    const State& state() const { return state_; }

    // Future isolated signer must additionally enforce recovery policy, object membership,
    // epoch and durable offline intent. This lookup cannot grant those capabilities.
    // Integration contract for the next stage: drain the online owner, revalidate the saved
    // recovery generation and selection constraints, then register only the explicitly created
    // signer object in a scoped offline membership set. Every continuation must retain 0013's
    // epoch/retirement fences. Normal startup must exclude durable offline intents before any
    // creator runs. Until that mode and journal exclusions exist, Ready is observation only.
    bool lookup(const std::string& contextId, Range, proto::ShieldedList&) const;
    // Owned immutable coverage for one signing scope. Overlapping windows share the
    // deduplicated commitments and each window keeps its exact response-end State1;
    // item lists are never stored, a requested range is materialised one window at a time.
    class Snapshot final {
    public:
        bool materialize(Range, proto::ShieldedList&) const;
        size_t windows() const { return windows_.size(); }
        size_t items() const { return commitments_.size(); }
        size_t bytes() const {
            return items() * (sizeof(TxoID) + sizeof(ECC::Point::Storage)) +
                windows() * (sizeof(Range) + sizeof(ECC::Hash::Value));
        }
    private:
        friend class OfflineContext;
        friend struct OfflineContextTestAccess;
        TxoID count_ = 0;
        std::map<Range, ECC::Hash::Value> windows_;
        std::map<TxoID, ECC::Point::Storage> commitments_;
    };
    // Same fences as lookup, over exactly the requested saved windows. The result is owned by
    // the caller: reload/invalidate of this cache can never change or revoke it.
    std::shared_ptr<const Snapshot> snapshot(const std::string& contextId, std::vector<Range>) const;
    static std::vector<Range> coverage(const std::vector<wallet::ShieldedCoin>&, TxoID);

private:
    friend struct OfflineContextTestAccess;
    friend struct SigningContext;
    struct Window {
        Range range;
        // State1 is the exact response-end accumulator, NOT a Merkle root. Commitments are
        // shared in Record; this checkpoint is never reused for a different response end.
        proto::ShieldedList response;
        template<class A> void serialize(A& a) { a & range & response.m_State1; }
    };
    struct OwnProof {
        TxoID id = 0;
        ECC::Point serial;
        proto::ProofShieldedOutp response;
        template<class A> void serialize(A& a) { a & id & serial & response; }
    };
    struct Record {
        uint32_t version = 1;
        int network = -1;
        std::string rules;
        uint64_t generation = 0, recoveryGeneration = 0;
        Block::SystemState::Full checkpoint{};
        TxoID count = 0;
        std::vector<Window> windows;
        std::map<TxoID, ECC::Point::Storage> commitments;
        std::vector<OwnProof> proofs;
        template<class A> void serialize(A& a) {
            a & version & network & rules & generation & recoveryGeneration & checkpoint
                & count & windows & commitments & proofs;
        }
    };
    using Request = proto::FlyClient::Request;
    std::shared_ptr<wallet::WalletDB> db_;
    int network_;
    Changed changed_;
    State state_;
    Record saved_, pending_;
    std::vector<Range> downloads_;
    std::map<TxoID, ECC::Hash::Value> endpoints_;
    proto::FlyClient::INetwork* transport_ = nullptr;
    Request::Ptr request_;
    ByteBuffer candidate_;
    io::Address firstPeer_;
    uint64_t epoch_ = 0, requestEpoch_ = 0, nextGeneration_ = 0;
    size_t windowIndex_ = 0, proofIndex_ = 0, pendingBytes_ = 0;
    bool boundaryDone_ = false, closed_ = false;
    std::atomic<bool> stopRequested_{false};

    void OnComplete(Request&) override;
    void onShieldedCoinsChanged(wallet::ChangeAction, const std::vector<wallet::ShieldedCoin>&) override {
        invalidate("shielded-coins-changed");
    }
    void onSystemStateChanged(const HeightHash&) override;
    void next();
    void cancel();
    void publish();
    void report(Phase, std::string reason);
    void notify(std::string reason);
    bool matchesDatabase(const Record&) const;
    bool complete(const Record&, bool currentCoins = true) const;
    bool reusable() const;
    void planDownloads();
    std::vector<wallet::ShieldedCoin> coins() const;
    static ECC::Point serial(const wallet::ShieldedCoin&, Key::IPKdf&);
    static std::string identity(const Record&);
    static bool validProof(const Record&, const OwnProof&);
    static bool build(const Record&, const std::vector<Range>& sorted, Snapshot&);
};

// Bounded, sequenced log of reported states for diagnostics. Not synchronised: the owner serialises
// access. A state equal to the latest entry is not repeated.
class OfflineContextEvents final {
public:
    static constexpr size_t Capacity = 64;
    struct Event { uint64_t seq; OfflineContext::State state; };
    void add(const OfflineContext::State&);
    const std::deque<Event>& events() const { return events_; }
private:
    uint64_t seq_ = 0;
    std::deque<Event> events_;
};
} // namespace beam::sdk
