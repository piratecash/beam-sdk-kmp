#pragma once

#include "stateless_codec.h"
#include <atomic>
#include <memory>

namespace beam::sdk {
enum class RelayOutcome {
    Accepted = 0, NetworkUnavailable = 1, UnknownAcceptance = 2,
    TimeoutBeforeSend = 3, TimeoutUnknownAcceptance = 4,
    CancelledBeforeSend = 5, CancelledUnknownAcceptance = 6,
    Rejected = 256 // Add the uint8_t node status for the JNI representation.
};

struct RelayResult {
    RelayOutcome outcome;
    uint8_t nodeStatus = 0;
    int encoded() const { return static_cast<int>(outcome) + nodeStatus; }
};

// One operation, no worker thread or Java callbacks. Constructor validates before DNS.
// Caller owns the blocking run and MUST join it before destruction. cancel is thread-safe.
class TransactionRelay {
public:
    TransactionRelay(const ByteBuffer&, int network, const std::string& expectedRules);
    ~TransactionRelay();
    TransactionRelay(const TransactionRelay&) = delete;
    TransactionRelay& operator=(const TransactionRelay&) = delete;
    RelayResult run(uint32_t timeoutMillis);
    void cancel() noexcept { cancelled_.store(true); }
    const InspectedTransaction& inspected() const { return inspected_; }

#ifdef BEAM_SDK_KMP_TESTS
    // Synthetic rules and loopback endpoints only; absent from the shipped API/JNI.
    TransactionRelay(const ByteBuffer&, const Rules&, const std::string& expectedRules,
        const std::vector<std::string>& loopbackPeers);
#endif

private:
    Rules rules_;
    InspectedTransaction inspected_;
    std::vector<std::string> peers_;
    std::atomic<bool> cancelled_{false};
    std::atomic<bool> started_{false};
};
} // namespace beam::sdk
