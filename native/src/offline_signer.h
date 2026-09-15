#pragma once
#include "offline_context.h"
#include "stateless_codec.h"
#include "wallet/core/common_utils.h"
#include <atomic>

namespace beam::sdk {
class SendError final : public std::runtime_error {
public:
    const char* code;
    SendError(const char* code, const char* message) : std::runtime_error(message), code(code) {}
};

// Owner-thread only. No receiver-specific material is required during prior normal sync.
struct SendQuote {
    wallet::CoinsSelectionInfo selection;
    wallet::TxAddressType receiverType;
    std::string version, contextId, rules;
    Height height = 0;
    Amount remainder = 0;
    std::vector<wallet::Coin> ordinary;
    std::vector<wallet::ShieldedCoin> shielded;
};
SendQuote quoteSend(const std::shared_ptr<wallet::WalletDB>&, const std::string& receiver,
    Amount amount, bool maximum, const std::string& comment, const std::string& contextId);
std::string walletSelectionVersion(const wallet::WalletDB&);

struct SigningContext {
    Block::SystemState::Full tip;
    TxoID count = 0;
    wallet::RecognitionRecovery recovery;
    std::string id, rules;
    // Owned deduplicated coverage, not expanded proof windows: overlapping windows share
    // their commitments here and a range is materialised only when Core actually asks.
    std::shared_ptr<const OfflineContext::Snapshot> windows;
    static SigningContext capture(const std::shared_ptr<wallet::WalletDB>&, OfflineContext&,
        const std::string& expectedId);
    bool current(wallet::WalletDB&) const;
};
struct SignedMaterial {
    ByteBuffer bytes, inputs, shieldedInputs;
    InspectedTransaction inspection;
};
// No bytes leave this function except through persist, which MUST atomically flush Signed
// plus input inventories before returning. It never uses Wallet::StartTransaction.
void signOffline(const std::shared_ptr<wallet::WalletDB>&, const wallet::TxParameters&,
    const SigningContext&, Amount totalFee, Amount explicitFee, Amount amount,
    std::atomic<bool>& cancelled, const std::function<void(const SignedMaterial&)>& persist,
    const std::function<void(const char*)>& boundary = {});
} // namespace beam::sdk
