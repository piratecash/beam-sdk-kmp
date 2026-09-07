#include <jni.h>

#include "core/block_crypt.h"
#include "wallet/client/wallet_client.h"
#include "wallet/core/base_transaction.h"
#include "wallet/core/common_utils.h"
#include "wallet/core/default_peers.h"
#include "wallet/core/simple_transaction.h"
#include "wallet/core/wallet.h"
#include "wallet/core/wallet_db.h"
#include "wallet/transactions/lelantus/push_transaction.h"

#include "3rdparty/nlohmann/json.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <future>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

thread_local const beam::Rules* beam::Rules::s_pInstance = nullptr;

namespace {

using Json = nlohmann::json;
using beam::Amount;
using beam::Asset;
using beam::Height;
using beam::Rules;
using beam::Timestamp;
using beam::TxoID;
using beam::wallet::ChangeAction;
using beam::wallet::CoinsSelectionInfo;
using beam::wallet::IWalletDB;
using beam::wallet::Notification;
using beam::wallet::TokenType;
using beam::wallet::TxAddressType;
using beam::wallet::TxDescription;
using beam::wallet::TxFailureReason;
using beam::wallet::TxID;
using beam::wallet::TxParameterID;
using beam::wallet::TxParameters;
using beam::wallet::TxStatus;
using beam::wallet::TxType;
using beam::wallet::Wallet;
using beam::wallet::WalletAddress;
using beam::wallet::WalletClient;
using beam::wallet::WalletStatus;

namespace AmountBig = beam::AmountBig;

constexpr const char* kWalletFile = "wallet.db";
constexpr const char* kNetworkVar = "pcash.beam.network.v1";
constexpr const char* kRestoreVar = "pcash.beam.restore.v1";
constexpr const char* kRestoreInitializedVar = "pcash.beam.restore.initialized.v1";
constexpr const char* kRestoreHeightVar = "pcash.beam.restore.height.v1";
constexpr const char* kRestoreDateVar = "pcash.beam.restore.date.v1";
constexpr const char* kNewWalletCreatedAtVar = "pcash.beam.new-wallet.created-at.v1";
constexpr const char* kSnapshotPathVar = "pcash.beam.restore.snapshot.path.v1";
constexpr const char* kSnapshotUrlVar = "pcash.beam.restore.snapshot.url.v1";
constexpr const char* kSnapshotHashVar = "pcash.beam.restore.snapshot.sha256.v1";
constexpr const char* kActiveSendVar = "pcash.beam.send.active.v1";
constexpr std::uint64_t kMaxJavaLong = std::numeric_limits<std::int64_t>::max();
constexpr Height kBirthdaySafetyWindow = 1'440;
constexpr Timestamp kNewWalletTipFreshnessTolerance = 10 * 60;

bool isNewWalletTipFresh(Timestamp creationTimestamp, Timestamp tipTimestamp) {
    return creationTimestamp <= tipTimestamp ||
        creationTimestamp - tipTimestamp <= kNewWalletTipFreshnessTolerance;
}

enum class Phase {
    Stopped,
    Connecting,
    ResolvingBirthday,
    DownloadingSnapshot,
    ValidatingSnapshot,
    CountingShieldedOutputs,
    ScanningWalletOutputs,
    ImportingSnapshot,
    CatchingUp,
    Syncing,
    Ready,
    Offline,
    Error,
};

enum class RestoreKind {
    Existing,
    NewWallet,
    Height,
    Date,
    Snapshot,
    Full,
};

enum class SendState {
    Prepared,
    Committing,
    Submitted,
};

struct SendRecord {
    std::string operationId;
    std::string txId;
    std::string receiver;
    std::string comment;
    std::string requestHash;
    std::uint64_t amount = 0;
    std::uint64_t fee = 0;
    std::int64_t previewVersion = 0;
    SendState state = SendState::Prepared;
};

struct SnapshotImportResult {
    bool success = false;
    std::string error;
};

struct TransactionSnapshot {
    TxDescription transaction;
    Height proofHeight = 0;
};

struct NativeSnapshot {
    Phase phase = Phase::Stopped;
    std::uint64_t currentHeight = 0;
    std::uint64_t targetHeight = 0;
    std::uint64_t available = 0;
    std::uint64_t receiving = 0;
    std::uint64_t sending = 0;
    std::uint64_t maturing = 0;
    std::uint64_t shielded = 0;
    std::uint64_t restoreCurrent = 0;
    std::uint64_t restoreTarget = 0;
    bool hasRestoreProgress = false;
    std::uint64_t syncDone = 0;
    std::uint64_t syncTotal = 0;
    bool hasSyncProgress = false;
    std::uint64_t restoreBytes = 0;
    std::uint64_t restoreTotalBytes = 0;
    bool hasRestoreBytes = false;
    std::string failureMessage;
    bool failureRetryable = true;
    std::string failureKind = "Node";
    std::vector<TransactionSnapshot> transactions;
};

class Session;

class BridgeWalletClient final : public WalletClient {
public:
    BridgeWalletClient(
        Session& owner,
        const Rules& rules,
        IWalletDB::Ptr database,
        const std::string& nodeAddress,
        beam::io::Reactor::Ptr reactor
    );

    void shutdown();

private:
    void onStatus(const WalletStatus& status) override;
    void onTxStatus(ChangeAction action, const std::vector<TxDescription>& items) override;
    void onSyncProgressUpdated(int done, int total) override;
    void onNodeConnectionChanged(bool connected) override;
    void onWalletError(beam::wallet::ErrorType error) override;
    void FailedToStartWallet() override;
    void onPostFunctionToClientContext(MessageFunction&& function) override;

    Session& owner_;
};

const char* phaseName(Phase phase) {
    switch (phase) {
        case Phase::Stopped: return "Stopped";
        case Phase::Connecting: return "Connecting";
        case Phase::ResolvingBirthday: return "ResolvingBirthday";
        case Phase::DownloadingSnapshot: return "DownloadingSnapshot";
        case Phase::ValidatingSnapshot: return "ValidatingSnapshot";
        case Phase::CountingShieldedOutputs: return "CountingShieldedOutputs";
        case Phase::ScanningWalletOutputs: return "ScanningWalletOutputs";
        case Phase::ImportingSnapshot: return "ImportingSnapshot";
        case Phase::CatchingUp: return "CatchingUp";
        case Phase::Syncing: return "Syncing";
        case Phase::Ready: return "Ready";
        case Phase::Offline: return "Offline";
        case Phase::Error: return "Error";
    }
    return "Error";
}

const char* sendStateName(SendState state) {
    switch (state) {
        case SendState::Prepared: return "Prepared";
        case SendState::Committing: return "Committing";
        case SendState::Submitted: return "Submitted";
    }
    return "Committing";
}

SendState parseSendState(const std::string& value) {
    if (value == "Prepared") return SendState::Prepared;
    if (value == "Committing") return SendState::Committing;
    if (value == "Submitted") return SendState::Submitted;
    throw std::runtime_error("Unknown durable send state");
}

std::uint64_t toAtomic(const AmountBig::Number& value) {
    auto low = AmountBig::get_Lo(value);
    if (AmountBig::get_Hi(value) != 0 || low > kMaxJavaLong) {
        throw std::overflow_error("Beam amount does not fit signed 64-bit SDK model");
    }
    return low;
}

std::string base64Url(const std::vector<std::uint8_t>& bytes) {
    static constexpr char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    std::string output;
    output.reserve((bytes.size() * 4 + 2) / 3);
    std::uint32_t accumulator = 0;
    int bits = 0;
    for (std::uint8_t value : bytes) {
        accumulator = (accumulator << 8U) | value;
        bits += 8;
        while (bits >= 6) {
            bits -= 6;
            output.push_back(alphabet[(accumulator >> bits) & 0x3fU]);
        }
    }
    if (bits > 0) {
        output.push_back(alphabet[(accumulator << (6 - bits)) & 0x3fU]);
    }
    return output;
}

class ByteWiper {
public:
    explicit ByteWiper(std::vector<std::uint8_t>& value) : value_(value) {}
    ~ByteWiper() { std::fill(value_.begin(), value_.end(), std::uint8_t{0}); }
    ByteWiper(const ByteWiper&) = delete;
    ByteWiper& operator=(const ByteWiper&) = delete;

private:
    std::vector<std::uint8_t>& value_;
};

class StringWiper {
public:
    explicit StringWiper(std::string& value) : value_(value) {}
    ~StringWiper() { std::fill(value_.begin(), value_.end(), '\0'); }
    StringWiper(const StringWiper&) = delete;
    StringWiper& operator=(const StringWiper&) = delete;

private:
    std::string& value_;
};

void ensureLogger() {
    // WalletClient always creates a LogRotation object. Beam Core requires a logger
    // singleton even when the embedding application requested no logs. Keep both
    // sinks disabled here; the KMP layer owns user-visible/redacted logging.
    static auto logger = beam::Logger::create(
        BEAM_LOG_LEVEL_WARNING,
        BEAM_LOG_LEVEL_CRITICAL,
        BEAM_LOG_SINK_DISABLED
    );
    (void) logger;
}

std::filesystem::path utf8Path(const std::string& value) {
    return std::filesystem::u8path(value);
}

std::string utf8String(const std::filesystem::path& value) {
    return value.u8string();
}

std::pair<std::int64_t, std::string> stableDigest(const std::string& value) {
    if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("Send request is too large to hash");
    }
    ECC::Hash::Value hash;
    ECC::Hash::Processor()
        << beam::Blob(value.data(), static_cast<std::uint32_t>(value.size()))
        >> hash;
    std::uint64_t version = 0;
    for (std::size_t index = 0; index < sizeof(version); ++index) {
        version = (version << 8U) | hash.m_pData[index];
    }
    return {
        static_cast<std::int64_t>(version & kMaxJavaLong),
        beam::to_hex(hash.m_pData, hash.nBytes),
    };
}

std::string sendRecordKey(const std::string& operationId) {
    return "pcash.beam.send.v1." + operationId;
}

void setRawString(IWalletDB& database, const char* key, const std::string& value) {
    database.setVarRaw(key, value.data(), value.size());
}

bool getRawString(const IWalletDB& database, const char* key, std::string& value) {
    beam::ByteBuffer bytes;
    if (!database.getBlob(key, bytes)) return false;
    value.assign(bytes.begin(), bytes.end());
    return true;
}

void flushDatabase(const IWalletDB::Ptr& database) {
    auto concrete = std::dynamic_pointer_cast<beam::wallet::WalletDB>(database);
    if (!concrete) throw std::runtime_error("Beam WalletDB implementation does not support durable flush");
    concrete->FlushNow();
}

void rollbackDatabase(const IWalletDB::Ptr& database) {
    auto concrete = std::dynamic_pointer_cast<beam::wallet::WalletDB>(database);
    if (!concrete) throw std::runtime_error("Beam WalletDB implementation does not support rollback");
    concrete->RollbackNow(false);
}

void saveSendRecord(IWalletDB& database, const SendRecord& record) {
    Json json = {
        {"operationId", record.operationId},
        {"txId", record.txId},
        {"receiver", record.receiver},
        {"comment", record.comment},
        {"requestHash", record.requestHash},
        {"amount", record.amount},
        {"fee", record.fee},
        {"previewVersion", record.previewVersion},
        {"state", sendStateName(record.state)},
    };
    auto text = json.dump();
    auto key = sendRecordKey(record.operationId);
    setRawString(database, key.c_str(), text);
}

bool loadSendRecord(const IWalletDB& database, const std::string& operationId, SendRecord& record) {
    std::string text;
    auto key = sendRecordKey(operationId);
    if (!getRawString(database, key.c_str(), text)) return false;
    auto json = Json::parse(text);
    record.operationId = json.at("operationId").get<std::string>();
    record.txId = json.at("txId").get<std::string>();
    record.receiver = json.at("receiver").get<std::string>();
    record.comment = json.at("comment").get<std::string>();
    record.requestHash = json.at("requestHash").get<std::string>();
    record.amount = json.at("amount").get<std::uint64_t>();
    record.fee = json.at("fee").get<std::uint64_t>();
    record.previewVersion = json.at("previewVersion").get<std::int64_t>();
    record.state = parseSendState(json.at("state").get<std::string>());
    return true;
}

TxID parseTxId(const std::string& value) {
    auto bytes = beam::from_hex(value);
    if (bytes.size() != TxID{}.size()) throw std::runtime_error("Invalid durable Beam TxID");
    TxID txId{};
    std::copy(bytes.begin(), bytes.end(), txId.begin());
    return txId;
}

std::string txIdString(const TxID& txId) {
    return beam::to_hex(txId.data(), txId.size());
}

TxAddressType requireOneSidedAddress(const std::string& receiver) {
    if (!beam::wallet::CheckReceiverAddress(receiver)) {
        throw std::invalid_argument("Receiver token is invalid");
    }
    auto type = beam::wallet::GetAddressType(receiver);
    if (type != TxAddressType::Offline &&
        type != TxAddressType::PublicOffline &&
        type != TxAddressType::MaxPrivacy) {
        throw std::invalid_argument("Only one-sided Beam receiver tokens are supported");
    }
    return type;
}

std::string addressTypeName(TxAddressType type) {
    switch (type) {
        case TxAddressType::Offline: return "Offline";
        case TxAddressType::PublicOffline: return "PublicOffline";
        case TxAddressType::MaxPrivacy: return "MaxPrivacy";
        default: throw std::invalid_argument("Unsupported Beam receiver type");
    }
}

TokenType tokenType(int ordinal) {
    switch (ordinal) {
        case 0: return TokenType::Offline;
        case 1: return TokenType::Public;
        case 2: return TokenType::MaxPrivacy;
        default: throw std::invalid_argument("Unknown Beam address type");
    }
}

std::string tokenTypeName(int ordinal) {
    switch (ordinal) {
        case 0: return "Offline";
        case 1: return "PublicOffline";
        case 2: return "MaxPrivacy";
        default: throw std::invalid_argument("Unknown Beam address type");
    }
}

void copyParameter(TxParameterID id, const TxParameters& source, TxParameters& target) {
    beam::ByteBuffer buffer;
    if (source.GetParameter(id, buffer)) target.SetParameter(id, buffer);
}

TxParameters makeSendParameters(const SendRecord& record) {
    auto receiverParameters = beam::wallet::ParseParameters(record.receiver);
    if (!receiverParameters) throw std::invalid_argument("Receiver token is invalid");
    auto type = requireOneSidedAddress(record.receiver);
    auto txId = parseTxId(record.txId);
    auto parameters = beam::wallet::CreateSimpleTransactionParameters(txId);
    if (!beam::wallet::LoadReceiverParams(*receiverParameters, parameters, type)) {
        throw std::invalid_argument("Receiver token does not contain required parameters");
    }
    parameters
        .SetParameter(TxParameterID::Amount, Amount(record.amount))
        .SetParameter(TxParameterID::Fee, Amount(record.fee))
        .SetParameter(TxParameterID::AssetID, Asset::s_BeamID)
        .SetParameter(
            TxParameterID::Message,
            beam::ByteBuffer(record.comment.begin(), record.comment.end())
        )
        .SetParameter(TxParameterID::OriginalToken, record.receiver);
    if (type == TxAddressType::MaxPrivacy) {
        copyParameter(TxParameterID::Voucher, *receiverParameters, parameters);
        parameters.SetParameter(TxParameterID::MaxPrivacyMinAnonimitySet, std::uint8_t(64));
    }
    return parameters;
}

std::string transactionStatus(TxStatus status) {
    switch (status) {
        case TxStatus::Pending: return "Pending";
        case TxStatus::InProgress: return "InProgress";
        case TxStatus::Registering: return "Registering";
        case TxStatus::Confirming: return "Confirming";
        case TxStatus::Completed: return "Completed";
        case TxStatus::Failed: return "Failed";
        case TxStatus::Canceled: return "Canceled";
    }
    return "Unknown";
}

bool isTerminal(TxStatus status) {
    return status == TxStatus::Completed || status == TxStatus::Failed || status == TxStatus::Canceled;
}

Json transactionJson(const TransactionSnapshot& snapshot) {
    const auto& transaction = snapshot.transaction;
    std::string kernelId;
    if (transaction.m_kernelID != beam::Zero) kernelId = std::to_string(transaction.m_kernelID);
    return {
        {"id", txIdString(transaction.m_txId)},
        {"direction", transaction.m_selfTx ? "Self" : (transaction.m_sender ? "Outgoing" : "Incoming")},
        {"amount", transaction.m_amount},
        {"fee", transaction.m_fee},
        {"createdAtEpochSeconds", transaction.m_createTime},
        {"minHeight", transaction.m_minHeight == 0 ? Json(nullptr) : Json(transaction.m_minHeight)},
        {"proofHeight", snapshot.proofHeight == 0 ? Json(nullptr) : Json(snapshot.proofHeight)},
        {"kernelId", kernelId.empty() ? Json(nullptr) : Json(kernelId)},
        {"status", transactionStatus(transaction.m_status)},
        {"failureReason", transaction.m_status == TxStatus::Failed
            ? Json(std::to_string(static_cast<std::uint32_t>(transaction.m_failureReason)))
            : Json(nullptr)},
    };
}

Json sendResolution(const char* kind, const std::string& txId = {}, const char* status = nullptr) {
    Json result = {{"kind", kind}};
    if (!txId.empty()) result["transactionId"] = txId;
    if (status != nullptr) result["status"] = status;
    return result;
}

std::int64_t daysFromCivil(int year, unsigned month, unsigned day) {
    year -= month <= 2;
    const int era = (year >= 0 ? year : year - 399) / 400;
    const unsigned yearOfEra = static_cast<unsigned>(year - era * 400);
    const unsigned dayOfYear = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
    const unsigned dayOfEra = yearOfEra * 365 + yearOfEra / 4 - yearOfEra / 100 + dayOfYear;
    return era * 146097 + static_cast<int>(dayOfEra) - 719468;
}

Timestamp parseUtcDate(const std::string& value) {
    if (value.size() != 10 || value[4] != '-' || value[7] != '-') {
        throw std::invalid_argument("Restore date must use YYYY-MM-DD");
    }
    int year = std::stoi(value.substr(0, 4));
    unsigned month = static_cast<unsigned>(std::stoi(value.substr(5, 2)));
    unsigned day = static_cast<unsigned>(std::stoi(value.substr(8, 2)));
    static constexpr unsigned daysByMonth[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year < 1970 || month < 1 || month > 12) {
        throw std::invalid_argument("Restore date is invalid");
    }
    auto daysInMonth = daysByMonth[month - 1];
    const bool leapYear = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    if (month == 2 && leapYear) ++daysInMonth;
    if (day < 1 || day > daysInMonth) {
        throw std::invalid_argument("Restore date is invalid");
    }
    auto days = daysFromCivil(year, month, day);
    if (days < 0) return 0;
    return static_cast<Timestamp>(days * 86'400);
}

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(int network, std::string storagePath)
        : network_(network),
          storagePath_(std::move(storagePath)),
          reactor_(beam::io::Reactor::create()) {
        if (network_ != 0 && network_ != 1) throw std::invalid_argument("Unknown Beam network");
        ensureLogger();
        rules_.m_Network = network_ == 0 ? Rules::Network::mainnet : Rules::Network::testnet;
        rules_.UpdateChecksum();
    }

    ~Session() {
        try {
            close();
        } catch (...) {
        }
    }

    void create(
        const std::vector<std::uint8_t>& seedBytes,
        const std::vector<std::uint8_t>& keyBytes,
        int restoreType,
        const std::string& restoreValue
    ) {
        Rules::Scope scope(rules_);
        beam::io::Reactor::Scope reactorScope(*reactor_);
        std::filesystem::create_directories(utf8Path(storagePath_));
        beam::SecString seed;
        seed.assign(reinterpret_cast<const char*>(seedBytes.data()), seedBytes.size());
        auto passwordText = base64Url(keyBytes);
        StringWiper passwordWiper(passwordText);
        beam::SecString password(passwordText);
        database_ = beam::wallet::WalletDB::init(
            databasePath(),
            password,
            seed.hash(),
            false,
            [this, restoreType, &restoreValue](IWalletDB& initializingDatabase) {
                initializingDatabase.generateAndSaveDefaultAddress();
                beam::wallet::storage::setVar(initializingDatabase, kNetworkVar, network_);
                setRestoreSource(initializingDatabase, restoreType, restoreValue);
            }
        );
        if (!database_) throw std::runtime_error("Unable to create encrypted Beam WalletDB");
    }

    void open(const std::vector<std::uint8_t>& keyBytes) {
        Rules::Scope scope(rules_);
        beam::io::Reactor::Scope reactorScope(*reactor_);
        auto passwordText = base64Url(keyBytes);
        StringWiper passwordWiper(passwordText);
        beam::SecString password(passwordText);
        database_ = beam::wallet::WalletDB::open(databasePath(), password);
        if (!database_) throw std::runtime_error("Unable to open encrypted Beam WalletDB");
        int storedNetwork = -1;
        if (!beam::wallet::storage::getVar(*database_, kNetworkVar, storedNetwork) || storedNetwork != network_) {
            database_.reset();
            throw std::runtime_error("Beam WalletDB network does not match SDK configuration");
        }
        int storedRestore = static_cast<int>(RestoreKind::Existing);
        bool storedInitialized = true;
        beam::wallet::storage::getVar(*database_, kRestoreVar, storedRestore);
        beam::wallet::storage::getVar(*database_, kRestoreInitializedVar, storedInitialized);
        if (!storedInitialized && storedRestore >= static_cast<int>(RestoreKind::NewWallet) &&
            storedRestore <= static_cast<int>(RestoreKind::Full)) {
            bootstrap_ = static_cast<RestoreKind>(storedRestore);
            bootstrapInitialized_ = false;
            if (bootstrap_ == RestoreKind::Height &&
                !beam::wallet::storage::getVar(*database_, kRestoreHeightVar, restoreHeight_)) {
                throw std::runtime_error("Beam height-restore metadata is incomplete");
            }
            if (bootstrap_ == RestoreKind::Date &&
                !beam::wallet::storage::getVar(*database_, kRestoreDateVar, restoreDate_)) {
                throw std::runtime_error("Beam date-restore metadata is incomplete");
            }
            if (bootstrap_ == RestoreKind::NewWallet &&
                !beam::wallet::storage::getVar(*database_, kNewWalletCreatedAtVar, restoreDate_)) {
                // The original creation time cannot be reconstructed. A legacy build may have
                // handed out a receive token arbitrarily long before this reopen, so using the
                // current time could permanently skip funds. Fall back to the conservative
                // genesis boundary and persist it so every later reopen makes the same choice.
                restoreDate_ = 0;
                beam::wallet::storage::setVar(*database_, kNewWalletCreatedAtVar, restoreDate_);
                flushDatabase(database_);
            }
            if (bootstrap_ == RestoreKind::Snapshot &&
                !getRawString(*database_, kSnapshotPathVar, snapshotLocation_)) {
                throw std::runtime_error("Beam snapshot-restore metadata is incomplete");
            }
            if (bootstrap_ == RestoreKind::Snapshot) {
                snapshotUrl_.clear();
                snapshotExpectedSha256_.clear();
                getRawString(*database_, kSnapshotUrlVar, snapshotUrl_);
                getRawString(*database_, kSnapshotHashVar, snapshotExpectedSha256_);
            }
            if (bootstrap_ == RestoreKind::NewWallet || bootstrap_ == RestoreKind::Height ||
                bootstrap_ == RestoreKind::Date) {
                // Older or interrupted builds may have left Core's mobile body cursor enabled
                // before this SDK had resolved the requested birthday. Let the verified header
                // chain reach the node tip first; startBodyBootstrapLocked installs the exact
                // body cursor only after the birthday and shielded boundary are known.
                beam::wallet::storage::setNeedToRequestBodies(*database_, false);
                flushDatabase(database_);
            }
        } else {
            bootstrap_ = RestoreKind::Existing;
            bootstrapInitialized_ = true;
        }
    }

    void setRestoreSource(IWalletDB& database, int type, const std::string& value) {
        switch (type) {
            case -1:
                bootstrap_ = RestoreKind::NewWallet;
                restoreDate_ = beam::getTimestamp();
                beam::wallet::storage::setVar(database, kNewWalletCreatedAtVar, restoreDate_);
                database.removeVarRaw(kRestoreHeightVar);
                database.removeVarRaw(kRestoreDateVar);
                database.removeVarRaw(kSnapshotPathVar);
                database.removeVarRaw(kSnapshotUrlVar);
                database.removeVarRaw(kSnapshotHashVar);
                break;
            case 0:
                restoreHeight_ = static_cast<Height>(std::stoull(value));
                bootstrap_ = RestoreKind::Height;
                beam::wallet::storage::setVar(database, kRestoreHeightVar, restoreHeight_);
                database.removeVarRaw(kNewWalletCreatedAtVar);
                database.removeVarRaw(kRestoreDateVar);
                database.removeVarRaw(kSnapshotPathVar);
                database.removeVarRaw(kSnapshotUrlVar);
                database.removeVarRaw(kSnapshotHashVar);
                break;
            case 1:
                restoreDate_ = parseUtcDate(value);
                bootstrap_ = RestoreKind::Date;
                beam::wallet::storage::setVar(database, kRestoreDateVar, restoreDate_);
                database.removeVarRaw(kNewWalletCreatedAtVar);
                database.removeVarRaw(kRestoreHeightVar);
                database.removeVarRaw(kSnapshotPathVar);
                database.removeVarRaw(kSnapshotUrlVar);
                database.removeVarRaw(kSnapshotHashVar);
                break;
            case 2:
            {
                const auto snapshotConfig = Json::parse(value);
                const auto pathValue = snapshotConfig.find("path");
                if (!snapshotConfig.is_object() || pathValue == snapshotConfig.end() ||
                    !pathValue->is_string()) {
                    throw std::invalid_argument("Beam snapshot restore configuration is malformed");
                }
                snapshotLocation_ = pathValue->get<std::string>();
                if (snapshotLocation_.empty()) {
                    throw std::invalid_argument("Beam snapshot restore path is empty");
                }
                const auto optionalString = [&snapshotConfig](const char* key) -> std::string {
                    const auto value = snapshotConfig.find(key);
                    if (value == snapshotConfig.end() || value->is_null()) return {};
                    if (!value->is_string()) {
                        throw std::invalid_argument(std::string("Beam snapshot ") + key + " is malformed");
                    }
                    return value->get<std::string>();
                };
                snapshotUrl_ = optionalString("snapshotUrl");
                snapshotExpectedSha256_ = optionalString("expectedSha256");
                bootstrap_ = RestoreKind::Snapshot;
                database.removeVarRaw(kNewWalletCreatedAtVar);
                setRawString(database, kSnapshotPathVar, snapshotLocation_);
                if (snapshotUrl_.empty()) database.removeVarRaw(kSnapshotUrlVar);
                else setRawString(database, kSnapshotUrlVar, snapshotUrl_);
                if (snapshotExpectedSha256_.empty()) database.removeVarRaw(kSnapshotHashVar);
                else setRawString(database, kSnapshotHashVar, snapshotExpectedSha256_);
                database.removeVarRaw(kRestoreHeightVar);
                database.removeVarRaw(kRestoreDateVar);
                break;
            }
            case 3:
                bootstrap_ = RestoreKind::Full;
                database.removeVarRaw(kNewWalletCreatedAtVar);
                database.removeVarRaw(kRestoreHeightVar);
                database.removeVarRaw(kRestoreDateVar);
                database.removeVarRaw(kSnapshotPathVar);
                database.removeVarRaw(kSnapshotUrlVar);
                database.removeVarRaw(kSnapshotHashVar);
                break;
            default: throw std::invalid_argument("Unknown restore source");
        }
        beam::wallet::storage::setVar(database, kRestoreVar, static_cast<int>(bootstrap_));
        beam::wallet::storage::setVar(database, kRestoreInitializedVar, false);
        if (bootstrap_ == RestoreKind::NewWallet || bootstrap_ == RestoreKind::Height ||
            bootstrap_ == RestoreKind::Date) {
            beam::wallet::storage::setNeedToRequestBodies(database, false);
        }
        bootstrapInitialized_ = false;
        bootstrapBodyScanPending_ = false;
        bootstrapFailed_ = false;
    }

    void start() {
        std::unique_lock<std::mutex> lock(mutex_);
        ownerThreadStopped_.wait(lock, [this]() { return !ownerThreadStopping_; });
        requireDatabase();
        if (client_) return;
        snapshot_.phase = Phase::Connecting;
        snapshot_.failureMessage.clear();
        snapshot_.failureRetryable = true;
        snapshot_.failureKind = "Node";
        snapshot_.hasSyncProgress = false;
        snapshot_.syncDone = 0;
        snapshot_.syncTotal = 0;
        initialStatusLoaded_ = false;
        initialTransactionsLoaded_ = false;
        connected_ = false;
        recoveryQuorumFailed_ = false;
        bootstrapFailed_ = false;
        cancelRequested_.store(false);
        bootstrapPending_ = !bootstrapInitialized_ && (bootstrap_ == RestoreKind::NewWallet ||
            bootstrap_ == RestoreKind::Height || bootstrap_ == RestoreKind::Date);
        if (!bootstrapInitialized_ && bootstrap_ == RestoreKind::Full) {
            beam::wallet::storage::setNextEventHeight(*database_, 0);
            beam::wallet::storage::setTreasuryHandled(*database_, false);
            beam::wallet::storage::setNeedToRequestBodies(*database_, true);
            beam::wallet::storage::setVar(*database_, kRestoreInitializedVar, true);
            flushDatabase(database_);
            bootstrapInitialized_ = true;
            snapshot_.phase = Phase::ScanningWalletOutputs;
        }
        try {
            Rules::Scope scope(rules_);
            auto peers = beam::getDefaultPeers();
            if (peers.empty()) throw std::runtime_error("Beam Core returned no default peers");
            client_ = std::make_shared<BridgeWalletClient>(*this, rules_, database_, peers.front(), reactor_);
            auto creators = std::make_shared<std::unordered_map<TxType, beam::wallet::BaseTransaction::Creator::Ptr>>();
            creators->emplace(
                TxType::PushTransaction,
                std::make_shared<beam::wallet::lelantus::PushTransaction::Creator>([database = database_]() {
                    return database;
                })
            );
            client_->start(notificationSettings(), false, creators);
            auto self = shared_from_this();
            auto configuredClient = client_;
            client_->getAsync()->makeIWTCall(
                [self, configuredClient]() -> boost::any {
                    auto network = configuredClient->getNodeNetwork();
                    auto wallet = configuredClient->getWallet();
                    if (!network || !wallet) {
                        throw std::runtime_error("Beam recovery quorum could not be initialized");
                    }
                    network->SetRecoveryQuorum(true);
                    std::weak_ptr<Session> weak = self;
                    wallet->SetBodyQuorumFailureHandler([weak](const std::string& message) {
                        if (auto session = weak.lock()) session->onQuorumError(message);
                    });
                    return boost::any(true);
                },
                [self](const boost::any&) { (void) self; }
            );
            if (!bootstrapInitialized_ && bootstrap_ == RestoreKind::Snapshot) {
                startSnapshotImportLocked();
            }
            if (bootstrapInitialized_ || bootstrap_ == RestoreKind::Existing) {
                client_->getAsync()->enableBodyRequests(true);
            }
            client_->getAsync()->getTransactions();
            client_->getAsync()->getWalletStatus();
        } catch (...) {
            auto failure = std::current_exception();
            auto failedClient = std::move(client_);
            connected_ = false;
            bootstrapStarted_ = false;
            cancelRequested_.store(true);
            lock.unlock();
            if (failedClient) {
                try {
                    failedClient->shutdown();
                } catch (...) {
                    // Preserve the original start failure.
                }
            }
            lock.lock();
            if (snapshotImportPending_ && database_) {
                try {
                    Rules::Scope scope(rules_);
                    beam::io::Reactor::Scope reactorScope(*reactor_);
                    rollbackDatabase(database_);
                    snapshotImportPending_ = false;
                } catch (...) {
                    // Preserve the original start failure. Reopening the WalletDB is still safe.
                }
            }
            std::rethrow_exception(failure);
        }
    }

    void stop() {
        std::shared_ptr<BridgeWalletClient> client;
        std::exception_ptr failure;
        cancelRequested_.store(true);
        {
            std::unique_lock<std::mutex> lock(mutex_);
            ownerThreadStopped_.wait(lock, [this]() { return !ownerThreadStopping_; });
            if (!client_) {
                connected_ = false;
                bootstrapStarted_ = false;
                snapshot_.phase = Phase::Stopped;
                return;
            }
            ownerThreadStopping_ = true;
            client = std::move(client_);
            connected_ = false;
            bootstrapStarted_ = false;
            snapshot_.phase = Phase::Stopped;
        }
        try {
            client->shutdown();
        } catch (...) {
            failure = std::current_exception();
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            try {
                if (snapshotImportPending_ && database_) {
                    Rules::Scope scope(rules_);
                    beam::io::Reactor::Scope reactorScope(*reactor_);
                    rollbackDatabase(database_);
                    snapshotImportPending_ = false;
                }
            } catch (...) {
                if (!failure) failure = std::current_exception();
            }
            ownerThreadStopping_ = false;
        }
        ownerThreadStopped_.notify_all();
        if (failure) std::rethrow_exception(failure);
    }

    void close() {
        stop();
        std::lock_guard<std::mutex> lock(mutex_);
        if (database_) {
            Rules::Scope scope(rules_);
            beam::io::Reactor::Scope reactorScope(*reactor_);
            flushDatabase(database_);
        }
        database_.reset();
        reactor_.reset();
        snapshot_ = NativeSnapshot{};
    }

    std::string snapshotJson() const {
        std::lock_guard<std::mutex> lock(mutex_);
        Json result = {
            {"phase", phaseName(snapshot_.phase)},
            {"currentHeight", snapshot_.currentHeight},
            {"targetHeight", snapshot_.targetHeight},
            {"balance", {
                {"available", snapshot_.available},
                {"receiving", snapshot_.receiving},
                {"sending", snapshot_.sending},
                {"maturing", snapshot_.maturing},
                {"shielded", snapshot_.shielded},
            }},
            {"transactions", Json::array()},
            {"failureMessage", snapshot_.failureMessage.empty() ? Json(nullptr) : Json(snapshot_.failureMessage)},
            {"failureRetryable", snapshot_.failureRetryable},
            {"failureKind", snapshot_.failureKind},
        };
        if (snapshot_.hasRestoreProgress) {
            result["restoreCurrent"] = snapshot_.restoreCurrent;
            result["restoreTarget"] = snapshot_.restoreTarget;
        }
        if (snapshot_.hasSyncProgress) {
            result["syncDone"] = snapshot_.syncDone;
            result["syncTotal"] = snapshot_.syncTotal;
        }
        if (snapshot_.hasRestoreBytes) {
            result["restoreBytes"] = snapshot_.restoreBytes;
            result["restoreTotalBytes"] = snapshot_.restoreTotalBytes;
        }
        for (const auto& transaction : snapshot_.transactions) {
            result["transactions"].push_back(transactionJson(transaction));
        }
        return result.dump();
    }

    std::string snapshotRestoreIntentJson() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (bootstrap_ != RestoreKind::Snapshot || bootstrapInitialized_) return {};
        return Json({
            {"snapshotUrl", snapshotUrl_.empty() ? Json(nullptr) : Json(snapshotUrl_)},
            {"expectedSha256", snapshotExpectedSha256_.empty()
                ? Json(nullptr)
                : Json(snapshotExpectedSha256_)},
        }).dump();
    }

    std::string pathIdentity() const {
        return utf8String(std::filesystem::weakly_canonical(
            std::filesystem::absolute(utf8Path(storagePath_))
        )) + "#" +
            std::to_string(network_);
    }

    std::string accountIdentity() const {
        requireDatabase();
        auto owner = database_->get_OwnerKdf();
        if (!owner) throw std::runtime_error("Beam WalletDB has no owner identity");
        ECC::NoLeak<ECC::HKdfPub::Packed> packedOwner;
        if (owner->ExportP(nullptr) != sizeof(packedOwner)) {
            throw std::runtime_error("Beam owner identity has an unexpected size");
        }
        owner->ExportP(&packedOwner);
        ECC::Hash::Value digest;
        ECC::Hash::Processor() << beam::Blob(&packedOwner, sizeof(packedOwner)) >> digest;
        return std::string("owner#") + std::to_string(network_) + "#" +
            beam::to_hex(digest.m_pData, digest.nBytes);
    }

    std::string receiveAddress(int ordinal) {
        return invokeOrUseStoppedDatabase<std::string>([this, ordinal]() {
            auto token = beam::wallet::GenerateTokenDefaultAddr(tokenType(ordinal), database_);
            return Json({
                {"token", token},
                {"type", tokenTypeName(ordinal)},
                {"network", network_ == 0 ? "Mainnet" : "Testnet"},
            }).dump();
        });
    }

    std::string transactions(int offset, int limit) const {
        if (offset < 0 || limit <= 0) throw std::invalid_argument("Invalid transaction page");
        std::lock_guard<std::mutex> lock(mutex_);
        Json result = Json::array();
        auto begin = std::min<std::size_t>(static_cast<std::size_t>(offset), snapshot_.transactions.size());
        auto end = std::min<std::size_t>(begin + static_cast<std::size_t>(limit), snapshot_.transactions.size());
        for (auto index = begin; index < end; ++index) {
            result.push_back(transactionJson(snapshot_.transactions[index]));
        }
        return result.dump();
    }

    std::string previewSend(const std::string& receiver, std::uint64_t amount, const std::string& comment) {
        return invoke<std::string>([this, receiver, amount, comment]() {
            auto type = requireOneSidedAddress(receiver);
            CoinsSelectionInfo selection;
            selection.m_requestedSum = amount;
            selection.m_assetID = Asset::s_BeamID;
            selection.Calculate(database_->getCurrentHeight(), database_, true);
            if (!selection.m_isEnought) throw std::runtime_error("Insufficient Beam funds including fee");
            auto fee = selection.get_TotalFee();
            if (amount > kMaxJavaLong - fee) throw std::overflow_error("Send total overflows SDK amount model");
            auto digest = requestDigest(receiver, amount, comment, fee);
            return Json({
                {"requestHash", digest.second},
                {"previewVersion", digest.first},
                {"amount", amount},
                {"fee", fee},
                {"total", amount + fee},
                {"receiverType", addressTypeName(type)},
            }).dump();
        });
    }

    std::string prepareSend(
        const std::string& operationId,
        const std::string& receiver,
        std::uint64_t amount,
        const std::string& comment,
        std::int64_t previewVersion
    ) {
        return invoke<std::string>([this, operationId, receiver, amount, comment, previewVersion]() {
            SendRecord existing;
            if (loadSendRecord(*database_, operationId, existing)) {
                auto expected = requestDigest(receiver, amount, comment, existing.fee);
                if (existing.requestHash != expected.second) {
                    throw std::logic_error("operationId is already bound to another send request");
                }
                return Json({{"transactionId", existing.txId}}).dump();
            }
            std::string activeOperation;
            if (getRawString(*database_, kActiveSendVar, activeOperation) && !activeOperation.empty()) {
                throw std::logic_error("Another Beam send operation is unresolved");
            }
            auto preview = previewMaterial(receiver, amount, comment);
            if (preview.first != previewVersion) throw std::logic_error("Send preview is stale");
            SendRecord record;
            record.operationId = operationId;
            record.txId = txIdString(beam::wallet::GenerateTxID());
            record.receiver = receiver;
            record.amount = amount;
            record.comment = comment;
            record.fee = previewFee(receiver, amount);
            record.previewVersion = previewVersion;
            record.requestHash = preview.second;
            record.state = SendState::Prepared;
            saveSendRecord(*database_, record);
            setRawString(*database_, kActiveSendVar, operationId);
            flushDatabase(database_);
            return Json({{"transactionId", record.txId}}).dump();
        });
    }

    std::string commitSend(const std::string& operationId) {
        return invoke<std::string>([this, operationId]() {
            SendRecord record;
            if (!loadSendRecord(*database_, operationId, record)) return sendResolution("NotPrepared").dump();
            if (record.state == SendState::Prepared) {
                record.state = SendState::Committing;
                saveSendRecord(*database_, record);
                flushDatabase(database_);
            }
            if (record.state == SendState::Committing) {
                auto txId = parseTxId(record.txId);
                if (!database_->getTx(txId)) {
                    auto wallet = client_->getWallet();
                    if (!wallet) throw std::runtime_error("Beam wallet engine is not available");
                    auto startedTxId = wallet->StartTransaction(makeSendParameters(record));
                    if (startedTxId != txId || !database_->getTx(txId)) {
                        throw std::runtime_error("Beam Core did not durably create the prepared transaction");
                    }
                    // Patched Core durably persists this stable TxID and immutable parameters
                    // before any reservation or node submission. Therefore a missing row means
                    // no payment side effect occurred and replay remains exactly-once.
                    flushDatabase(database_);
                }
                record.state = SendState::Submitted;
                saveSendRecord(*database_, record);
                flushDatabase(database_);
            }
            return resolveSendOnWalletThread(record).dump();
        });
    }

    std::string resolveSend(const std::string& operationId) {
        return invokeOrUseStoppedDatabase<std::string>([this, operationId]() {
            SendRecord record;
            if (!loadSendRecord(*database_, operationId, record)) return sendResolution("NotPrepared").dump();
            return resolveSendOnWalletThread(record).dump();
        });
    }

    bool abortPrepared(const std::string& operationId) {
        return invokeOrUseStoppedDatabase<bool>([this, operationId]() {
            SendRecord record;
            if (!loadSendRecord(*database_, operationId, record)) return false;
            if (record.state != SendState::Prepared) return false;
            auto key = sendRecordKey(operationId);
            database_->removeVarRaw(key.c_str());
            clearActiveSendIfMatches(operationId);
            flushDatabase(database_);
            return true;
        });
    }

#ifdef BEAM_SDK_KMP_TESTS
    std::string seedPreparedSendForTests(const std::string& operationId) {
        std::lock_guard<std::mutex> lock(mutex_);
        requireDatabase();
        if (client_) throw std::logic_error("Test send fixture requires a stopped wallet");
        Rules::Scope scope(rules_);
        beam::io::Reactor::Scope reactorScope(*reactor_);
        SendRecord record;
        record.operationId = operationId;
        record.txId = txIdString(beam::wallet::GenerateTxID());
        record.requestHash = "test-request-hash";
        record.state = SendState::Prepared;
        saveSendRecord(*database_, record);
        setRawString(*database_, kActiveSendVar, operationId);
        flushDatabase(database_);
        return record.txId;
    }

    void seedInterruptedBootstrapForTests() {
        std::lock_guard<std::mutex> lock(mutex_);
        requireDatabase();
        if (client_) throw std::logic_error("Test bootstrap fixture requires a stopped wallet");
        Rules::Scope scope(rules_);
        beam::io::Reactor::Scope reactorScope(*reactor_);
        beam::wallet::storage::setNeedToRequestBodies(*database_, true);
        beam::wallet::storage::setNextEventHeight(*database_, 3'928'609);
        database_->removeVarRaw(kNewWalletCreatedAtVar);
        flushDatabase(database_);
    }

    std::uint64_t newWalletCreatedAtForTests() {
        std::lock_guard<std::mutex> lock(mutex_);
        requireDatabase();
        if (client_) throw std::logic_error("Test bootstrap fixture requires a stopped wallet");
        Timestamp createdAt = std::numeric_limits<Timestamp>::max();
        if (!beam::wallet::storage::getVar(*database_, kNewWalletCreatedAtVar, createdAt)) {
            throw std::logic_error("New-wallet creation timestamp is missing");
        }
        return createdAt;
    }

    bool bodyRequestsPendingForTests() {
        std::lock_guard<std::mutex> lock(mutex_);
        requireDatabase();
        if (client_) throw std::logic_error("Test bootstrap fixture requires a stopped wallet");
        return beam::wallet::storage::needToRequestBodies(*database_);
    }
#endif

    void onStatus(const WalletStatus& status) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto beamStatus = status.GetBeamStatus();
        auto available = beamStatus.available;
        available += beamStatus.shielded;
        auto receiving = beamStatus.receivingIncoming;
        receiving += beamStatus.receivingChange;
        auto maturing = beamStatus.maturing;
        maturing += beamStatus.maturingMP;
        snapshot_.available = toAtomic(available);
        snapshot_.receiving = toAtomic(receiving);
        snapshot_.sending = toAtomic(beamStatus.sending);
        snapshot_.maturing = toAtomic(maturing);
        snapshot_.shielded = toAtomic(beamStatus.shielded);
        snapshot_.currentHeight = status.stateID.m_Height;
        initialStatusLoaded_ = true;
        // Sync task counts are not block heights (a body scan can report millions of units).
        // Expose the verified header tip instead of accumulating those counts into a fictitious
        // target height.
        snapshot_.targetHeight = snapshot_.currentHeight;
        if (client_) {
            if (auto wallet = client_->getWallet()) {
                snapshot_.targetHeight = std::max<std::uint64_t>(
                    snapshot_.targetHeight,
                    wallet->get_TipHeight()
                );
            }
        }
        maybeReportReadyLocked();
        maybeStartBootstrapLocked();
    }

    void onTransactions(ChangeAction action, const std::vector<TxDescription>& items) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (action == ChangeAction::Reset) {
            snapshot_.transactions.clear();
            for (const auto& item : items) {
                if (item.m_assetId == Asset::s_BeamID) {
                    snapshot_.transactions.push_back(transactionSnapshotOnOwnerThread(item));
                }
            }
            initialTransactionsLoaded_ = true;
        } else {
            for (const auto& item : items) {
                if (item.m_assetId != Asset::s_BeamID) continue;
                auto existing = std::find_if(
                    snapshot_.transactions.begin(),
                    snapshot_.transactions.end(),
                    [&item](const TransactionSnapshot& value) {
                        return value.transaction.m_txId == item.m_txId;
                    }
                );
                if (action == ChangeAction::Removed) {
                    if (existing != snapshot_.transactions.end()) snapshot_.transactions.erase(existing);
                } else if (existing == snapshot_.transactions.end()) {
                    snapshot_.transactions.push_back(transactionSnapshotOnOwnerThread(item));
                } else {
                    *existing = transactionSnapshotOnOwnerThread(item);
                }
            }
        }
        std::sort(snapshot_.transactions.begin(), snapshot_.transactions.end(), [](const auto& left, const auto& right) {
            return left.transaction.m_createTime > right.transaction.m_createTime;
        });
        maybeReportReadyLocked();
    }

    TransactionSnapshot transactionSnapshotOnOwnerThread(const TxDescription& transaction) {
        Height proofHeight = 0;
        transaction.GetParameter(TxParameterID::KernelProofHeight, proofHeight);
        if (proofHeight == 0 &&
            !transaction.m_sender &&
            transaction.m_txType == TxType::PushTransaction) {
            auto shieldedCoin = database_->getShieldedCoin(transaction.m_txId);
            if (shieldedCoin &&
                shieldedCoin->m_confirmHeight != 0 &&
                shieldedCoin->m_confirmHeight != beam::MaxHeight) {
                proofHeight = shieldedCoin->m_confirmHeight;
            }
        }
        return TransactionSnapshot{transaction, proofHeight};
    }

    void onSyncProgress(int done, int total) {
        std::lock_guard<std::mutex> lock(mutex_);
        snapshot_.hasSyncProgress = true;
        snapshot_.syncDone = static_cast<std::uint64_t>(std::max(0, done));
        snapshot_.syncTotal = static_cast<std::uint64_t>(std::max(0, total));
        if (client_) {
            if (auto wallet = client_->getWallet()) {
                snapshot_.targetHeight = std::max<std::uint64_t>(
                    snapshot_.currentHeight,
                    wallet->get_TipHeight()
                );
            }
        }
        if (recoveryQuorumFailed_ || bootstrapFailed_) {
            return;
        } else if (snapshotImportPending_) {
            snapshot_.phase = Phase::ImportingSnapshot;
        } else if (bootstrapBodyScanPending_) {
            if (done < total) {
                snapshot_.phase = isRestoreMode() ? Phase::ScanningWalletOutputs : Phase::Syncing;
            } else {
                // Body completion can be reported before WalletClient publishes the matching
                // header-sync state on its client context. Remember that the body work is done,
                // but do not expose an authoritative Ready window at the snapshot/checkpoint
                // height while a newer node tip is still being adopted.
                bootstrapBodyScanPending_ = false;
                snapshot_.phase = Phase::CatchingUp;
                maybeReportReadyLocked();
            }
        } else if (bootstrapPending_) {
            if (bootstrapStarted_) {
                snapshot_.phase = bootstrap_ == RestoreKind::Date || bootstrap_ == RestoreKind::NewWallet
                    ? Phase::ResolvingBirthday
                    : Phase::CountingShieldedOutputs;
            } else {
                // A fresh wallet must first synchronize and validate the header chain before
                // resolving its persisted creation-time birthday. Report that header work as
                // ordinary sync rather than making the UI appear stuck in birthday resolution.
                snapshot_.phase = bootstrap_ == RestoreKind::NewWallet
                    ? Phase::Syncing
                    : Phase::ResolvingBirthday;
            }
        } else if (done < total) {
            snapshot_.phase = isRestoreMode() ? Phase::ScanningWalletOutputs : Phase::Syncing;
        } else if (connected_ && client_ && client_->isSynced()) {
            snapshot_.phase = Phase::CatchingUp;
            maybeReportReadyLocked();
        } else if (connected_) {
            snapshot_.phase = isRestoreMode() ? Phase::CatchingUp : Phase::Syncing;
        }
        if (isRestoreMode()) {
            snapshot_.hasRestoreProgress = true;
            snapshot_.restoreCurrent = static_cast<std::uint64_t>(std::max(0, done));
            snapshot_.restoreTarget = static_cast<std::uint64_t>(std::max(0, total));
        }
        maybeStartBootstrapLocked();
    }

    void onConnection(bool connected) {
        std::lock_guard<std::mutex> lock(mutex_);
        connected_ = connected;
        if (recoveryQuorumFailed_ || bootstrapFailed_) return;
        if (!connected_) {
            snapshot_.phase = Phase::Offline;
        } else if (snapshot_.phase == Phase::Offline || snapshot_.phase == Phase::Stopped) {
            snapshot_.phase = Phase::Connecting;
        }
        maybeStartBootstrapLocked();
    }

    void onError(const std::string& message, bool retryable) {
        std::lock_guard<std::mutex> lock(mutex_);
        snapshot_.phase = Phase::Error;
        snapshot_.failureMessage = message;
        snapshot_.failureRetryable = retryable;
        snapshot_.failureKind = retryable ? "Node" : "Native";
    }

    void onQuorumError(const std::string& message) {
        std::lock_guard<std::mutex> lock(mutex_);
        recoveryQuorumFailed_ = true;
        snapshot_.phase = Phase::Error;
        snapshot_.failureMessage = message;
        snapshot_.failureRetryable = true;
        snapshot_.failureKind = "Quorum";
    }

private:
    bool canReportReadyLocked() const {
        return !recoveryQuorumFailed_ && !bootstrapFailed_ && !bootstrapPending_ &&
            !bootstrapBodyScanPending_ && !snapshotImportPending_ &&
            initialStatusLoaded_ && initialTransactionsLoaded_ &&
            connected_ && client_ && client_->isSynced();
    }

    void maybeReportReadyLocked() {
        if (canReportReadyLocked()) snapshot_.phase = Phase::Ready;
    }

    class RecoveryProgress final : public IWalletDB::IRecoveryProgress {
    public:
        explicit RecoveryProgress(Session& session) : session_(session) {}

        bool OnProgress(std::uint64_t done, std::uint64_t total) override {
            session_.onSnapshotImportProgress(done, total);
            return !session_.cancelRequested_.load();
        }

    private:
        Session& session_;
    };

    void startSnapshotImportLocked() {
        const auto snapshotPath = utf8Path(snapshotLocation_);
        if (snapshotLocation_.empty() || !std::filesystem::is_regular_file(snapshotPath)) {
            throw std::invalid_argument("Downloaded Beam recovery snapshot is missing");
        }
        constexpr std::uintmax_t maxSnapshotBytes = 2ULL * 1024ULL * 1024ULL * 1024ULL;
        if (std::filesystem::file_size(snapshotPath) > maxSnapshotBytes) {
            throw std::invalid_argument("Beam recovery snapshot exceeds the 2 GiB safety limit");
        }
        auto self = shared_from_this();
        auto client = client_;
        auto path = snapshotLocation_;
        snapshotImportPending_ = true;
        snapshot_.phase = Phase::ImportingSnapshot;
        client->getAsync()->makeIWTCall(
            [self, client, path]() -> boost::any {
                SnapshotImportResult result;
                try {
                    auto wallet = client->getWallet();
                    if (!wallet) throw std::runtime_error("Beam wallet engine is unavailable for snapshot import");
                    RecoveryProgress progress(*self);
                    result.success = self->database_->ImportRecovery(path, *wallet, progress);
                } catch (const std::exception& error) {
                    result.error = error.what();
                } catch (...) {
                    result.error = "Unknown Beam Core recovery import error";
                }
                return boost::any(std::move(result));
            },
            [self, client](const boost::any& value) {
                try {
                    self->finishSnapshotImport(client, boost::any_cast<const SnapshotImportResult&>(value));
                } catch (const std::exception& error) {
                    self->finishSnapshotImport(client, SnapshotImportResult{
                        false,
                        std::string("Beam recovery snapshot result failed: ") + error.what(),
                    });
                }
            }
        );
    }

    void onSnapshotImportProgress(std::uint64_t done, std::uint64_t total) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!snapshotImportPending_) return;
        snapshot_.phase = Phase::ImportingSnapshot;
        snapshot_.hasRestoreBytes = true;
        snapshot_.restoreBytes = done;
        snapshot_.restoreTotalBytes = total;
    }

    void finishSnapshotImport(
        const std::shared_ptr<BridgeWalletClient>& client,
        const SnapshotImportResult& result
    ) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!snapshotImportPending_) return;
        if (!result.success || cancelRequested_.load()) {
            snapshotImportPending_ = false;
            snapshot_.transactions.clear();
            if (!cancelRequested_.load()) {
                bootstrapFailed_ = true;
                snapshot_.phase = Phase::Error;
                snapshot_.failureMessage = result.error.empty()
                    ? "Beam Core rejected the recovery snapshot"
                    : "Beam recovery snapshot import failed: " + result.error;
                snapshot_.failureRetryable = false;
                snapshot_.failureKind = "Download";
            }
            try {
                rollbackDatabase(database_);
            } catch (const std::exception& rollbackError) {
                if (!cancelRequested_.load()) {
                    snapshot_.failureMessage += std::string("; rollback failed: ") + rollbackError.what();
                    snapshot_.failureKind = "Storage";
                }
            }
            return;
        }
        try {
            auto wallet = client->getWallet();
            if (!wallet) throw std::runtime_error("Beam wallet engine is unavailable after snapshot import");
            beam::Block::SystemState::Full importedTip;
            if (!database_->get_History().get_Tip(importedTip)) {
                throw std::runtime_error("Beam recovery snapshot has no verified header tip");
            }
            const auto snapshotHeight = importedTip.get_Height();
            if (snapshotHeight == std::numeric_limits<Height>::max()) {
                throw std::runtime_error("Beam recovery snapshot height is invalid");
            }
            // ImportRecovery updates WalletDB after Wallet was constructed. Seed both Wallet's
            // in-memory shielded counter and its durable reorg checkpoint at the imported tip
            // before any post-snapshot body can be recognized.
            wallet->StartBodyRequestsAt(
                snapshotHeight + 1,
                database_->get_ShieldedOuts(),
                snapshotHeight + 1,
                true
            );
            wallet->EnableBodyRequests(true);
            beam::wallet::storage::setVar(*database_, kRestoreInitializedVar, true);
            database_->removeVarRaw(kSnapshotPathVar);
            database_->removeVarRaw(kSnapshotUrlVar);
            database_->removeVarRaw(kSnapshotHashVar);
            flushDatabase(database_);
            snapshotImportPending_ = false;
            bootstrapInitialized_ = true;
            snapshot_.phase = Phase::CatchingUp;
            client->getAsync()->getTransactions();
            client->getAsync()->getWalletStatus();
        } catch (const std::exception& error) {
            snapshotImportPending_ = false;
            snapshot_.transactions.clear();
            bootstrapFailed_ = true;
            snapshot_.phase = Phase::Error;
            snapshot_.failureMessage = std::string("Beam recovery snapshot finalization failed: ") + error.what();
            snapshot_.failureRetryable = false;
            snapshot_.failureKind = "Native";
            try {
                rollbackDatabase(database_);
            } catch (const std::exception& rollbackError) {
                snapshot_.failureMessage += std::string("; rollback failed: ") + rollbackError.what();
                snapshot_.failureKind = "Storage";
            }
        }
    }

    template <typename T, typename Function>
    T invoke(Function&& function) {
        std::shared_ptr<BridgeWalletClient> client;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!client_) throw std::logic_error("Beam wallet is not running");
            client = client_;
        }
        struct Result {
            std::shared_ptr<T> value;
            std::exception_ptr error;
        };
        auto promise = std::make_shared<std::promise<Result>>();
        auto future = promise->get_future();
        client->getAsync()->makeIWTCall(
            [call = std::forward<Function>(function)]() mutable -> boost::any {
                Result result;
                try {
                    result.value = std::make_shared<T>(call());
                } catch (...) {
                    result.error = std::current_exception();
                }
                return boost::any(std::move(result));
            },
            [promise](const boost::any& value) {
                promise->set_value(boost::any_cast<const Result&>(value));
            }
        );
        if (future.wait_for(std::chrono::seconds(30)) != std::future_status::ready) {
            throw std::runtime_error("Timed out waiting for Beam owner thread");
        }
        auto result = future.get();
        if (result.error) std::rethrow_exception(result.error);
        if (!result.value) throw std::runtime_error("Beam owner thread returned no result");
        return std::move(*result.value);
    }

    template <typename T, typename Function>
    T invokeOrUseStoppedDatabase(Function&& function) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            ownerThreadStopped_.wait(lock, [this]() { return !ownerThreadStopping_; });
            requireDatabase();
            if (!client_) {
                Rules::Scope scope(rules_);
                beam::io::Reactor::Scope reactorScope(*reactor_);
                return function();
            }
        }
        return invoke<T>(std::forward<Function>(function));
    }

    std::pair<std::int64_t, std::string> previewMaterial(
        const std::string& receiver,
        std::uint64_t amount,
        const std::string& comment
    ) {
        auto fee = previewFee(receiver, amount);
        return requestDigest(receiver, amount, comment, fee);
    }

    std::pair<std::int64_t, std::string> requestDigest(
        const std::string& receiver,
        std::uint64_t amount,
        const std::string& comment,
        std::uint64_t fee
    ) const {
        auto material = receiver + "\n" + std::to_string(amount) + "\n" + comment + "\n" + std::to_string(fee);
        return stableDigest(material);
    }

    std::uint64_t previewFee(const std::string& receiver, std::uint64_t amount) {
        requireOneSidedAddress(receiver);
        CoinsSelectionInfo selection;
        selection.m_requestedSum = amount;
        selection.m_assetID = Asset::s_BeamID;
        selection.Calculate(database_->getCurrentHeight(), database_, true);
        if (!selection.m_isEnought) throw std::runtime_error("Insufficient Beam funds including fee");
        return selection.get_TotalFee();
    }

    Json resolveSendOnWalletThread(SendRecord& record) {
        auto txId = parseTxId(record.txId);
        auto transaction = database_->getTx(txId);
        if (transaction && isTerminal(transaction->m_status)) {
            clearActiveSendIfMatches(record.operationId);
            flushDatabase(database_);
            auto status = transactionStatus(transaction->m_status);
            return sendResolution("Terminal", record.txId, status.c_str());
        }
        if (record.state == SendState::Prepared) return sendResolution("Prepared", record.txId);
        if (record.state == SendState::Committing && transaction) {
            record.state = SendState::Submitted;
            saveSendRecord(*database_, record);
            flushDatabase(database_);
            return sendResolution("Submitted", record.txId);
        }
        if (record.state == SendState::Committing) return sendResolution("Indeterminate", record.txId);
        if (transaction) return sendResolution("Submitted", record.txId);
        return sendResolution("Indeterminate", record.txId);
    }

    void clearActiveSendIfMatches(const std::string& operationId) {
        std::string activeOperation;
        if (getRawString(*database_, kActiveSendVar, activeOperation) && activeOperation == operationId) {
            database_->removeVarRaw(kActiveSendVar);
        }
    }

    void maybeStartBootstrapLocked() {
        if (!bootstrapPending_ || bootstrapStarted_ || !connected_ || !client_ || !client_->isSynced()) return;
        auto wallet = client_->getWallet();
        if (!wallet) return;
        auto tip = database_->getCurrentHeight();
        if (bootstrap_ == RestoreKind::Date || bootstrap_ == RestoreKind::NewWallet) {
            if (tip == std::numeric_limits<Height>::max()) {
                bootstrapFailed_ = true;
                snapshot_.phase = Phase::Error;
                snapshot_.failureMessage = "Beam verified tip is unavailable for date restore";
                snapshot_.failureRetryable = true;
                snapshot_.failureKind = "Node";
                return;
            }
            if (bootstrap_ == RestoreKind::NewWallet && tip <= rejectedNewWalletTip_) return;
            bootstrapStarted_ = true;
            snapshot_.phase = Phase::ResolvingBirthday;
            // Beam's historical-header protocol starts at height 1. A date before genesis
            // converges to that first header, after which the safety window intentionally
            // selects birthday 0 for a complete treasury/body scan.
            dateSearchTipHeight_ = tip;
            if (bootstrap_ == RestoreKind::NewWallet) {
                requestNewWalletTipFreshnessLocked(wallet);
            } else {
                startDateSearchLocked(wallet, tip);
            }
            return;
        }

        Height birthday = 0;
        if (bootstrap_ == RestoreKind::Height) {
            birthday = std::min(restoreHeight_, tip + 1);
        }
        bootstrapStarted_ = true;
        startBodyBootstrapLocked(wallet, birthday);
    }

    void startDateSearchLocked(const std::shared_ptr<beam::wallet::Wallet>& wallet, Height tip) {
        dateSearchTipHeight_ = tip;
        dateSearchLow_ = 1;
        dateSearchHighExclusive_ = tip + 1;
        requestNextDateHeaderLocked(wallet);
    }

    void requestNewWalletTipFreshnessLocked(const std::shared_ptr<beam::wallet::Wallet>& wallet) {
        const auto requestedTip = dateSearchTipHeight_;
        std::weak_ptr<Session> weak = shared_from_this();
        wallet->RequestHistoricalHeaderAt(requestedTip, [weak, requestedTip](Height height, Timestamp timestamp) {
            auto session = weak.lock();
            if (!session) return;
            std::lock_guard<std::mutex> lock(session->mutex_);
            if (!session->client_ || session->cancelRequested_.load() ||
                session->bootstrap_ != RestoreKind::NewWallet ||
                !session->bootstrapPending_ || !session->bootstrapStarted_ ||
                session->dateSearchTipHeight_ != requestedTip) {
                return;
            }
            if (height != requestedTip) {
                session->bootstrapFailed_ = true;
                session->snapshot_.phase = Phase::Error;
                session->snapshot_.failureMessage = "Beam node returned the wrong tip header";
                session->snapshot_.failureRetryable = true;
                session->snapshot_.failureKind = "Quorum";
                return;
            }
            if (!isNewWalletTipFresh(session->restoreDate_, timestamp)) {
                // A newly created wallet can briefly connect to a live but badly outdated peer
                // before a current peer wins chain selection. Never turn that stale tip into a
                // durable birthday: wait until a newer verified tip is observed instead.
                session->rejectedNewWalletTip_ = requestedTip;
                session->bootstrapStarted_ = false;
                session->snapshot_.phase = Phase::Syncing;
                return;
            }
            session->rejectedNewWalletTip_ = 0;
            auto activeWallet = session->client_->getWallet();
            if (activeWallet) session->startDateSearchLocked(activeWallet, requestedTip);
        });
    }

    void requestNextDateHeaderLocked(const std::shared_ptr<beam::wallet::Wallet>& wallet) {
        if (dateSearchLow_ >= dateSearchHighExclusive_) {
            const auto currentTip = database_->getCurrentHeight();
            if (bootstrap_ == RestoreKind::NewWallet && currentTip > dateSearchTipHeight_ &&
                currentTip - dateSearchTipHeight_ > kBirthdaySafetyWindow) {
                dateSearchTipHeight_ = currentTip;
                requestNewWalletTipFreshnessLocked(wallet);
                return;
            }
            const auto birthday = dateSearchLow_ > kBirthdaySafetyWindow
                ? dateSearchLow_ - kBirthdaySafetyWindow
                : 0;
            startBodyBootstrapLocked(wallet, birthday);
            return;
        }

        const auto requested = dateSearchLow_ + (dateSearchHighExclusive_ - dateSearchLow_) / 2;
        dateSearchRequested_ = requested;
        std::weak_ptr<Session> weak = shared_from_this();
        wallet->RequestHistoricalHeaderAt(requested, [weak, requested](Height height, Timestamp timestamp) {
            auto session = weak.lock();
            if (!session) return;
            std::lock_guard<std::mutex> lock(session->mutex_);
            if (!session->client_ || session->cancelRequested_.load() ||
                (session->bootstrap_ != RestoreKind::Date && session->bootstrap_ != RestoreKind::NewWallet) ||
                !session->bootstrapPending_ ||
                !session->bootstrapStarted_ || session->dateSearchRequested_ != requested) {
                return;
            }
            if (height != requested) {
                session->bootstrapFailed_ = true;
                session->snapshot_.phase = Phase::Error;
                session->snapshot_.failureMessage = "Beam node returned the wrong historical header";
                session->snapshot_.failureRetryable = true;
                session->snapshot_.failureKind = "Quorum";
                return;
            }
            if (timestamp < session->restoreDate_) {
                session->dateSearchLow_ = requested + 1;
            } else {
                session->dateSearchHighExclusive_ = requested;
            }
            auto activeWallet = session->client_->getWallet();
            if (activeWallet) session->requestNextDateHeaderLocked(activeWallet);
        });
    }

    void startBodyBootstrapLocked(
        const std::shared_ptr<beam::wallet::Wallet>& wallet,
        Height birthday
    ) {
        snapshot_.phase = birthday == 0 ? Phase::ScanningWalletOutputs : Phase::CountingShieldedOutputs;
        if (bootstrap_ == RestoreKind::NewWallet && birthday > 0) {
            wallet->RequestShieldedOutputsAt(birthday - 1, [this, birthday](Height height, TxoID count) {
                std::lock_guard<std::mutex> lock(mutex_);
                if (!client_ || height != birthday - 1) {
                    recoveryQuorumFailed_ = true;
                    snapshot_.phase = Phase::Error;
                    snapshot_.failureMessage = "Beam node returned an invalid new-wallet boundary";
                    snapshot_.failureRetryable = true;
                    snapshot_.failureKind = "Quorum";
                    bootstrapStarted_ = false;
                    return;
                }
                auto activeWallet = client_->getWallet();
                if (!activeWallet) return;
                // A new wallet's boundary is derived from its persisted creation time, not
                // supplied by the user. A reorg below that boundary must lower it so
                // replacement-chain receipts are recognized instead of processed count-only.
                bootstrapBodyScanPending_ = true;
                activeWallet->StartBodyRequestsAt(birthday, count, birthday, true);
                activeWallet->EnableBodyRequests(true);
                beam::wallet::storage::setVar(*database_, kRestoreInitializedVar, true);
                flushDatabase(database_);
                bootstrapInitialized_ = true;
                bootstrapPending_ = false;
                snapshot_.phase = Phase::Syncing;
            });
            return;
        }
        // The fallback checkpoint is the consensus-bound genesis/treasury state. Bodies before
        // birthday are processed in Core's count-only mode, so they advance global shielded
        // indices without creating wallet records. A future signed checkpoint can move the scan
        // start forward without changing the birthday semantics.
        bootstrapBodyScanPending_ = true;
        wallet->StartBodyRequestsAt(0, 0, birthday);
        wallet->EnableBodyRequests(true);
        beam::wallet::storage::setVar(*database_, kRestoreInitializedVar, true);
        flushDatabase(database_);
        bootstrapInitialized_ = true;
        bootstrapPending_ = false;
    }

    bool isRestoreMode() const {
        return bootstrap_ == RestoreKind::Height || bootstrap_ == RestoreKind::Date ||
            bootstrap_ == RestoreKind::Full || bootstrap_ == RestoreKind::Snapshot;
    }

    std::string databasePath() const {
        return utf8String(utf8Path(storagePath_) / std::filesystem::u8path(kWalletFile));
    }

    void requireDatabase() const {
        if (!database_) throw std::logic_error("Beam WalletDB is not open");
    }

    static std::map<Notification::Type, bool> notificationSettings() {
        return {
            {Notification::Type::SoftwareUpdateAvailable, false},
            {Notification::Type::BeamNews, false},
            {Notification::Type::WalletImplUpdateAvailable, false},
            {Notification::Type::TransactionCompleted, false},
            {Notification::Type::TransactionFailed, false},
            {Notification::Type::AddressStatusChanged, false},
        };
    }

    int network_;
    std::string storagePath_;
    Rules rules_;
    beam::io::Reactor::Ptr reactor_;
    IWalletDB::Ptr database_;
    std::shared_ptr<BridgeWalletClient> client_;
    mutable std::mutex mutex_;
    std::condition_variable ownerThreadStopped_;
    NativeSnapshot snapshot_;
    RestoreKind bootstrap_ = RestoreKind::Existing;
    Height restoreHeight_ = 0;
    Timestamp restoreDate_ = 0;
    Height dateSearchLow_ = 1;
    Height dateSearchHighExclusive_ = 0;
    Height dateSearchRequested_ = 0;
    Height dateSearchTipHeight_ = 0;
    Height rejectedNewWalletTip_ = 0;
    std::string snapshotLocation_;
    std::string snapshotUrl_;
    std::string snapshotExpectedSha256_;
    bool connected_ = false;
    bool bootstrapPending_ = false;
    bool bootstrapStarted_ = false;
    bool bootstrapInitialized_ = false;
    bool bootstrapBodyScanPending_ = false;
    bool snapshotImportPending_ = false;
    bool bootstrapFailed_ = false;
    bool recoveryQuorumFailed_ = false;
    bool initialStatusLoaded_ = false;
    bool initialTransactionsLoaded_ = false;
    bool ownerThreadStopping_ = false;
    std::atomic<bool> cancelRequested_{false};
};

BridgeWalletClient::BridgeWalletClient(
    Session& owner,
    const Rules& rules,
    IWalletDB::Ptr database,
    const std::string& nodeAddress,
    beam::io::Reactor::Ptr reactor
) : WalletClient(rules, std::move(database), nodeAddress, std::move(reactor)), owner_(owner) {
}

void BridgeWalletClient::shutdown() {
    stopReactor();
}

void BridgeWalletClient::onStatus(const WalletStatus& status) {
    owner_.onStatus(status);
}

void BridgeWalletClient::onTxStatus(ChangeAction action, const std::vector<TxDescription>& items) {
    owner_.onTransactions(action, items);
}

void BridgeWalletClient::onSyncProgressUpdated(int done, int total) {
    owner_.onSyncProgress(done, total);
}

void BridgeWalletClient::onNodeConnectionChanged(bool connected) {
    owner_.onConnection(connected);
}

void BridgeWalletClient::onWalletError(beam::wallet::ErrorType error) {
    owner_.onError("Beam wallet error " + std::to_string(static_cast<int>(error)), true);
}

void BridgeWalletClient::FailedToStartWallet() {
    owner_.onError("Beam Core failed to start the wallet", false);
}

void BridgeWalletClient::onPostFunctionToClientContext(MessageFunction&& function) {
    function();
}

// The JNI library is process-scoped. Keep its registry process-scoped as well instead of
// destroying active Sessions from global C++ destructors while the JVM is unloading this DSO.
// At that point OpenSSL/SQLCipher globals may already be gone, and Session::close() can race
// their teardown. Explicit close still removes and destroys every normal SDK session; any
// sessions left during abrupt JVM termination are reclaimed by the operating system.
std::mutex& registryMutex = *new std::mutex();
std::mutex& lifecycleMutex = *new std::mutex();
std::unordered_map<std::int64_t, std::shared_ptr<Session>>& sessions =
    *new std::unordered_map<std::int64_t, std::shared_ptr<Session>>();
std::unordered_map<std::string, std::int64_t>& activeSessionIdentities =
    *new std::unordered_map<std::string, std::int64_t>();
std::unordered_set<std::int64_t>& closingSessions =
    *new std::unordered_set<std::int64_t>();
std::atomic<std::int64_t> nextHandle{1};

std::shared_ptr<Session> requireSession(jlong handle) {
    std::lock_guard<std::mutex> lock(registryMutex);
    auto iterator = sessions.find(handle);
    if (iterator == sessions.end() || closingSessions.count(handle)) {
        throw std::invalid_argument("Unknown or closing Beam wallet handle");
    }
    return iterator->second;
}

jlong registerSession(std::shared_ptr<Session> session) {
    std::lock_guard<std::mutex> lock(registryMutex);
    auto identity = session->pathIdentity();
    if (activeSessionIdentities.find(identity) != activeSessionIdentities.end()) {
        throw std::logic_error("A Beam wallet session is already open for this account and network");
    }
    auto handle = nextHandle.fetch_add(1);
    sessions.emplace(handle, std::move(session));
    activeSessionIdentities.emplace(std::move(identity), handle);
    return handle;
}

void registerAccountIdentity(jlong handle, const std::string& identity) {
    std::lock_guard<std::mutex> lock(registryMutex);
    if (sessions.find(handle) == sessions.end()) throw std::logic_error("Beam session reservation was lost");
    auto existing = activeSessionIdentities.find(identity);
    if (existing != activeSessionIdentities.end() && existing->second != handle) {
        throw std::logic_error("A Beam wallet session is already open for this account and network");
    }
    activeSessionIdentities[identity] = handle;
}

std::shared_ptr<Session> beginCloseSession(jlong handle) {
    std::lock_guard<std::mutex> lock(registryMutex);
    auto iterator = sessions.find(handle);
    if (iterator == sessions.end()) return {};
    if (!closingSessions.emplace(handle).second) return {};
    return iterator->second;
}

void finishCloseSession(jlong handle) {
    std::lock_guard<std::mutex> lock(registryMutex);
    for (auto iterator = activeSessionIdentities.begin(); iterator != activeSessionIdentities.end();) {
        if (iterator->second == handle) iterator = activeSessionIdentities.erase(iterator);
        else ++iterator;
    }
    sessions.erase(handle);
    closingSessions.erase(handle);
}

void appendUtf8(std::string& output, std::uint32_t codePoint) {
    if (codePoint <= 0x7f) {
        output.push_back(static_cast<char>(codePoint));
    } else if (codePoint <= 0x7ff) {
        output.push_back(static_cast<char>(0xc0 | (codePoint >> 6)));
        output.push_back(static_cast<char>(0x80 | (codePoint & 0x3f)));
    } else if (codePoint <= 0xffff) {
        output.push_back(static_cast<char>(0xe0 | (codePoint >> 12)));
        output.push_back(static_cast<char>(0x80 | ((codePoint >> 6) & 0x3f)));
        output.push_back(static_cast<char>(0x80 | (codePoint & 0x3f)));
    } else {
        output.push_back(static_cast<char>(0xf0 | (codePoint >> 18)));
        output.push_back(static_cast<char>(0x80 | ((codePoint >> 12) & 0x3f)));
        output.push_back(static_cast<char>(0x80 | ((codePoint >> 6) & 0x3f)));
        output.push_back(static_cast<char>(0x80 | (codePoint & 0x3f)));
    }
}

std::string javaString(JNIEnv* environment, jstring value) {
    if (value == nullptr) throw std::invalid_argument("Unexpected null string");
    const auto length = environment->GetStringLength(value);
    const jchar* characters = environment->GetStringChars(value, nullptr);
    if (characters == nullptr) throw std::runtime_error("Unable to read Java string");
    try {
        std::string result;
        result.reserve(static_cast<std::size_t>(length) * 3);
        for (jsize index = 0; index < length; ++index) {
            std::uint32_t codePoint = characters[index];
            if (codePoint >= 0xd800 && codePoint <= 0xdbff) {
                if (++index >= length || characters[index] < 0xdc00 || characters[index] > 0xdfff) {
                    throw std::invalid_argument("Java string contains an unpaired UTF-16 surrogate");
                }
                codePoint = 0x10000 + ((codePoint - 0xd800) << 10) + (characters[index] - 0xdc00);
            } else if (codePoint >= 0xdc00 && codePoint <= 0xdfff) {
                throw std::invalid_argument("Java string contains an unpaired UTF-16 surrogate");
            }
            appendUtf8(result, codePoint);
        }
        environment->ReleaseStringChars(value, characters);
        return result;
    } catch (...) {
        environment->ReleaseStringChars(value, characters);
        throw;
    }
}

std::vector<std::uint8_t> javaBytes(JNIEnv* environment, jbyteArray value) {
    if (value == nullptr) throw std::invalid_argument("Unexpected null byte array");
    auto size = environment->GetArrayLength(value);
    std::vector<std::uint8_t> result(static_cast<std::size_t>(size));
    environment->GetByteArrayRegion(value, 0, size, reinterpret_cast<jbyte*>(result.data()));
    return result;
}

jstring newJavaString(JNIEnv* environment, const std::string& value) {
    std::vector<jchar> utf16;
    utf16.reserve(value.size());
    for (std::size_t index = 0; index < value.size();) {
        const auto first = static_cast<std::uint8_t>(value[index++]);
        std::uint32_t codePoint = 0;
        std::size_t continuationCount = 0;
        std::uint32_t minimum = 0;
        if (first <= 0x7f) {
            codePoint = first;
        } else if ((first & 0xe0) == 0xc0) {
            codePoint = first & 0x1f;
            continuationCount = 1;
            minimum = 0x80;
        } else if ((first & 0xf0) == 0xe0) {
            codePoint = first & 0x0f;
            continuationCount = 2;
            minimum = 0x800;
        } else if ((first & 0xf8) == 0xf0) {
            codePoint = first & 0x07;
            continuationCount = 3;
            minimum = 0x10000;
        } else {
            throw std::invalid_argument("Native string is not valid UTF-8");
        }
        if (index + continuationCount > value.size()) {
            throw std::invalid_argument("Native string contains truncated UTF-8");
        }
        for (std::size_t continuation = 0; continuation < continuationCount; ++continuation) {
            const auto next = static_cast<std::uint8_t>(value[index++]);
            if ((next & 0xc0) != 0x80) throw std::invalid_argument("Native string is not valid UTF-8");
            codePoint = (codePoint << 6) | (next & 0x3f);
        }
        if ((continuationCount && codePoint < minimum) || codePoint > 0x10ffff ||
            (codePoint >= 0xd800 && codePoint <= 0xdfff)) {
            throw std::invalid_argument("Native string is not canonical UTF-8");
        }
        if (codePoint <= 0xffff) {
            utf16.push_back(static_cast<jchar>(codePoint));
        } else {
            codePoint -= 0x10000;
            utf16.push_back(static_cast<jchar>(0xd800 + (codePoint >> 10)));
            utf16.push_back(static_cast<jchar>(0xdc00 + (codePoint & 0x3ff)));
        }
    }
    if (utf16.size() > static_cast<std::size_t>(std::numeric_limits<jsize>::max())) {
        throw std::overflow_error("Native string is too large for Java");
    }
    return environment->NewString(
        utf16.empty() ? nullptr : utf16.data(),
        static_cast<jsize>(utf16.size())
    );
}

void throwJava(JNIEnv* environment, const std::exception& error) {
    const std::string detail = error.what();
    const char* className = "java/lang/IllegalStateException";
    const char* code = "NATIVE";
    if (dynamic_cast<const std::invalid_argument*>(&error) != nullptr ||
        dynamic_cast<const std::overflow_error*>(&error) != nullptr) {
        className = "java/lang/IllegalArgumentException";
        code = "VALIDATION";
    } else if (dynamic_cast<const std::filesystem::filesystem_error*>(&error) != nullptr ||
        detail.find("WalletDB") != std::string::npos || detail.find("database") != std::string::npos ||
        detail.find("storage") != std::string::npos || detail.find("disk") != std::string::npos) {
        code = "STORAGE";
    } else if (detail.find("Insufficient") != std::string::npos ||
        detail.find("insufficient") != std::string::npos) {
        code = "INSUFFICIENT_FUNDS";
    } else if (dynamic_cast<const std::logic_error*>(&error) != nullptr) {
        code = "VALIDATION";
    }
    auto type = environment->FindClass(className);
    if (type != nullptr) {
        auto message = std::string("BEAM_") + code + "|" + detail;
        try {
            auto constructor = environment->GetMethodID(type, "<init>", "(Ljava/lang/String;)V");
            auto javaMessage = newJavaString(environment, message);
            if (constructor != nullptr && javaMessage != nullptr) {
                auto exception = static_cast<jthrowable>(environment->NewObject(type, constructor, javaMessage));
                if (exception != nullptr) environment->Throw(exception);
                environment->DeleteLocalRef(exception);
            }
            environment->DeleteLocalRef(javaMessage);
        } catch (...) {
            environment->ThrowNew(type, "BEAM_NATIVE|Native call failed with a non-UTF-8 error");
        }
        environment->DeleteLocalRef(type);
    }
}

template <typename Function, typename Fallback>
auto jniCall(JNIEnv* environment, Function&& function, Fallback fallback) -> decltype(function()) {
    try {
        return function();
    } catch (const std::exception& error) {
        throwJava(environment, error);
        return fallback;
    }
}

}  // namespace

extern "C" JNIEXPORT jstring JNICALL
Java_cash_p_beam_internal_BeamNative_version(JNIEnv* environment, jobject) {
    return newJavaString(environment, std::string("beam-7.5.14493+") + BEAM_SDK_CORE_COMMIT);
}

extern "C" JNIEXPORT jstring JNICALL
Java_cash_p_beam_internal_BeamNative_stringRoundTripForTests(
    JNIEnv* environment,
    jobject,
    jstring value
) {
    return jniCall(environment, [&]() {
        return newJavaString(environment, javaString(environment, value));
    }, static_cast<jstring>(nullptr));
}

extern "C" JNIEXPORT jlong JNICALL
Java_cash_p_beam_internal_BeamNative_create(
    JNIEnv* environment,
    jobject,
    jstring storagePath,
    jbyteArray databaseKey,
    jbyteArray seed,
    jint network,
    jint restoreType,
    jstring restoreValue,
    jint
) {
    return jniCall(environment, [&]() -> jlong {
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex);
        auto seedBytes = javaBytes(environment, seed);
        ByteWiper seedWiper(seedBytes);
        auto keyBytes = javaBytes(environment, databaseKey);
        ByteWiper keyWiper(keyBytes);
        auto session = std::make_shared<Session>(network, javaString(environment, storagePath));
        auto handle = registerSession(session);
        try {
            session->create(
                seedBytes,
                keyBytes,
                restoreType,
                javaString(environment, restoreValue)
            );
            registerAccountIdentity(handle, session->accountIdentity());
            return handle;
        } catch (...) {
            auto closing = beginCloseSession(handle);
            if (closing) {
                try { closing->close(); } catch (...) {}
            }
            finishCloseSession(handle);
            throw;
        }
    }, jlong(0));
}

extern "C" JNIEXPORT jlong JNICALL
Java_cash_p_beam_internal_BeamNative_open(
    JNIEnv* environment,
    jobject,
    jstring storagePath,
    jbyteArray databaseKey,
    jint network,
    jint
) {
    return jniCall(environment, [&]() -> jlong {
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex);
        auto keyBytes = javaBytes(environment, databaseKey);
        ByteWiper keyWiper(keyBytes);
        auto session = std::make_shared<Session>(network, javaString(environment, storagePath));
        auto handle = registerSession(session);
        try {
            session->open(keyBytes);
            registerAccountIdentity(handle, session->accountIdentity());
            return handle;
        } catch (...) {
            auto closing = beginCloseSession(handle);
            if (closing) {
                try { closing->close(); } catch (...) {}
            }
            finishCloseSession(handle);
            throw;
        }
    }, jlong(0));
}

extern "C" JNIEXPORT jstring JNICALL
Java_cash_p_beam_internal_BeamNative_snapshotRestoreIntent(
    JNIEnv* environment,
    jobject,
    jlong handle
) {
    return jniCall(environment, [&]() {
        return newJavaString(environment, requireSession(handle)->snapshotRestoreIntentJson());
    }, static_cast<jstring>(nullptr));
}

extern "C" JNIEXPORT void JNICALL
Java_cash_p_beam_internal_BeamNative_start(JNIEnv* environment, jobject, jlong handle) {
    jniCall(environment, [&]() { requireSession(handle)->start(); return true; }, false);
}

extern "C" JNIEXPORT void JNICALL
Java_cash_p_beam_internal_BeamNative_stop(JNIEnv* environment, jobject, jlong handle) {
    jniCall(environment, [&]() { requireSession(handle)->stop(); return true; }, false);
}

extern "C" JNIEXPORT void JNICALL
Java_cash_p_beam_internal_BeamNative_close(JNIEnv* environment, jobject, jlong handle) {
    jniCall(environment, [&]() {
        std::lock_guard<std::mutex> lifecycleLock(lifecycleMutex);
        auto session = beginCloseSession(handle);
        if (!session) return true;
        try {
            session->close();
        } catch (...) {
            finishCloseSession(handle);
            throw;
        }
        finishCloseSession(handle);
        return true;
    }, false);
}

extern "C" JNIEXPORT jstring JNICALL
Java_cash_p_beam_internal_BeamNative_snapshot(JNIEnv* environment, jobject, jlong handle) {
    return jniCall(environment, [&]() {
        return newJavaString(environment, requireSession(handle)->snapshotJson());
    }, static_cast<jstring>(nullptr));
}

extern "C" JNIEXPORT jstring JNICALL
Java_cash_p_beam_internal_BeamNative_receiveAddress(
    JNIEnv* environment,
    jobject,
    jlong handle,
    jint type
) {
    return jniCall(environment, [&]() {
        return newJavaString(environment, requireSession(handle)->receiveAddress(type));
    }, static_cast<jstring>(nullptr));
}

extern "C" JNIEXPORT jstring JNICALL
Java_cash_p_beam_internal_BeamNative_transactions(
    JNIEnv* environment,
    jobject,
    jlong handle,
    jint offset,
    jint limit
) {
    return jniCall(environment, [&]() {
        return newJavaString(environment, requireSession(handle)->transactions(offset, limit));
    }, static_cast<jstring>(nullptr));
}

extern "C" JNIEXPORT jstring JNICALL
Java_cash_p_beam_internal_BeamNative_previewSend(
    JNIEnv* environment,
    jobject,
    jlong handle,
    jstring receiver,
    jlong amount,
    jstring comment
) {
    return jniCall(environment, [&]() {
        if (amount <= 0) throw std::invalid_argument("Send amount must be positive");
        return newJavaString(environment, requireSession(handle)->previewSend(
            javaString(environment, receiver),
            static_cast<std::uint64_t>(amount),
            javaString(environment, comment)
        ));
    }, static_cast<jstring>(nullptr));
}

extern "C" JNIEXPORT jstring JNICALL
Java_cash_p_beam_internal_BeamNative_prepareSend(
    JNIEnv* environment,
    jobject,
    jlong handle,
    jstring operationId,
    jstring receiver,
    jlong amount,
    jstring comment,
    jlong previewVersion
) {
    return jniCall(environment, [&]() {
        if (amount <= 0) throw std::invalid_argument("Send amount must be positive");
        return newJavaString(environment, requireSession(handle)->prepareSend(
            javaString(environment, operationId),
            javaString(environment, receiver),
            static_cast<std::uint64_t>(amount),
            javaString(environment, comment),
            previewVersion
        ));
    }, static_cast<jstring>(nullptr));
}

extern "C" JNIEXPORT jstring JNICALL
Java_cash_p_beam_internal_BeamNative_commitSend(
    JNIEnv* environment,
    jobject,
    jlong handle,
    jstring operationId
) {
    return jniCall(environment, [&]() {
        return newJavaString(environment, requireSession(handle)->commitSend(javaString(environment, operationId)));
    }, static_cast<jstring>(nullptr));
}

extern "C" JNIEXPORT jstring JNICALL
Java_cash_p_beam_internal_BeamNative_resolveSend(
    JNIEnv* environment,
    jobject,
    jlong handle,
    jstring operationId
) {
    return jniCall(environment, [&]() {
        return newJavaString(environment, requireSession(handle)->resolveSend(javaString(environment, operationId)));
    }, static_cast<jstring>(nullptr));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_cash_p_beam_internal_BeamNative_abortPrepared(
    JNIEnv* environment,
    jobject,
    jlong handle,
    jstring operationId
) {
    return jniCall(environment, [&]() -> jboolean {
        return requireSession(handle)->abortPrepared(javaString(environment, operationId)) ? JNI_TRUE : JNI_FALSE;
    }, JNI_FALSE);
}

#ifdef BEAM_SDK_KMP_TESTS
extern "C" JNIEXPORT jstring JNICALL
Java_cash_p_beam_internal_BeamNative_seedPreparedSendForTests(
    JNIEnv* environment,
    jobject,
    jlong handle,
    jstring operationId
) {
    return jniCall(environment, [&]() {
        return newJavaString(
            environment,
            requireSession(handle)->seedPreparedSendForTests(javaString(environment, operationId))
        );
    }, static_cast<jstring>(nullptr));
}

extern "C" JNIEXPORT void JNICALL
Java_cash_p_beam_internal_BeamNative_seedInterruptedBootstrapForTests(
    JNIEnv* environment,
    jobject,
    jlong handle
) {
    jniCall(environment, [&]() {
        requireSession(handle)->seedInterruptedBootstrapForTests();
        return true;
    }, false);
}

extern "C" JNIEXPORT jboolean JNICALL
Java_cash_p_beam_internal_BeamNative_bodyRequestsPendingForTests(
    JNIEnv* environment,
    jobject,
    jlong handle
) {
    return jniCall(environment, [&]() -> jboolean {
        return requireSession(handle)->bodyRequestsPendingForTests() ? JNI_TRUE : JNI_FALSE;
    }, JNI_FALSE);
}

extern "C" JNIEXPORT jlong JNICALL
Java_cash_p_beam_internal_BeamNative_newWalletCreatedAtForTests(
    JNIEnv* environment,
    jobject,
    jlong handle
) {
    return jniCall(environment, [&]() -> jlong {
        return static_cast<jlong>(requireSession(handle)->newWalletCreatedAtForTests());
    }, static_cast<jlong>(-1));
}

extern "C" JNIEXPORT jboolean JNICALL
Java_cash_p_beam_internal_BeamNative_newWalletTipFreshForTests(
    JNIEnv* environment,
    jobject,
    jlong creationTimestamp,
    jlong tipTimestamp
) {
    return jniCall(environment, [&]() -> jboolean {
        if (creationTimestamp < 0 || tipTimestamp < 0) {
            throw std::invalid_argument("Test timestamps must be non-negative");
        }
        return isNewWalletTipFresh(
            static_cast<Timestamp>(creationTimestamp),
            static_cast<Timestamp>(tipTimestamp)
        ) ? JNI_TRUE : JNI_FALSE;
    }, JNI_FALSE);
}
#endif
