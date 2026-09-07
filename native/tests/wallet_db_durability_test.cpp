#include "wallet/core/wallet_db.h"
#include "wallet/core/wallet.h"
#include "utility/io/reactor.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

thread_local const beam::Rules* beam::Rules::s_pInstance = nullptr;

namespace {

void require(bool condition, const char* message)
{
    if (!condition)
        throw std::runtime_error(message);
}

class RecordingNetwork final : public beam::proto::FlyClient::INetwork
{
public:
    void Connect() override {}
    void Disconnect() override {}
    void PostRequestInternal(beam::proto::FlyClient::Request&) override { ++postedRequests; }

    unsigned int postedRequests = 0;
};

class RegistrationTestWallet final : public beam::wallet::Wallet
{
public:
    explicit RegistrationTestWallet(const beam::wallet::IWalletDB::Ptr& database)
        : Wallet(database)
    {}

    void submitForTest(const beam::wallet::TxID& transactionId)
    {
        SendTransactionToNode(
            transactionId,
            std::make_shared<beam::Transaction>(),
            nullptr,
            beam::wallet::kDefaultSubTxID
        );
    }
};

} // namespace

int main()
{
    namespace fs = std::filesystem;
    using namespace beam;
    using namespace beam::wallet;

    const auto unique = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto databasePath = fs::temp_directory_path() /
        ("beam-sdk-kmp-durability-" + std::to_string(unique) + ".db");
    const auto atomicCreationPath = fs::temp_directory_path() /
        ("beam-sdk-kmp-atomic-create-" + std::to_string(unique) + ".db");

    try
    {
        auto reactor = io::Reactor::create();
        io::Reactor::Scope reactorScope(*reactor);
        Rules rules;
        Rules::Scope rulesScope(rules);
        ECC::NoLeak<ECC::uintBig> seed;
        seed.V = 10283UL;
        SecString password(std::string("test-password"));
        bool firstCommitFailed = false;
        try
        {
            WalletDB::init(
                atomicCreationPath.string(),
                password,
                seed,
                false,
                [](IWalletDB& initializingDatabase)
                {
                    storage::setVar(initializingDatabase, "beam.sdk.kmp.atomic.create.test", 42);
                    auto concrete = dynamic_cast<WalletDB*>(&initializingDatabase);
                    require(concrete != nullptr, "atomic creation fixture is not a WalletDB");
                    concrete->FailCurrentTransactionForTests();
                }
            );
        }
        catch (const DatabaseException&)
        {
            firstCommitFailed = true;
        }
        require(firstCommitFailed, "atomic WalletDB initializer did not share the first COMMIT");
        bool incompleteWalletOpened = false;
        try
        {
            incompleteWalletOpened = static_cast<bool>(
                WalletDB::open(atomicCreationPath.string(), password)
            );
        }
        catch (const std::exception&)
        {
            // Expected: the schema, master key, and restore metadata were all rolled back.
        }
        require(
            !incompleteWalletOpened,
            "failed initial metadata COMMIT left a valid wallet without its restore source"
        );
        fs::remove(atomicCreationPath);
        fs::remove(atomicCreationPath.string() + "-journal");

        auto database = std::dynamic_pointer_cast<WalletDB>(
            WalletDB::init(databasePath.string(), password, seed)
        );
        require(static_cast<bool>(database), "failed to create WalletDB test fixture");

        const TxID transactionId = {{1, 3, 4, 5, 65}};
        const ByteBuffer value = {9, 8, 7, 6};
        require(
            database->setTxParameter(
                transactionId,
                kDefaultSubTxID,
                TxParameterID::Message,
                value,
                false
            ),
            "failed to stage the transaction parameter"
        );

        database->FailCurrentTransactionForTests();
        bool commitFailed = false;
        try
        {
            database->FlushNow();
        }
        catch (const DatabaseException&)
        {
            commitFailed = true;
        }
        require(commitFailed, "fault injection did not fail COMMIT");

        ByteBuffer loaded;
        require(
            !database->getTxParameter(
                transactionId,
                kDefaultSubTxID,
                TxParameterID::Message,
                loaded
            ),
            "rolled-back transaction parameter remained visible through cache"
        );

        require(
            database->setTxParameter(
                transactionId,
                kDefaultSubTxID,
                TxParameterID::Message,
                value,
                false
            ),
            "same-operation retry was skipped after rollback"
        );
        database->FlushNow();
        loaded.clear();
        require(
            database->getTxParameter(
                transactionId,
                kDefaultSubTxID,
                TxParameterID::Message,
                loaded
            ) && loaded == value,
            "same-operation retry was not durable"
        );

        auto network = std::make_shared<RecordingNetwork>();
        auto registrationWallet = std::make_shared<RegistrationTestWallet>(database);
        registrationWallet->SetNodeEndpoint(network);
        const TxID registrationId = {{8, 5, 3, 2, 1}};
        const ByteBuffer signedKernelState = {4, 2, 4, 2};
        require(
            database->setTxParameter(
                registrationId,
                kDefaultSubTxID,
                TxParameterID::Kernel,
                signedKernelState,
                false
            ),
            "failed to stage signed transaction state"
        );
        database->FailCurrentTransactionForTests();
        bool registrationCommitFailed = false;
        try
        {
            registrationWallet->submitForTest(registrationId);
        }
        catch (const DatabaseException&)
        {
            registrationCommitFailed = true;
        }
        require(registrationCommitFailed, "registration fence did not surface COMMIT failure");
        require(network->postedRequests == 0, "transaction crossed the network boundary before COMMIT");

        require(
            database->setTxParameter(
                registrationId,
                kDefaultSubTxID,
                TxParameterID::Kernel,
                signedKernelState,
                false
            ),
            "failed to restage signed transaction state"
        );
        registrationWallet->submitForTest(registrationId);
        require(network->postedRequests == 1, "durable transaction was not submitted");
        registrationWallet.reset();

        auto wallet = std::make_shared<Wallet>(database);
        proto::Body honestBody;
        honestBody.m_Body.m_Perishable = {0x01, 0x02, 0x00};
        honestBody.m_Body.m_Eternal = {0x00, 0x03, 0x04};
        proto::Body boundaryShiftedBody;
        boundaryShiftedBody.m_Body.m_Perishable = {0x01, 0x02};
        boundaryShiftedBody.m_Body.m_Eternal = {0x00, 0x00, 0x03, 0x04};
        require(
            wallet->BodyDigestForTests(honestBody) != wallet->BodyDigestForTests(boundaryShiftedBody),
            "recovery quorum digest did not authenticate the body buffer boundary"
        );

        proto::FlyClient::RequestBodyPack retainedResponse;
        retainedResponse.m_Res.m_Bodies.push_back(honestBody.m_Body);
        proto::FlyClient::NetworkStd::ClearRecoveryResponseForTests(retainedResponse);
        require(
            retainedResponse.m_Res.m_Bodies.empty(),
            "reassigned recovery request retained a previous peer response"
        );

        const auto checkpointLimit = wallet->ShieldedCheckpointLimitForTests();
        for (std::size_t index = 1; index <= checkpointLimit + 17; ++index)
            wallet->SaveShieldedCountCheckpointForTests(index, index);
        require(
            wallet->ShieldedCheckpointCountForTests() == checkpointLimit,
            "shielded rollback checkpoint history grew beyond its mobile bound"
        );

        wallet->StartBodyRequestsAt(100, 42, 100, true);
        const auto oldBranchGeneration = wallet->BodyRequestGenerationForTests();
        wallet->RestoreBodyStateAfterRollbackForTests(50);
        require(
            wallet->BodyRequestGenerationForTests() != oldBranchGeneration,
            "reorg did not invalidate old-branch recovery requests"
        );
        require(
            wallet->BodyRecognitionBoundaryForTests() == 51,
            "snapshot reorg did not lower recognition boundary to the replacement branch"
        );
        require(
            database->get_ShieldedOuts() == 0,
            "snapshot reorg did not reset the shielded index before count-only rescan"
        );

        wallet->StartBodyRequestsAt(1001, 42, 1001, true);
        wallet->RestoreBodyStateAfterRollbackForTests(999);
        require(
            wallet->BodyRecognitionBoundaryForTests() == 1000,
            "new-wallet reorg kept the obsolete tip boundary and would skip replacement receipts"
        );

        wallet->StartBodyRequestsAt(100, 42, 100, false);
        wallet->RestoreBodyStateAfterRollbackForTests(50);
        require(
            wallet->BodyRecognitionBoundaryForTests() == 100,
            "birthday restore incorrectly lowered the user-selected recognition boundary"
        );
        wallet.reset();

        database.reset();
        fs::remove(databasePath);
        fs::remove(databasePath.string() + "-journal");
        fs::remove(atomicCreationPath);
        fs::remove(atomicCreationPath.string() + "-journal");
        return 0;
    }
    catch (const std::exception& error)
    {
        std::cerr << error.what() << std::endl;
        fs::remove(databasePath);
        fs::remove(databasePath.string() + "-journal");
        fs::remove(atomicCreationPath);
        fs::remove(atomicCreationPath.string() + "-journal");
        return 1;
    }
}
