// Dedicated JNI fixture: compile with BEAM_SDK_KMP_TESTS and the bridge's
// include/link settings, then load it alongside the instrumented production bridge.
#include "../src/beam_jni.cpp"
#include "offline_record_fixture.h"

namespace {

class OfflineHistoryFixture {
public:
    static std::vector<std::uint8_t> key() { return std::vector<std::uint8_t>(32, 0x41); }

    static void seed(const std::string& directory, int restoreType, bool initialized, bool history) {
        Session session(1, directory, false, 0);
        std::string value;
        if (restoreType == 0) value = "100";
        if (restoreType == 1) value = "2024-01-01";
        if (restoreType == 2) value = Json({{"path", directory + "/recovery.bin"}}).dump();
        session.create(std::vector<std::uint8_t>(64, 0x31), key(), restoreType, value);
        Rules::Scope rules(session.rules_);
        beam::io::Reactor::Scope reactor(*session.reactor_);
        if (history) {
            auto older = transaction(1, 100, TxType::Simple);
            older.m_sender = true;
            session.database_->saveTx(older);
            beam::wallet::storage::setTxParameter(
                *session.database_, older.m_txId, TxParameterID::KernelProofHeight, Height(123), false);
            auto newer = transaction(2, 200, TxType::PushTransaction);
            session.database_->saveTx(newer);
            beam::wallet::ShieldedCoin coin;
            coin.m_CoinID = {};
            coin.m_CoinID.m_Value = newer.m_amount;
            coin.m_TxoID = 0;
            coin.m_createTxId = newer.m_txId;
            coin.m_confirmHeight = 456;
            session.database_->saveShieldedCoin(coin);
            auto asset = transaction(3, 300, TxType::Simple);
            asset.m_assetId = 1;
            session.database_->saveTx(asset);
        }
        beam::wallet::storage::setVar(*session.database_, kRestoreInitializedVar, initialized);
        flushDatabase(session.database_);
        require(Json::parse(session.transactions(0, 10)).empty(), "Create exposed seeded history");
        session.close();
    }

    // An interrupted offline signing that open() drops, or signed offline rows history hides until observed.
    static void seedOffline(const std::string& directory, bool interrupted) {
        Session session(1, directory, false, 0);
        session.create(std::vector<std::uint8_t>(64, 0x31), key(), -1, "");
        Rules::Scope rules(session.rules_);
        beam::io::Reactor::Scope reactor(*session.reactor_);
        auto& db = *session.database_;
        beam::wallet::WalletAddress address;
        db.createAddress(address);
        db.saveAddress(address);
        const auto receiver = beam::wallet::GeneratePublicToken(address, db, "");
        auto offline = [&](std::uint8_t id, Timestamp created, SendState state, Height observed) {
            auto row = transaction(id, created, TxType::PushTransaction);
            row.m_sender = true;
            row.m_status = TxStatus::Registering;
            db.saveTx(row);
            if (observed)
                beam::wallet::storage::setTxParameter(db, row.m_txId, TxParameterID::KernelProofHeight, observed, false);
            const auto record = syntheticOfflineRecord("offline-" + std::to_string(id), row.m_txId, receiver,
                row.m_amount, row.m_fee, state, session.requestDigest(receiver, row.m_amount, "", row.m_fee).second);
            saveSendRecord(db, record);
            return row.m_txId;
        };
        if (interrupted) {
            const auto id = offline(5, 500, SendState::Signing, 0);
            beam::wallet::Coin input(20'000'000);
            input.m_confirmHeight = 1;
            input.m_maturity = 1;
            input.m_spentTxId = id;
            db.storeCoin(input);
            beam::wallet::Coin change(9'000'000);
            change.m_createTxId = id;
            db.storeCoin(change);
        } else {
            offline(6, 600, SendState::Exported, 0);
            offline(7, 700, SendState::Exported, 77);
            db.saveTx(transaction(8, 800, TxType::Simple));
            setRawString(db, sendRecordKey("unreadable").c_str(), "{ damaged");
        }
        beam::wallet::storage::setVar(db, kRestoreInitializedVar, true);
        flushDatabase(session.database_);
        session.close();
    }

    static void seedContext(const std::string& directory) {
        seedContextWallet(directory, false);
    }

    static Height seedFundedContext(const std::string& directory) {
        return seedContextWallet(directory, true);
    }

private:
    static Height seedContextWallet(const std::string& directory, bool funded) {
        Session session(1, directory, false, 0);
        session.create(std::vector<std::uint8_t>(64, 0x31), key(), -1, "");
        Rules::Scope rules(session.rules_);
        beam::io::Reactor::Scope reactor(*session.reactor_);
        auto db = std::dynamic_pointer_cast<beam::wallet::WalletDB>(session.database_);
        beam::Block::SystemState::Full tip{};
        // Keep the history-only fixture unchanged. Funding uses supported Testnet rules,
        // after the one-sided forks, without overriding consensus or generating a chain.
        tip.m_Number.v = funded ? session.rules_.pForks[5].m_Height + 1000 : 500;
        tip.m_TimeStamp = beam::getTimestamp();
        if (funded) {
            const Height coinHeight = tip.get_Height() - 100;
            auto owner = db->get_MasterKdf();
            require(bool(owner), "funded fixture requires the actual wallet KDF");
            beam::CoinID coinId(10'000'000, 3000, beam::Key::Type::Regular);
            auto child = coinId.get_ChildKdf(owner);
            beam::Output output;
            ECC::Scalar::Native secret;
            output.Create(coinHeight, secret, *child, coinId, *owner, beam::Output::OpCode::Public);
            beam::wallet::Coin coin;
            require(output.Recover(coinHeight, *owner, coin.m_ID) &&
                output.VerifyRecovered(*child, coin.m_ID), "ordinary funding is not owned by this wallet");
            coin.m_confirmHeight = coinHeight;
            coin.m_maturity = output.get_MinMaturity(coinHeight);
            require(coin.m_maturity <= tip.get_Height(), "ordinary funding is immature");
            db->saveCoin(coin);
        }
        beam::HeightHash id;
        tip.get_ID(id);
        db->get_History().AddStates(&tip, 1);
        db->setSystemStateID(id);
        beam::wallet::RecognitionRecovery recovery;
        recovery.m_Phase = beam::wallet::RecognitionRecovery::Complete;
        recovery.m_Generation = 7;
        recovery.m_Target = recovery.m_Cursor = id;
        beam::wallet::storage::setBlobVar(*db, beam::wallet::RecognitionRecovery::Key, recovery);
        beam::wallet::storage::setVar(*db, kRestoreInitializedVar, true);
        db->FlushNow();
        {
            Wallet wallet(db);
            db->SetSelectionAllowed(true); // synthetic completed-sync boundary; no network
            struct Network : beam::proto::FlyClient::NetworkStd {
                using NetworkStd::NetworkStd;
                beam::proto::FlyClient::Request::Ptr pending;
                void PostRequestInternal(beam::proto::FlyClient::Request& r) override { pending = &r; }
            } network(wallet);
            {
            // Bootstrap/recovery invokes context notifications under Session's lifecycle lock.
            std::lock_guard<std::mutex> ownerLock(session.mutex_);
            session.offlineContext_->prepare(network);
            for (unsigned i = 0; i < 2; ++i) {
                auto request = std::move(network.pending);
                require(bool(request), "missing synthetic boundary request");
                request->m_ResponseCheckpoint = tip;
                request->m_RecoveryResponder = beam::io::Address::localhost().port(19001 + i);
                request->As<beam::proto::FlyClient::RequestShieldedOutputsAt>().m_Res.m_ShieldedOuts = 0;
                auto handler = request->m_pTrg;
                request->m_pTrg = nullptr;
                handler->OnComplete(*request);
            }
            }
            require(Json::parse(session.snapshotJson())["offlineSigning"]["phase"] == "Ready",
                "JNI snapshot did not expose durable context");
        }
        session.close();
        return tip.get_Height();
    }

public:
    static void matrix(const std::string& directory) {
        for (int type : {-1, 0, 1, 2, 3}) {
            auto path = directory + "/pending-" + std::to_string(type);
            seed(path, type, false, true);
            Session pending(1, path, false, 0);
            pending.open(key());
            require(Json::parse(pending.transactions(0, 10)).empty(), "Uninitialized restore leaked history");
            require(!pending.initialTransactionsLoaded_, "Pending restore marked history loaded");
        }
        auto emptyPath = directory + "/empty";
        seed(emptyPath, -1, true, false);
        Session empty(1, emptyPath, false, 0);
        empty.open(key());
        require(Json::parse(empty.transactions(0, 10)).empty(), "Empty wallet leaked history");

        auto path = directory + "/completed";
        seed(path, -1, true, true);
        for (int i = 0; i < 2; ++i) {
            Session session(1, path, false, 0);
            session.open(key());
            auto rows = Json::parse(session.transactions(0, 10));
            require(rows.size() == 2, "Reopen lost history or exposed non-BEAM asset");
            require(rows[0]["proofHeight"] == 456 && rows[1]["proofHeight"] == 123,
                "Proof height conversion differs from live history");
            require(rows[0]["createdAtEpochSeconds"] == 200, "History is not newest first");
            require(Json::parse(session.transactions(1, 1))[0] == rows[1], "Paging changed history");
            require(Json::parse(session.transactions(2, 1)).empty(), "Paging overran history");
            require(session.snapshot_.phase == Phase::Stopped && !session.initialStatusLoaded_,
                "Local history made balance ready");
            session.onConnection(false);
            session.onError("Synthetic offline start failure", true);
            session.stop();
            session.stop();
            require(Json::parse(session.transactions(0, 10)) == rows, "Offline/stop lost history");

            // Exercise the actual owner-thread callback without a network endpoint.
            Rules::Scope rules(session.rules_);
            beam::io::Reactor::Scope reactor(*session.reactor_);
            auto replacement = transaction(4, 400, TxType::Simple);
            session.onTransactions(ChangeAction::Reset, {replacement});
            require(Json::parse(session.transactions(0, 10)).size() == 1, "Reset appended stale rows");
            replacement.m_status = TxStatus::Canceled;
            session.onTransactions(ChangeAction::Updated, {replacement});
            require(Json::parse(session.transactions(0, 10))[0]["status"] == "Canceled", "Update lost status");
            session.onTransactions(ChangeAction::Added, {replacement});
            require(Json::parse(session.transactions(0, 10)).size() == 1, "Update duplicated history");
            session.onTransactions(ChangeAction::Removed, {replacement});
            require(Json::parse(session.transactions(0, 10)).empty(), "Remove retained history");
            session.onTransactions(ChangeAction::Reset, {});
            require(Json::parse(session.transactions(0, 10)).empty(), "Empty Reset retained history");
        }
        {
            auto interrupted = directory + "/offline-interrupted";
            seedOffline(interrupted, true);
            Session session(1, interrupted, false, 0);
            session.open(key());
            TxID id{};
            id.back() = 5;
            SendRecord record;
            require(!loadSendRecord(*session.database_, "offline-5", record) && !session.database_->getTx(id) &&
                Json::parse(session.transactions(0, 10)).empty(), "Open kept an interrupted offline signing");
            unsigned coins = 0;
            session.database_->visitCoins([&](const beam::wallet::Coin& c) {
                require(c.m_spentTxId != id && c.m_createTxId != id, "Open kept an interrupted reservation");
                ++coins;
                return true;
            });
            require(coins == 1, "Open did not drop the interrupted change output");
        }
        {
            auto signedRows = directory + "/offline-signed";
            seedOffline(signedRows, false);
            Session session(1, signedRows, false, 0);
            session.open(key()); // the unreadable record defeats the sweep, never the open
            auto rows = Json::parse(session.transactions(0, 10));
            require(rows.size() == 2 && rows[0]["createdAtEpochSeconds"] == 800 &&
                rows[1]["createdAtEpochSeconds"] == 700, "Unobserved offline send appeared in history");
            require(rows[1]["status"] == "Completed" && rows[1]["proofHeight"] == 77,
                "Observed offline send was not shown as Completed");
        }
        for (bool wrongNetwork : {false, true}) {
            Session rejected(wrongNetwork ? 0 : 1, path, false, 0);
            bool failed = false;
            try {
                rejected.open(wrongNetwork ? key() : std::vector<std::uint8_t>(32, 0x42));
            } catch (const std::exception&) {
                failed = true;
            }
            require(failed, "Wrong key/network was accepted");
            require(Json::parse(rejected.transactions(0, 10)).empty(), "Rejected open exposed history");
        }
    }

private:
    static void require(bool condition, const char* message) {
        if (!condition) throw std::runtime_error(message);
    }

    static TxDescription transaction(std::uint8_t id, Timestamp created, TxType type) {
        TxID txId{};
        txId.back() = id;
        TxDescription result(txId, type);
        result.m_amount = 100'000;
        result.m_fee = 100;
        result.m_assetId = Asset::s_BeamID;
        result.m_sender = false;
        result.m_createTime = created;
        result.m_status = TxStatus::Completed;
        return result;
    }
};

} // namespace

extern "C" JNIEXPORT void JNICALL
Java_cash_p_beam_internal_OfflineHistoryNativeTest_seedWallet(
    JNIEnv* env, jobject, jstring directory, jint restoreType, jboolean initialized, jboolean history
) {
    jniCall(env, [&] {
        OfflineHistoryFixture::seed(javaString(env, directory), restoreType, initialized, history);
        return true;
    }, false);
}

extern "C" JNIEXPORT void JNICALL
Java_cash_p_beam_internal_OfflineHistoryNativeTest_runNativeMatrix(JNIEnv* env, jobject, jstring directory) {
    jniCall(env, [&] { OfflineHistoryFixture::matrix(javaString(env, directory)); return true; }, false);
}

extern "C" JNIEXPORT void JNICALL
Java_cash_p_beam_internal_OfflineHistoryNativeTest_seedOfflineContext(JNIEnv* env, jobject, jstring directory) {
    jniCall(env, [&] { OfflineHistoryFixture::seedContext(javaString(env, directory)); return true; }, false);
}

extern "C" JNIEXPORT jlong JNICALL
Java_cash_p_beam_internal_OfflineHistoryNativeTest_seedFundedOfflineContext(JNIEnv* env, jobject, jstring directory) {
    return jniCall(env, [&] {
        return static_cast<jlong>(OfflineHistoryFixture::seedFundedContext(javaString(env, directory)));
    }, jlong(0));
}
