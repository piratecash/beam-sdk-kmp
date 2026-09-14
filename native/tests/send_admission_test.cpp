// Compile the bridge into this test-only target to exercise its private owner-thread
// bodies and JNI exception encoder without exporting test controls in the SDK API.
#include "../src/beam_jni.cpp"

#include <iostream>

namespace {

void checkAdmissionTest(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

template <typename F>
void expectDeferred(F&& call) {
    try {
        call();
    } catch (const SendAdmissionDeferred&) {
        return;
    }
    throw std::runtime_error("Expected typed SendAdmissionDeferred");
}

class SendAdmissionFixture {
public:
    SendAdmissionFixture()
        : directory_(std::filesystem::temp_directory_path() /
              ("beam-send-admission-" + txIdString(beam::wallet::GenerateTxID()))),
          session_(1, directory_.string()), rulesScope_(session_.rules_),
          reactorScope_(*session_.reactor_) {
        std::filesystem::create_directory(directory_);
        ECC::NoLeak<ECC::uintBig> seed;
        seed.V = 8675309UL; // Synthetic fixture only; never opens an existing wallet.
        session_.database_ = beam::wallet::WalletDB::init(path(), password(), seed, false);
        checkAdmissionTest(bool(session_.database_), "Cannot initialize test WalletDB");
        for (int i = 0; i < 8; ++i) {
            beam::wallet::Coin coin(10'000'000);
            coin.m_confirmHeight = 0;
            coin.m_maturity = 0;
            session_.database_->storeCoin(coin);
        }
        receiver_ = beam::wallet::GenerateTokenDefaultAddr(TokenType::Offline, session_.database_, 1);
        flushDatabase(session_.database_);
        openEngine();
    }

    ~SendAdmissionFixture() {
        wallet_.reset();
        session_.database_.reset();
        std::error_code ignored;
        std::filesystem::remove_all(directory_, ignored);
    }

    SendRecord prepare(const std::string& operation) {
        const auto preview = session_.previewMaterial(receiver_, 100'000, "admission fixture");
        const auto result = Json::parse(session_.prepareSendOnWalletThread(
            operation, receiver_, 100'000, "admission fixture", preview.first));
        auto saved = record(operation);
        checkAdmissionTest(result.at("transactionId") == saved.txId, "Prepare changed TxID");
        return saved;
    }

    Json commit(const std::string& operation) {
        return Json::parse(session_.commitSendOnWalletThread(operation, wallet_));
    }

    Json commitPublicAcrossStop(const std::string& operation) {
        // Pump the real client queue ourselves, without starting a wallet engine or
        // installing a network endpoint. Existing rows must reconcile even then.
        session_.client_ = std::make_shared<BridgeWalletClient>(
            session_, session_.rules_, session_.database_, "", session_.reactor_);
        std::weak_ptr<BridgeWalletClient> captured = session_.client_;
        std::promise<void> queued;
        auto queuedFuture = queued.get_future();
        session_.invokeQueuedForTests_ = [&] { queued.set_value(); };
        auto result = std::async(std::launch::async, [&] { return session_.commitSend(operation); });
        checkAdmissionTest(queuedFuture.wait_for(std::chrono::seconds(5)) == std::future_status::ready,
            "Public commit did not reach the client queue");

        session_.stop();
        checkAdmissionTest(!session_.client_, "Stop did not detach the session client");
        auto retainedClient = captured.lock();
        checkAdmissionTest(bool(retainedClient), "Queued commit lost its captured client");
        checkAdmissionTest(!retainedClient->getWallet(), "Queue fixture unexpectedly started a wallet engine");
        // FIFO sentinel exits only after the public commit callback. Database work
        // stays on this fixture's owner thread, including the stopped-client window.
        retainedClient->getAsync()->makeIWTCall(
            []() -> boost::any { return true; },
            [&](const boost::any&) { session_.reactor_->stop(); });
        // Match stop() retaining its local client until owner-thread work finishes.
        session_.reactor_->run();
        auto resolution = Json::parse(result.get());
        session_.invokeQueuedForTests_ = {};
        return resolution;
    }

    Json resolve(const std::string& operation) {
        auto saved = record(operation);
        return session_.resolveSendOnWalletThread(saved);
    }

    SendRecord record(const std::string& operation) const {
        SendRecord saved;
        checkAdmissionTest(loadSendRecord(*session_.database_, operation, saved), "Missing send journal");
        return saved;
    }

    std::string journal(const std::string& operation) const {
        std::string raw;
        checkAdmissionTest(getRawString(*session_.database_, sendRecordKey(operation).c_str(), raw), "Missing raw journal");
        return raw;
    }

    std::string marker() const {
        std::string result;
        getRawString(*session_.database_, kActiveSendVar, result);
        return result;
    }

    void marker(const std::string& operation) {
        setRawString(*session_.database_, kActiveSendVar, operation);
        flushDatabase(session_.database_);
    }

    void setState(const std::string& operation, SendState state) {
        auto saved = record(operation);
        saved.state = state;
        saveSendRecord(*session_.database_, saved);
        flushDatabase(session_.database_);
    }

    void status(const std::string& operation, TxStatus value) {
        const auto id = parseTxId(record(operation).txId);
        beam::wallet::storage::setTxParameter(*session_.database_, id, TxParameterID::Status, value, true);
        if (value == TxStatus::Completed) {
            beam::wallet::storage::setTxParameter(*session_.database_, id, TxParameterID::KernelProofHeight, Height(100), true);
        }
        flushDatabase(session_.database_);
    }

    void rollback(const std::string& operation) {
        const auto id = parseTxId(record(operation).txId);
        beam::wallet::BaseTransaction::Creator::Ptr creator =
            std::make_shared<beam::wallet::lelantus::PushTransaction::Creator>([this] { return session_.database_; });
        const auto tx = creator->Create(beam::wallet::BaseTransaction::TxContext(*wallet_, *wallet_, id));
        checkAdmissionTest(tx->Rollback(99), "Core rollback did not reactivate completed send");
        flushDatabase(session_.database_);
        checkAdmissionTest(session_.database_->getTx(id)->m_status == TxStatus::Registering,
            "Core did not change Completed to Registering");
        checkAdmissionTest(session_.database_->getTx(id)->m_txId == id, "Rollback changed TxID");
    }

    SendRecord completedA() {
        const auto a = prepare("A");
        commit("A"); // Actual Core StartTransaction, with no network endpoint installed.
        status("A", TxStatus::Completed);
        checkAdmissionTest(resolve("A").at("kind") == "Terminal", "A did not resolve terminal");
        checkAdmissionTest(marker().empty(), "Terminal resolve did not clear active marker");
        return a;
    }

    void reopen() {
        wallet_.reset();
        flushDatabase(session_.database_);
        session_.database_.reset();
        session_.database_ = beam::wallet::WalletDB::open(path(), password());
        checkAdmissionTest(bool(session_.database_), "Cannot reopen fixture WalletDB");
        openEngine();
    }

    unsigned starts() const { return session_.startTransactionCallsForTests_; }
    bool hasRow(const SendRecord& saved) const { return bool(session_.database_->getTx(parseTxId(saved.txId))); }
    std::size_t rowCount() const { return session_.database_->getTxHistory(TxType::ALL).size(); }

    void assertDeferredUnchanged(const SendRecord& b) {
        const auto raw = journal(b.operationId);
        const auto active = marker();
        const auto count = rowCount();
        const auto calls = starts();
        expectDeferred([&] { commit(b.operationId); });
        checkAdmissionTest(journal(b.operationId) == raw, "Deferred commit changed durable journal");
        checkAdmissionTest(marker() == active, "Deferred commit changed active marker");
        checkAdmissionTest(!hasRow(b) && rowCount() == count, "Deferred commit created a Core row");
        checkAdmissionTest(starts() == calls, "Deferred commit called StartTransaction");
    }

    static void run() {
        {
            SendAdmissionFixture f;
            const auto a = f.prepare("A");
            const auto raw = f.journal("A");
            const auto active = f.marker();
            f.session_.client_ = std::make_shared<BridgeWalletClient>(
                f.session_, f.session_.rules_, f.session_.database_, "", f.session_.reactor_);
            std::weak_ptr<BridgeWalletClient> captured = f.session_.client_;
            std::promise<void> queued;
            auto queuedFuture = queued.get_future();
            f.session_.invokeQueuedForTests_ = [&] { queued.set_value(); };
            auto result = std::async(std::launch::async, [&] { return f.session_.commitSend("A"); });
            checkAdmissionTest(queuedFuture.wait_for(std::chrono::seconds(5)) == std::future_status::ready,
                "Public commit did not queue before timeout test");
            f.session_.stop();
            checkAdmissionTest(!f.session_.client_ && !captured.expired(),
                "Invoking stack did not retain the detached client while waiting");
            // Do not pump the queue: shutdown may leave this callback undrained.
            // Exercise the real timeout so no test hook changes client ownership.
            bool timedOut = false;
            try {
                result.get();
            } catch (const std::runtime_error& error) {
                timedOut = std::string(error.what()) == "Timed out waiting for Beam owner thread";
            }
            f.session_.invokeQueuedForTests_ = {};
            checkAdmissionTest(timedOut, "Undrained public commit did not time out");
            checkAdmissionTest(captured.expired(), "Undrained callback retained the stopped client after timeout");
            checkAdmissionTest(f.journal("A") == raw && f.marker() == active &&
                !f.hasRow(a) && f.rowCount() == 0 && f.starts() == 0,
                "Undrained public commit mutated send state or started a transaction");
        }
        for (const auto state : {SendState::Committing, SendState::Submitted}) {
            for (const auto status : {TxStatus::Registering, TxStatus::Completed,
                                      TxStatus::Failed, TxStatus::Canceled}) {
                SendAdmissionFixture f;
                const auto a = f.completedA();
                f.status("A", status);
                f.setState("A", state);
                f.marker("A");
                f.wallet_.reset();
                const auto result = f.commitPublicAcrossStop("A");
                checkAdmissionTest(result.at("transactionId") == a.txId,
                    "Public commit/stop changed existing transaction identity");
                checkAdmissionTest(result.at("kind") ==
                    (isTerminal(status) ? "Terminal" : "Submitted"),
                    "Public commit/stop lost existing-row reconciliation");
                checkAdmissionTest(f.record("A").state == SendState::Submitted &&
                    f.hasRow(a) && f.rowCount() == 1 && f.starts() == 1,
                    "Public commit/stop failed reconciliation or duplicated the send");
                checkAdmissionTest(f.marker() == (isTerminal(status) ? "" : "A"),
                    "Public commit/stop lost terminal marker reconciliation");
            }
        }
        {
            SendAdmissionFixture f;
            const auto a = f.completedA();
            f.rollback("A");
            expectDeferred([&] { f.prepare("B"); });
            checkAdmissionTest(f.marker().empty() && f.rowCount() == 1 && f.starts() == 1,
                "Rejected prepare mutated history/marker or started a transaction");
            SendRecord absent;
            checkAdmissionTest(!loadSendRecord(*f.session_.database_, "B", absent), "Rejected prepare wrote a journal");
            checkAdmissionTest(f.prepare("A").txId == a.txId, "Idempotent prepare changed A identity");
            checkAdmissionTest(f.commit("A").at("kind") == "Submitted" && f.starts() == 1,
                "A continuation was blocked or duplicated");
        }
        for (const auto state : {SendState::Prepared, SendState::Committing}) {
            SendAdmissionFixture f;
            const auto a = f.completedA();
            const auto b = f.prepare("B");
            f.setState("B", state); // Crash boundary: Committing flush, no Core row yet.
            const auto raw = f.journal("B");
            f.rollback("A");
            f.assertDeferredUnchanged(b);
            f.reopen();
            checkAdmissionTest(f.journal("B") == raw, "Reopen changed deferred B");
            f.assertDeferredUnchanged(b);
            checkAdmissionTest(f.resolve("B").at("kind") ==
                (state == SendState::Prepared ? "Prepared" : "Indeterminate"), "Wrong deferred resolution");
            checkAdmissionTest(f.prepare("B").txId == b.txId, "Retry prepare changed B identity");
            checkAdmissionTest(f.commit("A").at("transactionId") == a.txId, "A continuation changed identity");
            f.status("A", TxStatus::Completed);
            f.resolve("A");
            checkAdmissionTest(f.marker() == "B", "Resolving A cleared B's marker");
            f.commit("B");
            checkAdmissionTest(f.hasRow(b) && f.rowCount() == 2 && f.starts() == 2, "Retry B did not start exactly once");
            f.commit("B");
            f.reopen();
            checkAdmissionTest(f.commit("B").at("transactionId") == b.txId && f.starts() == 2,
                "Commit retry/reopen duplicated B or changed TxID");
        }
        for (const auto state : {SendState::Committing, SendState::Submitted}) {
            SendAdmissionFixture f;
            const auto a = f.completedA();
            const auto b = f.prepare("B");
            f.commit("B");
            f.setState("B", state); // Also cover crash after durable Core row, before Submitted.
            f.rollback("A");
            f.reopen();
            checkAdmissionTest(f.commit("B").at("transactionId") == b.txId, "Existing B did not reconcile");
            checkAdmissionTest(f.commit("A").at("transactionId") == a.txId, "Existing A did not reconcile");
            expectDeferred([&] { f.prepare("C"); });
            checkAdmissionTest(f.hasRow(a) && f.hasRow(b) && f.rowCount() == 2 && f.starts() == 2,
                "Existing A/B were aborted, recreated, or duplicated");
        }
        for (const auto terminal : {TxStatus::Completed, TxStatus::Failed, TxStatus::Canceled}) {
            SendAdmissionFixture f;
            f.completedA();
            f.status("A", terminal);
            f.marker("A"); // Terminal row is authoritative even before resolve cleared its marker.
            const auto b = f.prepare("B");
            f.commit("B");
            checkAdmissionTest(f.hasRow(b) && f.starts() == 2, "Terminal A blocked normal B admission");
        }
        {
            SendAdmissionFixture f;
            f.marker("missing-journal");
            expectDeferred([&] { f.prepare("B"); });
            checkAdmissionTest(f.marker() == "missing-journal", "Indeterminate marker was silently cleared");
        }
        {
            SendAdmissionFixture f;
            const auto a = f.prepare("A");
            expectDeferred([&] { f.prepare("B"); });
            checkAdmissionTest(!f.hasRow(a) && f.starts() == 0, "Prepared A isolation failed");
        }
        // History must include every transaction type and fail closed on unknown status.
        for (const auto status : {TxStatus::Pending, TxStatus::InProgress, TxStatus::Registering,
                                 TxStatus::Confirming, static_cast<TxStatus>(999)}) {
            SendAdmissionFixture f;
            TxDescription tx(beam::wallet::GenerateTxID(), TxType::PushTransaction);
            tx.m_status = status;
            f.session_.database_->saveTx(tx);
            flushDatabase(f.session_.database_);
            expectDeferred([&] { f.prepare("B"); });
        }
        {
            SendAdmissionFixture f;
            TxDescription incoming(beam::wallet::GenerateTxID());
            incoming.m_sender = false;
            incoming.m_status = TxStatus::Registering;
            f.session_.database_->saveTx(incoming);
            const auto b = f.prepare("B");
            f.commit("B");
            checkAdmissionTest(f.hasRow(b), "Incoming history incorrectly blocked send");
        }
    }

    static void throwFromAdmission(bool commit) {
        SendAdmissionFixture f;
        f.completedA();
        if (commit) f.prepare("B");
        f.rollback("A");
        if (commit) f.commit("B");
        else f.prepare("B");
        throw std::runtime_error("Admission unexpectedly succeeded");
    }

private:
    std::string path() const { return (directory_ / "wallet.db").string(); }
    static beam::SecString password() { return beam::SecString(std::string("synthetic-admission-test")); }
    void openEngine() {
        wallet_ = std::make_shared<Wallet>(session_.database_);
        wallet_->RegisterTransactionType(TxType::PushTransaction,
            std::make_shared<beam::wallet::lelantus::PushTransaction::Creator>([this] { return session_.database_; }));
    }

    std::filesystem::path directory_;
    Session session_;
    Rules::Scope rulesScope_;
    beam::io::Reactor::Scope reactorScope_;
    Wallet::Ptr wallet_;
    std::string receiver_;
};

} // namespace

#ifdef BEAM_SDK_KMP_ADMISSION_TEST_MAIN
int main() {
    try {
        SendAdmissionFixture::run();
        std::cout << "Send admission native matrix passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
#else
extern "C" JNIEXPORT void JNICALL
Java_cash_p_beam_internal_SendAdmissionNativeTest_deferFromNative(JNIEnv* env, jobject, jboolean commit) {
    jniCall(env, [&] { SendAdmissionFixture::throwFromAdmission(commit == JNI_TRUE); return true; }, false);
}
#endif
