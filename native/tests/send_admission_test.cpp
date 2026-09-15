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
        session_.stop();
        wallet_.reset();
        session_.database_.reset();
        std::error_code ignored;
        std::filesystem::remove_all(directory_, ignored);
    }

    SendRecord prepare(const std::string& operation) {
        const auto preview = session_.previewMaterial(receiver_, 100'000, "admission fixture");
        checkAdmissionTest(preview == session_.previewMaterial(receiver_, 100'000, "admission fixture"),
            "Repeated unchanged preview was not stable");
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

    Json inventory() { return Json::parse(session_.sendOperations()); }
    Json recover() { return Json::parse(session_.recoverSendOperationsOnWalletThread(wallet_)); }

    std::shared_ptr<beam::wallet::WalletDB> database() {
        return std::dynamic_pointer_cast<beam::wallet::WalletDB>(session_.database_);
    }

    void raw(const std::string& key, const std::string& value) {
        setRawString(*session_.database_, key.c_str(), value);
        flushDatabase(session_.database_);
    }

    template <typename F>
    auto queuedCall(F&& call, bool loseResponse = false) {
        if (!session_.client_) {
            session_.client_ = std::make_shared<BridgeWalletClient>(
                session_, session_.rules_, session_.database_, "", session_.reactor_);
            session_.client_->walletForTests = wallet_;
            session_.snapshot_.phase = Phase::Ready;
        }
        session_.dropInvokeResponseForTests_ = loseResponse;
        session_.invokeTimeoutForTests_ = std::chrono::milliseconds(loseResponse ? 250 : 5'000);
        std::promise<void> queued;
        auto accepted = queued.get_future();
        session_.invokeQueuedForTests_ = [&] { queued.set_value(); };
        auto result = std::async(std::launch::async, std::forward<F>(call));
        checkAdmissionTest(accepted.wait_for(std::chrono::seconds(5)) == std::future_status::ready,
            "Native operation was not accepted by the owner queue");
        session_.client_->getAsync()->makeIWTCall([]() -> boost::any { return true; },
            [&](const boost::any&) { session_.reactor_->stop(); });
        session_.reactor_->run();
        result.wait();
        session_.invokeQueuedForTests_ = {};
        session_.dropInvokeResponseForTests_ = false;
        return result;
    }

    template <typename F>
    static void expectFailure(F&& call) {
        try { call(); } catch (const std::exception&) { return; }
        throw std::runtime_error("Expected deterministic send fault");
    }

    template <typename T>
    static void expectTimeout(std::future<T>& result) {
        bool timedOut = false;
        try { result.get(); } catch (const std::runtime_error& error) {
            timedOut = std::string(error.what()) == "Timed out waiting for Beam owner thread";
        }
        checkAdmissionTest(timedOut, "Lost accepted response did not reach the real owner-thread timeout");
    }

    void failBoundary(const std::string& boundary, bool commitFailure) {
        session_.sendBoundaryForTests_ = [&, boundary, commitFailure](const char* current) {
            if (boundary != current) return;
            if (commitFailure) database()->FailNextFlushForTests();
            else throw std::runtime_error("Synthetic lost response after send boundary");
        };
    }

    void clearFault() { session_.sendBoundaryForTests_ = {}; }

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
        if (operation.empty()) session_.database_->removeVarRaw(kActiveSendVar);
        else setRawString(*session_.database_, kActiveSendVar, operation);
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
        runInventoryRecovery();
        {
            SendAdmissionFixture f;
            const auto a = f.prepare("A");
            const auto raw = f.journal("A");
            checkAdmissionTest(f.prepare("A").txId == a.txId, "Idempotent prepare changed its recorded identity");
            bool changedRequestRejected = false;
            try {
                f.session_.prepareSendOnWalletThread("A", f.receiver_, 100'001, "admission fixture", a.previewVersion);
            } catch (const std::logic_error& error) {
                changedRequestRejected = std::string(error.what()) == "operationId is already bound to another send request";
            }
            checkAdmissionTest(changedRequestRejected && f.journal("A") == raw && f.starts() == 0,
                "Changed request reused or mutated an existing operation identity");
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

    static void runInventoryRecovery() {
        // Real v1 bytes outside the active marker are the inventory, including inactive rows.
        {
            SendAdmissionFixture f;
            const auto a = f.prepare("A");
            const auto raw = f.journal("A");
            f.marker("");
            const auto listed = f.inventory();
            checkAdmissionTest(listed.size() == 1 && listed[0]["transactionId"] == a.txId &&
                listed[0]["resolution"]["kind"] == "Prepared" && listed[0].count("receiver") == 0 &&
                listed[0].count("comment") == 0, "Inactive legacy send was hidden or exposed request material");
            checkAdmissionTest(f.journal("A") == raw && f.marker().empty() && f.starts() == 0,
                "Local stopped inventory mutated or broadcast a send");
            f.reopen();
            expectDeferred([&] { f.prepare("B"); });
            checkAdmissionTest(f.recover()[0]["transactionId"] == a.txId && f.starts() == 1,
                "Recovery did not retain inactive legacy identity");
            f.recover();
            f.reopen();
            f.recover();
            checkAdmissionTest(f.starts() == 1 && f.rowCount() == 1, "Repeated recovery duplicated Core StartTransaction");
        }
        {
            SendAdmissionFixture first;
            const auto a = first.prepare("same-confirmation");
            SendAdmissionFixture second;
            checkAdmissionTest(second.inventory().empty(), "Independent wallet inherited another inventory");
            const auto b = second.prepare("same-confirmation");
            second.recover();
            checkAdmissionTest(a.txId != b.txId && first.starts() == 0 && second.starts() == 1,
                "Independent wallet admission or transaction identity was shared");
        }
        for (const auto state : {SendState::Prepared, SendState::Committing, SendState::Submitted}) {
            SendAdmissionFixture f;
            const auto a = f.prepare("A");
            f.setState("A", state);
            f.marker("");
            auto other = a;
            other.operationId = "B";
            other.txId = txIdString(beam::wallet::GenerateTxID());
            saveSendRecord(*f.session_.database_, other);
            flushDatabase(f.session_.database_);
            const auto before = f.inventory();
            expectDeferred([&] { f.recover(); });
            checkAdmissionTest(f.inventory() == before && f.starts() == 0 && f.rowCount() == 0,
                "Conflicting legacy records were partially recovered");
        }
        for (const auto state : {SendState::Prepared, SendState::Committing}) {
            SendAdmissionFixture f;
            const auto a = f.prepare("A");
            f.setState("A", state);
            f.reopen();
            f.recover();
            checkAdmissionTest(f.starts() == 1 && f.hasRow(a), "Missing-row retry did not use the recorded TxID");
        }
        {
            SendAdmissionFixture f;
            f.prepare("A");
            f.setState("A", SendState::Submitted);
            checkAdmissionTest(f.recover()[0]["resolution"]["kind"] == "Indeterminate" && f.starts() == 0,
                "Submitted without Core evidence was replayed");
            expectDeferred([&] { f.prepare("B"); });
        }
        // The invalid last record must be discovered before reconciling valid earlier records.
        for (int corruption = 0; corruption != 6; ++corruption) {
            SendAdmissionFixture f;
            const auto a = f.prepare("A");
            const auto before = f.journal("A");
            auto bad = Json::parse(before);
            bad["operationId"] = "Z";
            bad["txId"] = txIdString(beam::wallet::GenerateTxID());
            if (corruption == 1) bad["state"] = "future-state";
            if (corruption == 2) bad["operationId"] = "wrong-key";
            if (corruption == 3) bad["txId"] = "broken";
            if (corruption == 4) bad["amount"] = 123;
            const auto key = corruption == 5 ? "beam.sdk.kmp.send.v2.Z" : sendRecordKey("Z");
            const auto bytes = corruption == 0 ? "{ private-receiver-secret" : bad.dump();
            f.raw(key, bytes);
            for (auto action : {0, 1, 2}) {
                bool sanitized = false;
                try {
                    if (action == 0) f.inventory();
                    if (action == 1) f.recover();
                    if (action == 2) f.prepare("B");
                } catch (const std::runtime_error& error) {
                    sanitized = std::string(error.what()) == "Durable Beam send inventory is invalid or unsupported";
                }
                checkAdmissionTest(sanitized, "Invalid record did not fail with a sanitized inventory error");
            }
            checkAdmissionTest(f.journal("A") == before && f.starts() == 0 && !f.hasRow(a),
                "Malformed later record allowed partial recovery");
            std::string retained;
            checkAdmissionTest(getRawString(*f.session_.database_, key.c_str(), retained) && retained == bytes,
                "Invalid record was modified or deleted");
        }
        for (int mismatch = 0; mismatch != 3; ++mismatch) {
            SendAdmissionFixture f;
            const auto a = f.prepare("A");
            TxDescription tx(parseTxId(a.txId), TxType::PushTransaction);
            tx.m_sender = mismatch != 0;
            tx.m_amount = a.amount + (mismatch == 1 ? 1 : 0);
            tx.m_fee = a.fee + (mismatch == 2 ? 1 : 0);
            tx.m_assetId = Asset::s_BeamID;
            f.session_.database_->saveTx(tx);
            flushDatabase(f.session_.database_);
            expectFailure([&] { f.inventory(); });
            expectFailure([&] { f.recover(); });
            checkAdmissionTest(f.starts() == 0, "Mismatched Core evidence was recreated");
        }
        {
            SendAdmissionFixture f;
            const auto a = f.completedA();
            const auto b = f.prepare("B");
            checkAdmissionTest(f.inventory()[0]["resolution"]["kind"] == "Terminal", "Terminal record was not retained");
            f.rollback("A");
            checkAdmissionTest(f.inventory()[0]["resolution"]["kind"] == "Submitted", "Inventory cached terminal over reorg evidence");
            expectDeferred([&] { f.recover(); });
            checkAdmissionTest(f.starts() == 1 && f.hasRow(a) && !f.hasRow(b), "Reorg recovery started a competing send");
        }
        // Actual SQLite failures at the two prepare writes must not survive same-session reads or reopen.
        for (const auto key : {sendRecordKey("A"), std::string(kActiveSendVar)}) {
            SendAdmissionFixture f;
            f.database()->FailVariableWritesForTests(key);
            expectFailure([&] { f.prepare("A"); });
            checkAdmissionTest(f.inventory().empty() && f.marker().empty(), "Failed prepare left same-session journal evidence");
            f.database()->FailVariableWritesForTests("");
            f.reopen();
            checkAdmissionTest(f.inventory().empty() && f.starts() == 0, "Failed prepare became durable on reopen");
        }
        for (const auto boundary : {"prepare-record", "prepare-marker"}) {
            SendAdmissionFixture f;
            f.failBoundary(boundary, true);
            expectFailure([&] { f.prepare("A"); });
            f.clearFault();
            checkAdmissionTest(f.inventory().empty() && f.marker().empty(), "Prepare COMMIT failure leaked same-session state");
            f.reopen();
            checkAdmissionTest(f.inventory().empty(), "Prepare COMMIT failure survived reopen");
        }
        for (const auto boundary : {"committing-record", "core-start", "submitted-record"}) {
            SendAdmissionFixture f;
            const auto a = f.prepare("A");
            f.failBoundary(boundary, true);
            expectFailure([&] { f.commit("A"); });
            f.clearFault();
            const auto before = f.inventory();
            const bool coreDurable = std::string(boundary) == "submitted-record";
            checkAdmissionTest(f.hasRow(a) == coreDurable, "Failed durability fence exposed the wrong Core state");
            f.reopen();
            checkAdmissionTest(f.inventory() == before, "Same-session failed-fence state differed from reopened state");
            const auto starts = f.starts();
            f.recover();
            checkAdmissionTest(f.hasRow(a) && f.starts() == starts + (coreDurable ? 0 : 1),
                "Fence retry lost identity or recreated a durable Core transaction");
            f.recover();
            checkAdmissionTest(f.rowCount() == 1, "Fence retry duplicated the payment");
        }
        for (const auto boundary : {"prepared", "committing", "core-created", "submitted"}) {
            SendAdmissionFixture f;
            if (std::string(boundary) != "prepared") f.prepare("A");
            f.failBoundary(boundary, false);
            expectFailure([&] {
                if (std::string(boundary) == "prepared") f.prepare("A");
                else f.commit("A");
            });
            f.clearFault();
            const auto a = f.record("A");
            f.reopen();
            checkAdmissionTest(f.inventory()[0]["transactionId"] == a.txId, "Lost response lost durable identity");
            f.recover();
            f.recover();
            checkAdmissionTest(f.starts() == 1 && f.rowCount() == 1, "Lost-response retry duplicated StartTransaction");
        }
        for (bool coreCreated : {false, true}) {
            SendAdmissionFixture f;
            const auto a = f.prepare("A");
            if (coreCreated) {
                f.session_.sendBoundaryForTests_ = [&](const char* boundary) {
                    if (std::string(boundary) == "core-created") f.database()->FailVariableWritesForTests(sendRecordKey("A"));
                };
            } else f.database()->FailVariableWritesForTests(sendRecordKey("A"));
            expectFailure([&] { f.commit("A"); });
            f.clearFault();
            const auto before = f.inventory();
            checkAdmissionTest(f.hasRow(a) == coreCreated, "Failed commit record write lost its Core fence");
            f.database()->FailVariableWritesForTests("");
            f.reopen();
            checkAdmissionTest(f.inventory() == before, "Failed commit record write survived only in memory");
            f.recover();
            checkAdmissionTest(f.starts() == 1 && f.rowCount() == 1, "Failed commit record write caused duplicate creation");
        }
        for (const auto boundary : {"abort-record", "abort-marker"}) {
            SendAdmissionFixture f;
            f.prepare("A");
            const auto before = f.inventory();
            f.failBoundary(boundary, true);
            expectFailure([&] { f.session_.abortPrepared("A"); });
            f.clearFault();
            checkAdmissionTest(f.inventory() == before && f.marker() == "A", "Failed abort fence leaked partial removal");
            f.reopen();
            checkAdmissionTest(f.inventory() == before && f.marker() == "A", "Failed abort fence became durable");
        }
        for (const auto key : {sendRecordKey("A"), std::string(kActiveSendVar)}) {
            SendAdmissionFixture f;
            f.prepare("A");
            const auto before = f.inventory();
            f.database()->FailVariableWritesForTests(key);
            expectFailure([&] { f.session_.abortPrepared("A"); });
            checkAdmissionTest(f.inventory() == before && f.marker() == "A", "Failed abort partially deleted journal/marker");
            f.database()->FailVariableWritesForTests("");
            f.reopen();
            checkAdmissionTest(f.inventory() == before && f.session_.abortPrepared("A") && f.inventory().empty(),
                "Abort rollback or retry was not durable");
        }
        {
            SendAdmissionFixture f;
            f.prepare("A");
            f.commit("A");
            f.setState("A", SendState::Prepared);
            checkAdmissionTest(!f.session_.abortPrepared("A") && f.inventory().size() == 1,
                "Abort removed Prepared evidence for an already started Core transaction");
        }
        // Accepted public prepare and commit execute before their response is intentionally dropped.
        {
            SendAdmissionFixture f;
            const auto preview = f.session_.previewMaterial(f.receiver_, 100'000, "queued fixture");
            auto prepareResult = f.queuedCall([&] {
                return f.session_.prepareSend("A", f.receiver_, 100'000, "queued fixture", preview.first);
            }, true);
            expectTimeout(prepareResult);
            const auto listed = Json::parse(f.queuedCall([&] { return f.session_.sendOperations(); }).get());
            checkAdmissionTest(listed.size() == 1 && listed[0]["resolution"]["kind"] == "Prepared" && f.starts() == 0,
                "Accepted prepare with lost response was not discoverable");
            const auto a = f.record("A");
            auto commitResult = f.queuedCall([&] { return f.session_.commitSend("A"); }, true);
            expectTimeout(commitResult);
            const auto recovered = Json::parse(f.queuedCall([&] { return f.session_.recoverSendOperations(); }).get());
            checkAdmissionTest(recovered[0]["transactionId"] == a.txId && f.starts() == 1 && f.hasRow(a),
                "Accepted commit with lost response was duplicated by public retry");
            f.session_.stop();
            f.wallet_.reset();
            f.session_.close();
            expectFailure([&] { f.session_.sendOperations(); });
            expectFailure([&] { f.session_.recoverSendOperations(); });
            checkAdmissionTest(!f.session_.client_ && !f.session_.database_ && !f.session_.reactor_,
                "Completed close retained native work ownership");
        }
        // Both FIFO orderings of accepted abort/recovery requests are safe on the owner queue.
        for (bool abortFirst : {false, true}) {
            SendAdmissionFixture f;
            f.prepare("A");
            f.queuedCall([&] { return f.session_.sendOperations(); }).get();
            std::promise<void> firstQueued, secondQueued;
            auto firstAccepted = firstQueued.get_future();
            auto secondAccepted = secondQueued.get_future();
            f.session_.invokeQueuedForTests_ = [&] { firstQueued.set_value(); };
            auto abort = [&] { return f.session_.abortPrepared("A"); };
            auto recovery = [&] { f.session_.recoverSendOperations(); return false; };
            auto first = std::async(std::launch::async, [&] { return abortFirst ? abort() : recovery(); });
            firstAccepted.wait();
            f.session_.invokeQueuedForTests_ = [&] { secondQueued.set_value(); };
            auto second = std::async(std::launch::async, [&] { return abortFirst ? recovery() : abort(); });
            secondAccepted.wait();
            f.session_.client_->getAsync()->makeIWTCall([]() -> boost::any { return true; },
                [&](const boost::any&) { f.session_.reactor_->stop(); });
            f.session_.reactor_->run();
            const auto firstResult = first.get();
            const auto secondResult = second.get();
            f.session_.invokeQueuedForTests_ = {};
            f.session_.stop();
            checkAdmissionTest(abortFirst ? firstResult && f.starts() == 0 && f.inventory().empty() :
                !secondResult && f.starts() == 1 && f.inventory().size() == 1,
                "Abort/recovery race lost started transaction evidence or broadcast an aborted operation");
        }
        {
            SendAdmissionFixture f;
            f.completedA();
            f.queuedCall([&] { return f.session_.sendOperations(); }).get();
            std::promise<void> firstQueued, secondQueued;
            auto firstAccepted = firstQueued.get_future();
            auto secondAccepted = secondQueued.get_future();
            f.session_.invokeQueuedForTests_ = [&] { firstQueued.set_value(); };
            auto first = std::async(std::launch::async, [&] { return f.session_.sendOperations(); });
            firstAccepted.wait();
            f.session_.client_->getAsync()->makeIWTCall([&]() -> boost::any {
                f.rollback("A");
                return true;
            }, [](const boost::any&) {});
            f.session_.invokeQueuedForTests_ = [&] { secondQueued.set_value(); };
            auto second = std::async(std::launch::async, [&] { return f.session_.sendOperations(); });
            secondAccepted.wait();
            f.session_.client_->getAsync()->makeIWTCall([]() -> boost::any { return true; },
                [&](const boost::any&) { f.session_.reactor_->stop(); });
            f.session_.reactor_->run();
            const auto before = Json::parse(first.get());
            const auto after = Json::parse(second.get());
            f.session_.invokeQueuedForTests_ = {};
            checkAdmissionTest(before[0]["resolution"]["kind"] == "Terminal" &&
                after[0]["resolution"]["kind"] == "Submitted" &&
                before[0]["transactionId"] == after[0]["transactionId"] && f.starts() == 1,
                "Inventory requests concurrent with Core rollback did not preserve serial current evidence");
        }
        std::cout << "SEND_OPERATION_INVENTORY_RECOVERY_OK\n";
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

    static void throwFromMalformedInventory() {
        SendAdmissionFixture f;
        f.prepare("A");
        f.raw(sendRecordKey("Z"), "{ private-receiver-secret");
        f.recover();
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

extern "C" JNIEXPORT void JNICALL
Java_cash_p_beam_internal_SendAdmissionNativeTest_malformedInventoryFromNative(JNIEnv* env, jobject) {
    jniCall(env, [&] { SendAdmissionFixture::throwFromMalformedInventory(); return true; }, false);
}
#endif
