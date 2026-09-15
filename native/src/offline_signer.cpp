#include "offline_signer.h"
#include "wallet/transactions/lelantus/push_transaction.h"
#include "utility/io/timer.h"
#include <chrono>
#include <limits>

namespace beam::sdk {
namespace {
using namespace wallet;
constexpr Amount Limit = std::numeric_limits<int64_t>::max();
[[noreturn]] void missing() { throw SendError("CONTEXT_UNAVAILABLE", "Durable signing context is unavailable"); }
void checkedAdd(Amount& a, Amount b) {
    if (b > Limit || a > Limit - b) throw SendError("UNSUPPORTED", "Amount exceeds SDK range");
    a += b;
}
template<class T> ByteBuffer encode(const T& x) {
    Serializer s; s & x; ByteBuffer b; s.swap_buf(b); return b;
}
std::string hash(const ByteBuffer& b) {
    ECC::Hash::Value h;
    ECC::Hash::Processor() << Blob(b.data(), static_cast<uint32_t>(b.size())) >> h;
    return to_hex(h.m_pData, h.nBytes);
}
CoinsSelectionInfo exact(const std::shared_ptr<WalletDB>& db, Height h, Amount amount) {
    CoinsSelectionInfo q;
    q.m_requestedSum = amount;
    q.Calculate(h, db, true);
    Amount total = amount; checkedAdd(total, q.get_TotalFee());
    return q;
}
}

std::string walletSelectionVersion(const wallet::WalletDB& db) {
    // Hash relevant state, not an in-memory counter: restart, foreign reservations and reorg
    // must invalidate quotes too. No writes, selections, tokens or secrets in diagnostics.
    Serializer s;
    HeightHash tip{Zero, 0}; db.getSystemStateID(tip);
    s & tip & db.get_ShieldedOuts();
    ByteBuffer recovery; db.getBlob(wallet::RecognitionRecovery::Key, recovery); s & recovery;
    db.visitCoins([&](const wallet::Coin& c) {
        s & c.m_ID & c.m_status & c.m_maturity & c.m_confirmHeight & c.m_spentHeight
            & c.m_spentTxId & c.m_createTxId & c.m_OBSOLETTEsessionId;
        return true;
    });
    db.visitShieldedCoins([&](const wallet::ShieldedCoin& c) {
        s & c.m_CoinID & c.m_TxoID & c.m_Status & c.m_confirmHeight & c.m_spentHeight
            & c.m_spentTxId & c.m_createTxId;
        return true;
    });
    for (const auto& entry : db.getBlobsByPrefix("beam.sdk.kmp.send.v")) s & entry.first & entry.second;
    ByteBuffer b; s.swap_buf(b); return hash(b);
}

SendQuote quoteSend(const std::shared_ptr<wallet::WalletDB>& db, const std::string& receiver,
    Amount amount, bool maximum, const std::string& comment, const std::string& contextId) {
    using namespace wallet;
    SendQuote result;
    try { result.receiverType = parseToken(receiver).type; }
    catch (...) { throw SendError("INVALID_ADDRESS", "Invalid or unsupported one-sided receiver"); }
    if (!db->IsSelectionAllowed()) throw SendError("SEND_BUSY", "Wallet selection is not admitted");
    result.height = db->getCurrentHeight();
    result.rules = Rules::get().get_SignatureStr();
    result.contextId = contextId;
    const auto generation = walletSelectionVersion(*db);
    Amount upper = 0;
    db->visitCoins([&](const Coin& c) {
        if (!c.isAsset() && c.m_status == Coin::Available && !c.m_spentTxId) checkedAdd(upper, c.m_ID.m_Value);
        return true;
    });
    db->visitShieldedCoins([&](const ShieldedCoin& c) {
        if (!c.isAsset() && c.m_Status == ShieldedCoin::Available && !c.m_spentTxId) checkedAdd(upper, c.m_CoinID.m_Value);
        return true;
    });
    if (!maximum && (!amount || amount > Limit)) throw SendError("UNSUPPORTED", "Amount is outside SDK range");
    if (maximum) {
        // Calculate deliberately on the upper bound even when insufficient: Core caps the
        // shielded count and reports the actually selectable sum and actual input fees.
        if (!upper) throw SendError("INSUFFICIENT_FUNDS", "Insufficient Beam funds");
        const auto bound = exact(db, result.height, upper);
        amount = bound.get_NettoValue();
        if (!amount) throw SendError("INSUFFICIENT_FUNDS", "Insufficient Beam funds after Core fees");
    }
    std::set<Amount> tried;
    for (unsigned attempt = 0; attempt != 8; ++attempt) {
        if (!amount || !tried.insert(amount).second) break;
        auto q = exact(db, result.height, amount);
        if (q.m_isEnought) {
            result.selection = q;
            // This is exactly the first builder Balance request. Involuntary shielded fees
            // are charged by Core's selected inputs, not added again to the main kernel.
            db->selectCoins2(result.height, amount + q.m_explicitFee, Asset::s_BeamID,
                result.ordinary, result.shielded, Rules::get().Shielded.MaxIns, true);
            Amount selected = 0;
            for (const auto& c : result.ordinary) checkedAdd(selected, c.m_ID.m_Value);
            for (const auto& c : result.shielded) checkedAdd(selected, c.m_CoinID.m_Value);
            if (selected != q.m_selectedSumBeam ||
                result.shielded.size() * Transaction::FeeSettings::get(result.height).m_ShieldedInputTotal != q.m_involuntaryFee)
                throw SendError("UNSUPPORTED", "Core quote and builder selection disagree");
            if (result.ordinary.size() > kMaxCodecVector || result.shielded.size() + 1 > kMaxCodecVector)
                throw SendError("UNSUPPORTED", "Selected transaction exceeds export limits");
            result.remainder = upper - amount - q.get_TotalFee();
            if (generation != walletSelectionVersion(*db)) throw SendError("STALE_QUOTE", "Wallet changed during quote");
            Serializer material;
            material & receiver & amount & maximum & comment & contextId & result.rules & generation
                & q.m_explicitFee & q.get_TotalFee() & q.m_changeBeam;
            ByteBuffer identity; material.swap_buf(identity); result.version = hash(identity);
            return result;
        }
        if (!maximum) throw SendError("INSUFFICIENT_FUNDS", "Insufficient Beam funds including fee");
        const auto next = q.get_NettoValue();
        if (next >= amount) break;
        amount = next;
    }
    throw SendError("QUOTE_UNAVAILABLE", "Core Max quote did not converge within its bound");
}

SigningContext SigningContext::capture(const std::shared_ptr<wallet::WalletDB>& db,
    OfflineContext& cache, const std::string& expectedId) {
    SigningContext c;
    cache.load(); // read-only reload also recovers readiness after our own reservation notifications
    if (cache.state().phase != OfflineContext::Phase::Ready || cache.state().contextId != expectedId ||
        !db->get_History().get_Tip(c.tip) ||
        !wallet::storage::getBlobVar(*db, wallet::RecognitionRecovery::Key, c.recovery) ||
        c.recovery.m_Version != 1 || c.recovery.m_Phase != wallet::RecognitionRecovery::Complete ||
        c.recovery.m_RollbackPending) missing();
    c.id = expectedId; c.rules = Rules::get().get_SignatureStr(); c.count = db->get_ShieldedOuts();
    std::vector<OfflineContext::Range> ranges;
    for (const auto& coin : cache.coins()) {
        wallet::BaseTxBuilder::ShieldedWindow w;
        if (!wallet::BaseTxBuilder::SelectShieldedWindow(coin, c.count, w)) missing();
        ranges.push_back({w.m_Start, w.m_Count});
    }
    // Coverage is verified by owning the deduplicated snapshot of exactly these windows.
    // Neither this readiness check nor a quote ever expands a window into an item list.
    if (!ranges.empty() && !(c.windows = cache.snapshot(expectedId, std::move(ranges)))) missing();
    if (!c.current(*db)) missing();
    return c;
}

bool SigningContext::current(wallet::WalletDB& db) const {
    Block::SystemState::Full now;
    wallet::RecognitionRecovery r;
    return db.get_ShieldedOuts() == count && db.get_History().get_Tip(now) && encode(now) == encode(tip) &&
        rules == Rules::get().get_SignatureStr() &&
        wallet::storage::getBlobVar(db, wallet::RecognitionRecovery::Key, r) && encode(r) == encode(recovery);
}

namespace {
class Gateway final : public wallet::INegotiatorGateway {
public:
    std::shared_ptr<wallet::WalletDB> db;
    const SigningContext& context;
    std::atomic<bool>& cancelled;
    const std::function<void(const SignedMaterial&)>& persist;
    Amount totalFee, explicitFee, amount;
    bool done = false, failed = false, closed = false;
    std::exception_ptr error;
    int async = 0;
    Gateway(std::shared_ptr<wallet::WalletDB> d, const SigningContext& c, std::atomic<bool>& stop,
        const std::function<void(const SignedMaterial&)>& p, Amount f, Amount e, Amount a)
        : db(std::move(d)), context(c), cancelled(stop), persist(p), totalFee(f), explicitFee(e), amount(a) {}
    bool current() const { return !closed && !done && !failed && !cancelled.load() && context.current(*db); }
    void check() const { if (!current()) throw SendError("CONTEXT_UNAVAILABLE", "Offline signing scope is no longer current"); }
    [[noreturn]] void deny() { throw SendError("UNSUPPORTED", "Offline signer rejected a network operation"); }
    void OnAsyncStarted() override { ++async; }
    void OnAsyncFinished() override { --async; }
    void on_tx_completed(const TxID&) override { deny(); }
    void on_tx_failed(const TxID&) override { failed = true; }
    void register_tx(const TxID& id, const Transaction::Ptr& tx, const Merkle::Hash* parent, SubTxID sub) override {
        try {
            check();
            if (parent || sub != kDefaultSubTxID) deny();
            SignedMaterial m;
            m.bytes = encode(*tx);
            m.inspection = inspectTransaction(m.bytes, Rules::get(), context.rules);
            Merkle::Hash kernel;
            Amount savedAmount = 0, savedFee = 0;
            if (!storage::getTxParameter(*db, id, TxParameterID::KernelID, kernel) ||
                m.inspection.mainKernelId != to_hex(kernel.m_pData, kernel.nBytes) ||
                !storage::getTxParameter(*db, id, TxParameterID::Amount, savedAmount) || savedAmount != amount ||
                !storage::getTxParameter(*db, id, TxParameterID::Fee, savedFee) || savedFee != explicitFee)
                throw SendError("UNSUPPORTED", "Signed transaction does not agree with quote");
            Amount actualFee = 0;
            for (const auto& k : m.inspection.transaction->m_vKernels) {
                checkedAdd(actualFee, k->m_Fee);
                if (k->get_Subtype() == TxKernel::Subtype::Std && k->m_Fee != explicitFee)
                    throw SendError("UNSUPPORTED", "Main kernel fee does not agree with quote");
            }
            if (actualFee != totalFee) throw SendError("UNSUPPORTED", "Kernel fees do not agree with quote");
            std::vector<CoinID> ordinary;
            std::vector<IPrivateKeyKeeper2::ShieldedInput> shielded;
            storage::getTxParameter(*db, id, TxParameterID::InputCoins, ordinary);
            storage::getTxParameter(*db, id, TxParameterID::InputCoinsShielded, shielded);
            if (ordinary.empty() && shielded.empty()) deny();
            for (const auto& input : ordinary) {
                Coin c; c.m_ID = input;
                if (!db->findCoin(c) || !c.m_spentTxId || *c.m_spentTxId != id) deny();
            }
            for (const auto& input : shielded) {
                auto c = db->getShieldedCoin(input.m_Key);
                if (!c || !c->m_spentTxId || *c->m_spentTxId != id) deny();
            }
            m.inputs = encode(ordinary); m.shieldedInputs = encode(shielded);
            persist(m); // flush Signed + reservations before acknowledging local capture
            done = true;
        } catch (...) { error = std::current_exception(); failed = true; }
    }
    bool get_tip(Block::SystemState::Full& tip) const override { check(); tip = context.tip; return true; }
    void get_shielded_list(const TxID&, TxoID start, uint32_t count, ShieldedListCallback&& cb) override {
        check();
        // The snapshot never stores item lists: exactly one window is materialised per Core
        // request. Core's builder swaps this vector into its own handler and owns it for the
        // selected input until keykeeper completion.
        proto::ShieldedList response;
        if (!context.windows || !context.windows->materialize({start, count}, response)) missing();
        cb(start, count, response);
    }
    void get_UniqueVoucher(const WalletID& peer, const TxID&, boost::optional<ShieldedTxo::Voucher>& out) override {
        check(); out = db->grabVoucher(peer);
        if (!out) throw SendError("INVALID_ADDRESS", "Receiver has no unused local voucher");
    }
    void confirm_kernel(const TxID&, const Merkle::Hash&, SubTxID) override { deny(); }
    void confirm_kernel_ex(const Merkle::Hash&, IConfirmCallback::Ptr&&) override { deny(); }
    void confirm_asset(const TxID&, const PeerID&, SubTxID) override { deny(); }
    void confirm_asset(const TxID&, Asset::ID, SubTxID) override { deny(); }
    void get_kernel(const TxID&, const Merkle::Hash&, SubTxID) override { deny(); }
    void send_tx_params(const WalletID&, const SetTxParameter&) override { deny(); }
    void get_proof_shielded_output(const TxID&, const ECC::Point&, ProofShildedOutputCallback&&) override { deny(); }
    void UpdateOnNextTip(const TxID&) override { deny(); }
    void Listen(const WalletID&, const ECC::Scalar::Native&, IHandler*) override { deny(); }
    void Unlisten(const WalletID&, IHandler*) override { deny(); }
    void Send(const WalletID&, const Blob&) override { deny(); }
    void HftSubscribe(bool) override { deny(); }
    const Merkle::Hash* get_DependentState(uint32_t&) override { deny(); }
};
}

void signOffline(const std::shared_ptr<wallet::WalletDB>& db, const wallet::TxParameters& parameters,
    const SigningContext& context, Amount totalFee, Amount explicitFee, Amount amount,
    std::atomic<bool>& cancelled, const std::function<void(const SignedMaterial&)>& persist,
    const std::function<void(const char*)>& boundary) {
    using namespace wallet;
    Gateway gateway(db, context, cancelled, persist, totalFee, explicitFee, amount);
    Wallet wallet(db, {}, {}, Wallet::ExecutionMode::Offline);
    auto creator = std::make_shared<lelantus::PushTransaction::Creator>([db] { return db; });
    BaseTransaction::Ptr tx;
    try {
        tx = wallet.BeginOfflineTransaction(parameters, gateway, creator, [&] { return gateway.current(); });
        if (boundary) boundary("offline-core-created");
        tx->Update();
        if (boundary) boundary("offline-updated");
        auto timer = io::Timer::create(io::Reactor::get_Current());
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::minutes(5);
        while (!gateway.done && !gateway.failed && !cancelled.load() && context.current(*db)) {
            if (std::chrono::steady_clock::now() >= deadline) break;
            timer->start(5, false, [] { io::Reactor::get_Current().stop(); });
            io::Reactor::get_Current().run();
        }
        gateway.closed = true;
        wallet.EndOfflineTransaction(); // retire before destroying async callback owners
        tx.reset();
        if (gateway.error) std::rethrow_exception(gateway.error);
        if (!gateway.done) throw SendError("SIGNING_INTERRUPTED", "Signing interrupted; retry the same operationId");
    } catch (...) {
        gateway.closed = true;
        wallet.EndOfflineTransaction();
        tx.reset();
        throw;
    }
}
} // namespace beam::sdk
