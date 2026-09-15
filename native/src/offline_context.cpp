#include "offline_context.h"
#include "core/shielded.h"
#include <algorithm>
#include <set>

namespace beam::sdk {
namespace {
constexpr const char* Key = "beam.sdk.kmp.offline-context.v1";
template<class T> ByteBuffer bytes(const T& value) { return wallet::toByteBuffer(value); }
bool equal(const Block::SystemState::Full& a, const Block::SystemState::Full& b) {
    return a == b;
}
void require(bool value) {
    if (!value) throw std::runtime_error("Offline signing context is incomplete or inconsistent");
}
}

OfflineContext::OfflineContext(std::shared_ptr<wallet::WalletDB> db, int network, Changed changed)
    : db_(std::move(db)), network_(network), changed_(std::move(changed)) { db_->Subscribe(this); }
OfflineContext::~OfflineContext() { cancel(); db_->Unsubscribe(this); }

void OfflineContext::cancel() {
    ++epoch_;
    if (request_) request_->m_pTrg = nullptr;
    request_.reset();
    candidate_.clear();
    firstPeer_ = {};
    transport_ = nullptr;
    pending_ = {};
    downloads_.clear();
    endpoints_.clear();
}
void OfflineContext::report(Phase phase) {
    state_ = {};
    state_.phase = phase;
    if (phase == Phase::Ready) {
        state_.contextId = identity(saved_);
        state_.height = saved_.checkpoint.get_Height();
        state_.shieldedCount = saved_.count;
    }
    if (changed_) changed_(state_);
}
void OfflineContext::onSystemStateChanged(const HeightHash& id) {
    const auto* record = state_.phase == Phase::Ready ? &saved_ :
        state_.phase == Phase::Preparing ? &pending_ : nullptr;
    if (!record) return;
    HeightHash expected;
    record->checkpoint.get_ID(expected);
    if (expected != id) invalidate();
}
void OfflineContext::invalidate() {
    cancel();
    if (!closed_) report(Phase::Invalidated);
}
void OfflineContext::stop() {
    requestStop();
    cancel();
    // A completed durable context survives a disconnected/stopped owner.
    if (state_.phase == Phase::Preparing) report(Phase::Unavailable);
}
void OfflineContext::close() {
    stop();
    closed_ = true;
    changed_ = {};
}

std::vector<wallet::ShieldedCoin> OfflineContext::coins() const {
    std::vector<wallet::ShieldedCoin> result;
    db_->visitShieldedCoinsUnspent([&](const wallet::ShieldedCoin& coin) {
        // A self push saves its incoming output before the signing callback. It is
        // not a spend candidate, including after an interrupted signing attempt.
        // Accept only the exact untouched pending shape with matching Core ownership
        // evidence; partially confirmed or unknown incoming rows still fail closed.
        if (coin.m_TxoID == wallet::ShieldedCoin::kTxoInvalidID &&
            coin.m_confirmHeight == MaxHeight && coin.m_spentHeight == MaxHeight &&
            !coin.m_spentTxId && coin.m_createTxId) {
            const auto tx = db_->getTx(*coin.m_createTxId);
            bool self = false;
            const auto* message = ShieldedTxo::User::ToPackedMessage(coin.m_CoinID.m_User);
            ShieldedTxo::User::PackedMessage::TxID created;
            created = Blob(coin.m_createTxId->data(), static_cast<uint32_t>(coin.m_createTxId->size()));
            if (tx && tx->m_sender && tx->m_txType == wallet::TxType::PushTransaction &&
                tx->m_amount == coin.m_CoinID.m_Value && tx->m_assetId == coin.m_CoinID.m_AssetID &&
                wallet::storage::getTxParameter(*db_, *coin.m_createTxId, wallet::TxParameterID::IsSelfTx, self) &&
                self && message->m_TxID == created)
                return true;
        }
        // Prepare confirmed coins including locked/maturing ones before amount selection.
        // Unknown incoming data must not masquerade as an empty shielded wallet.
        require(coin.m_TxoID != wallet::ShieldedCoin::kTxoInvalidID &&
            coin.m_confirmHeight != MaxHeight && coin.m_confirmHeight != 0);
        result.push_back(coin);
        require(result.size() <= MaxWindows);
        return true;
    });
    return result;
}

std::vector<OfflineContext::Range> OfflineContext::coverage(
    const std::vector<wallet::ShieldedCoin>& coins, TxoID count) {
    std::set<Range> ranges;
    const auto max = Rules::get().Shielded.m_ProofMax.get_N();
    require(max && max <= UINT32_MAX / 2);
    if (!coins.empty()) {
        require(count != 0);
        const auto n = static_cast<uint32_t>(std::min<TxoID>(count, 2 * max));
        ranges.emplace(count - n, n); // production candidate: latest 131072
    }
    for (const auto& coin : coins) {
        wallet::BaseTxBuilder::ShieldedWindow window;
        require(wallet::BaseTxBuilder::SelectShieldedWindow(coin, count, window));
        ranges.emplace(window.m_Start, window.m_Count);
        require(ranges.size() <= MaxWindows);
    }
    return {ranges.begin(), ranges.end()};
}

ECC::Point OfflineContext::serial(const wallet::ShieldedCoin& coin, Key::IPKdf& owner) {
    ShieldedTxo::Data::Params p;
    p.Set(owner, coin.m_CoinID);
    ECC::Point::Native point = ECC::Context::get().G * p.m_Ticket.m_pK[0];
    point += ECC::Context::get().J * p.m_Ticket.m_pK[1];
    ECC::Point result;
    point.Export(result);
    return result;
}

bool OfflineContext::matchesDatabase(const Record& record) const {
    if (record.version != 1 || record.network != network_ ||
        record.rules != Rules::get().get_SignatureStr() || !record.generation ||
        record.count != db_->get_ShieldedOuts() || !record.checkpoint.get_Height()) return false;
    Block::SystemState::Full tip;
    HeightHash id;
    db_->getSystemStateID(id);
    HeightHash savedId;
    record.checkpoint.get_ID(savedId);
    if (id != savedId || !db_->get_History().get_Tip(tip) || !equal(tip, record.checkpoint))
        return false; // conservative ancestry/eviction check: no unknown or advanced tip
    wallet::RecognitionRecovery recovery;
    if (!wallet::storage::getBlobVar(*db_, wallet::RecognitionRecovery::Key, recovery) ||
        recovery.m_Version != 1 || recovery.m_Phase != wallet::RecognitionRecovery::Complete ||
        recovery.m_RollbackPending || recovery.m_Generation != record.recoveryGeneration) return false;
    return true;
}

bool OfflineContext::validProof(const Record& record, const OwnProof& proof) {
    const auto& p = proof.response;
    if (p.m_Proof.empty() || p.m_ID != proof.id || p.m_ID >= record.count ||
        !p.m_Height || p.m_Height > record.checkpoint.get_Height()) return false;
    ShieldedTxo::DescriptionOutp desc;
    desc.m_ID = p.m_ID;
    desc.m_Height = p.m_Height;
    desc.m_Commitment = p.m_Commitment;
    desc.m_SerialPub = proof.serial;
    if (!record.checkpoint.IsValidProofShieldedOutp(desc, p.m_Proof)) return false;
    ECC::Point::Native point, serialPoint;
    if (!point.Import(p.m_Commitment) || !serialPoint.Import(proof.serial)) return false;
    point += serialPoint;
    ECC::Point::Storage commitment;
    point.Export(commitment);
    auto item = record.commitments.find(proof.id);
    return item != record.commitments.end() && bytes(item->second) == bytes(commitment);
}

bool OfflineContext::complete(const Record& record, bool currentCoins) const {
    auto owned = currentCoins ? coins() : std::vector<wallet::ShieldedCoin>{};
    std::vector<Range> required;
    if (currentCoins) required = coverage(owned, record.count);
    else for (const auto& w : record.windows) required.push_back(w.range);
    if (required.size() != record.windows.size() || required.size() > MaxWindows ||
        record.proofs.size() > MaxWindows || (currentCoins && owned.size() != record.proofs.size())) return false;
    std::map<TxoID, ECC::Hash::Value> endpoints;
    TxoID coveredEnd = 0, coveredCount = 0;
    for (size_t i = 0; i < required.size(); ++i) {
        const auto& w = record.windows[i];
        if (w.range != required[i] || w.range.first >= record.count || !w.range.second ||
            w.range.second > 2 * Rules::get().Shielded.m_ProofMax.get_N() ||
            (i && !(record.windows[i - 1].range < w.range))) return false;
        const auto n = std::min<TxoID>(w.range.second, record.count - w.range.first);
        const auto end = w.range.first + n;
        if (end > coveredEnd) coveredCount += end - std::max(coveredEnd, w.range.first);
        coveredEnd = std::max(coveredEnd, end);
        auto item = record.commitments.lower_bound(w.range.first);
        for (TxoID j = 0; j < n; ++j, ++item)
            if (item == record.commitments.end() || item->first != w.range.first + j) return false;
        const auto inserted = endpoints.emplace(w.range.first + n - 1, w.response.m_State1);
        if (!inserted.second && inserted.first->second != w.response.m_State1) return false;
    }
    if (coveredCount != record.commitments.size()) return false;
    // Validate every sequence for which a seed is known; historical islands still rely
    // on their saved independent-peer quorum, not on header Merkle authentication.
    ECC::Hash::Value state = Zero;
    TxoID nextId = 0;
    bool known = true;
    for (const auto& item : record.commitments) {
        if (item.first >= record.count) return false;
        if (item.first != nextId) known = false;
        if (known) ShieldedTxo::UpdateState(state, item.second);
        auto end = endpoints.find(item.first);
        if (end != endpoints.end()) {
            if (known && state != end->second) return false;
            state = end->second;
            known = true;
        }
        nextId = item.first + 1;
    }
    for (const auto& coin : owned) {
        const auto it = std::find_if(record.proofs.begin(), record.proofs.end(),
            [&](const OwnProof& proof) { return proof.id == coin.m_TxoID; });
        if (it == record.proofs.end() || it->response.m_Height != coin.m_confirmHeight ||
            bytes(it->serial) != bytes(serial(coin, *db_->get_OwnerKdf())) || !validProof(record, *it)) return false;
    }
    if (!currentCoins)
        for (const auto& proof : record.proofs) if (!validProof(record, proof)) return false;
    return true;
}

bool OfflineContext::reusable() const {
    try {
        if (saved_.version != 1 || !saved_.generation || saved_.network != pending_.network ||
            saved_.rules != pending_.rules || saved_.recoveryGeneration != pending_.recoveryGeneration ||
            saved_.count > pending_.count || !saved_.checkpoint.get_Height() || !complete(saved_, false)) return false;
        // History membership at the old height alone is insufficient: require every link to
        // the current checkpoint. Eviction, a gap, or any fork falls back to cold quorum fetch.
        auto next = pending_.checkpoint;
        while (next.get_Height() > saved_.checkpoint.get_Height()) {
            Block::SystemState::Full previous;
            if (!db_->get_History().get_At(previous, next.get_Height() - 1) || !previous.IsNext(next))
                return false;
            next = previous;
        }
        return equal(next, saved_.checkpoint);
    } catch (const std::exception&) { return false; }
}

void OfflineContext::planDownloads() {
    std::set<TxoID> ends;
    std::vector<std::pair<TxoID, TxoID>> spans;
    for (const auto& w : pending_.windows) {
        const auto end = w.range.first + std::min<TxoID>(w.range.second, pending_.count - w.range.first);
        ends.insert(end - 1);
        if (!spans.empty() && spans.back().second >= w.range.first)
            spans.back().second = std::max(spans.back().second, end);
        else spans.emplace_back(w.range.first, end);
    }
    if (reusable()) {
        for (const auto& w : saved_.windows) {
            const auto end = w.range.first + std::min<TxoID>(w.range.second, saved_.count - w.range.first) - 1;
            const auto inserted = endpoints_.emplace(end, w.response.m_State1);
            require(inserted.second || inserted.first->second == w.response.m_State1);
        }
        for (auto span : spans) {
            auto item = saved_.commitments.lower_bound(span.first);
            while (item != saved_.commitments.end() && item->first < span.second) {
                pending_.commitments.insert(*item);
                ++item;
            }
        }
    }
    const auto limit = 2 * Rules::get().Shielded.m_ProofMax.get_N();
    std::set<TxoID> fetchedEnds;
    for (auto span : spans) {
        auto item = pending_.commitments.lower_bound(span.first);
        for (auto id = span.first; id < span.second;) {
            if (item != pending_.commitments.end() && item->first == id) { ++id; ++item; continue; }
            auto end = std::min<TxoID>(span.second, id + std::min<TxoID>(limit, span.second - id));
            if (item != pending_.commitments.end()) end = std::min(end, item->first);
            // Split at required response ends. Every missing point is downloaded once,
            // while all builder endpoints retain an actual quorum response State1.
            auto checkpoint = ends.lower_bound(id);
            if (checkpoint != ends.end()) end = std::min(end, *checkpoint + 1);
            downloads_.emplace_back(id, static_cast<uint32_t>(end - id));
            fetchedEnds.insert(end - 1);
            id = end;
        }
    }
    for (auto end : ends)
        if (!endpoints_.count(end) && !fetchedEnds.count(end)) downloads_.emplace_back(end, 1);
    std::sort(downloads_.begin(), downloads_.end());
    pendingBytes_ = pending_.commitments.size() * (sizeof(TxoID) + sizeof(ECC::Point::Storage));
    require(pendingBytes_ <= MaxBytes);
}

std::string OfflineContext::identity(const Record& record) {
    const auto data = bytes(record);
    ECC::Hash::Value hash;
    ECC::Hash::Processor() << Blob(data) >> hash;
    return std::to_string(record.generation) + ":" + hash.str();
}

void OfflineContext::load() {
    if (closed_) return;
    cancel();
    saved_ = {};
    try {
        ByteBuffer data;
        if (!db_->getBlob(Key, data)) { report(Phase::Unavailable); return; }
        require(data.size() <= MaxBytes);
        Deserializer d;
        d.reset(data);
        Record record;
        d & record;
        require(!d.bytes_left() && bytes(record) == data);
        nextGeneration_ = std::max(nextGeneration_, record.generation);
        require(complete(record, false));
        saved_ = std::move(record);
        require(matchesDatabase(saved_)); // stale record may seed an ancestry-checked refresh only
        require(complete(saved_));
        report(Phase::Ready);
    } catch (const std::exception&) { report(Phase::Invalidated); }
}

void OfflineContext::prepare(proto::FlyClient::INetwork& transport) {
    if (closed_ || stopRequested_.load() || state_.phase == Phase::Preparing) return;
    try {
        if (state_.phase == Phase::Ready && matchesDatabase(saved_) && complete(saved_)) return;
        cancel();
        require(db_->IsSelectionAllowed());
        pending_.network = network_;
        pending_.rules = Rules::get().get_SignatureStr();
        if (!nextGeneration_) {
            Record previous;
            if (wallet::storage::getBlobVar(*db_, Key, previous))
                nextGeneration_ = previous.generation;
        }
        require(nextGeneration_ < UINT64_MAX);
        pending_.generation = ++nextGeneration_;
        pending_.count = db_->get_ShieldedOuts();
        require(db_->get_History().get_Tip(pending_.checkpoint));
        wallet::RecognitionRecovery recovery;
        require(wallet::storage::getBlobVar(*db_, wallet::RecognitionRecovery::Key, recovery));
        pending_.recoveryGeneration = recovery.m_Generation;
        require(matchesDatabase(pending_));
        auto owned = coins();
        for (auto range : coverage(owned, pending_.count)) pending_.windows.push_back({range, {}});
        for (const auto& coin : owned) pending_.proofs.push_back({coin.m_TxoID, serial(coin, *db_->get_OwnerKdf()), {}});
        planDownloads();
        transport_ = &transport;
        boundaryDone_ = false;
        windowIndex_ = proofIndex_ = 0;
        report(Phase::Preparing);
        next();
    } catch (const std::exception&) { invalidate(); }
}

void OfflineContext::next() {
    using Fly = proto::FlyClient;
    if (closed_ || stopRequested_.load() || state_.phase != Phase::Preparing || !transport_) return;
    if (!boundaryDone_) {
        auto r = new Fly::RequestShieldedOutputsAt;
        r->m_Msg.m_Height = pending_.checkpoint.get_Height();
        request_ = r;
    } else if (windowIndex_ < downloads_.size()) {
        auto r = new Fly::RequestShieldedList;
        r->m_Msg.m_Id0 = downloads_[windowIndex_].first;
        r->m_Msg.m_Count = downloads_[windowIndex_].second;
        request_ = r;
    } else if (proofIndex_ < pending_.proofs.size()) {
        auto r = new Fly::RequestProofShieldedOutp;
        r->m_Msg.m_SerialPub = pending_.proofs[proofIndex_].serial;
        request_ = r;
    } else { publish(); return; }
    requestEpoch_ = epoch_;
    request_->m_OfflineContext = true;
    request_->m_RecoveryExcludedAddress = firstPeer_;
    transport_->PostRequest(*request_, *this);
}

void OfflineContext::OnComplete(Request& request) {
    using Fly = proto::FlyClient;
    if (closed_ || stopRequested_.load() || requestEpoch_ != epoch_ || request_.get() != &request) return;
    try {
        require(state_.phase == Phase::Preparing && matchesDatabase(pending_) && db_->IsSelectionAllowed());
        require(request.m_OfflineContext && request.m_RecoveryResponder != io::Address() &&
            equal(request.m_ResponseCheckpoint, pending_.checkpoint));
        ByteBuffer response;
        if (!boundaryDone_) {
            const auto& r = request.As<Fly::RequestShieldedOutputsAt>();
            require(r.m_Msg.m_Height == pending_.checkpoint.get_Height() && r.m_Res.m_ShieldedOuts == pending_.count);
            response = bytes(r.m_Res);
        } else if (windowIndex_ < downloads_.size()) {
            const auto& r = request.As<Fly::RequestShieldedList>();
            const auto range = downloads_[windowIndex_];
            require(r.m_Msg.m_Id0 == range.first && r.m_Msg.m_Count == range.second &&
                r.m_Res.m_Items.size() == std::min<TxoID>(range.second, pending_.count - range.first));
            response = bytes(r.m_Res); // includes exact items and State1, never a fabricated subrange
        } else {
            const auto& r = request.As<Fly::RequestProofShieldedOutp>();
            auto proof = pending_.proofs[proofIndex_];
            require(bytes(r.m_Msg.m_SerialPub) == bytes(proof.serial));
            proof.response = r.m_Res;
            require(validProof(pending_, proof));
            response = bytes(r.m_Res);
        }
        require(response.size() <= MaxBytes);
        if (firstPeer_ == io::Address()) {
            candidate_ = std::move(response);
            firstPeer_ = request.m_RecoveryResponder;
        } else {
            require(firstPeer_ != request.m_RecoveryResponder && candidate_ == response);
            if (!boundaryDone_) boundaryDone_ = true;
            else if (windowIndex_ < downloads_.size()) {
                const auto& list = request.As<Fly::RequestShieldedList>().m_Res;
                const auto range = downloads_[windowIndex_++];
                auto seed = endpoints_.find(range.first - 1);
                if (!range.first || seed != endpoints_.end()) {
                    ECC::Hash::Value state = Zero;
                    if (range.first) state = seed->second;
                    for (const auto& point : list.m_Items) ShieldedTxo::UpdateState(state, point);
                    require(state == list.m_State1); // Core sequence, never a fabricated subrange root
                }
                for (size_t i = 0; i < list.m_Items.size(); ++i) {
                    const auto inserted = pending_.commitments.emplace(range.first + i, list.m_Items[i]);
                    require(inserted.second || bytes(inserted.first->second) == bytes(list.m_Items[i]));
                }
                const auto inserted = endpoints_.emplace(range.first + list.m_Items.size() - 1, list.m_State1);
                require(inserted.second || inserted.first->second == list.m_State1);
                pendingBytes_ = pending_.commitments.size() * (sizeof(TxoID) + sizeof(ECC::Point::Storage));
                require(pendingBytes_ <= MaxBytes);
            } else {
                pendingBytes_ += response.size();
                require(pendingBytes_ <= MaxBytes);
                pending_.proofs[proofIndex_++].response = std::move(request.As<Fly::RequestProofShieldedOutp>().m_Res);
            }
            firstPeer_ = {};
            candidate_.clear();
        }
        request_.reset();
        next();
    } catch (const std::exception&) { invalidate(); }
}

void OfflineContext::publish() {
    for (auto& w : pending_.windows) {
        const auto end = w.range.first + std::min<TxoID>(w.range.second, pending_.count - w.range.first) - 1;
        w.response.m_State1 = endpoints_.at(end);
    }
    require(!stopRequested_.load() && matchesDatabase(pending_) && complete(pending_));
    const auto data = bytes(pending_);
    require(data.size() <= MaxBytes);
    // Single encrypted row contains coverage AND generation; FlushNow precedes Ready.
    // Existing send operations and recovery batches are never opened/replaced here.
    db_->FlushNow();
    try {
        db_->setVarRaw(Key, data.data(), data.size());
        db_->FlushNow();
    } catch (...) { db_->RollbackNow(false); throw; }
    saved_ = std::move(pending_);
    transport_ = nullptr;
    report(Phase::Ready);
}

bool OfflineContext::build(const Record& record, const std::vector<Range>& sorted, Snapshot& out) {
    out.count_ = record.count;
    // Merge the requested windows into spans first: a commitment shared by many overlapping
    // windows is copied exactly once, so the snapshot stays at the deduplicated Record size.
    std::vector<std::pair<TxoID, TxoID>> spans;
    for (auto range : sorted) {
        if (range.first >= record.count || !range.second ||
            range.second > 2 * Rules::get().Shielded.m_ProofMax.get_N()) return false;
        const auto w = std::find_if(record.windows.begin(), record.windows.end(),
            [&](const Window& x) { return x.range == range; });
        if (w == record.windows.end()) return false; // never serve a window the quorum did not save
        if (!out.windows_.emplace(range, w->response.m_State1).second) return false;
        const auto end = range.first + std::min<TxoID>(range.second, record.count - range.first);
        if (!spans.empty() && spans.back().second >= range.first)
            spans.back().second = std::max(spans.back().second, end);
        else spans.emplace_back(range.first, end);
    }
    for (auto span : spans) {
        auto item = record.commitments.lower_bound(span.first);
        for (auto id = span.first; id < span.second; ++id, ++item) {
            if (item == record.commitments.end() || item->first != id) return false;
            out.commitments_.emplace_hint(out.commitments_.end(), *item);
        }
    }
    return out.bytes() <= MaxBytes;
}

bool OfflineContext::Snapshot::materialize(Range range, proto::ShieldedList& response) const {
    const auto w = windows_.find(range);
    if (w == windows_.end()) return false;
    const auto n = std::min<TxoID>(range.second, count_ - range.first);
    response.m_Items.clear();
    response.m_Items.reserve(static_cast<size_t>(n)); // one window per request; the caller takes ownership
    auto item = commitments_.lower_bound(range.first);
    for (TxoID i = 0; i < n; ++i, ++item) {
        if (item == commitments_.end() || item->first != range.first + i) { response.m_Items.clear(); return false; }
        response.m_Items.push_back(item->second);
    }
    response.m_State1 = w->second;
    return true;
}

std::shared_ptr<const OfflineContext::Snapshot> OfflineContext::snapshot(
    const std::string& id, std::vector<Range> ranges) const {
    if (closed_ || state_.phase != Phase::Ready || id != state_.contextId) return {};
    try {
        if (ranges.empty() || ranges.size() > MaxWindows) return {};
        std::sort(ranges.begin(), ranges.end());
        ranges.erase(std::unique(ranges.begin(), ranges.end()), ranges.end());
        if (!matchesDatabase(saved_) || !complete(saved_)) return {};
        auto result = std::make_shared<Snapshot>();
        if (!build(saved_, ranges, *result)) return {};
        return result; // owned copy: the caller keeps it across reload/invalidate of saved_
    } catch (const std::exception&) {}
    return {};
}

bool OfflineContext::lookup(const std::string& id, Range range, proto::ShieldedList& response) const {
    if (closed_ || state_.phase != Phase::Ready || id != state_.contextId) return false;
    try {
        if (!matchesDatabase(saved_) || !complete(saved_)) return false;
        for (const auto& w : saved_.windows) {
            if (w.range != range) continue;
            response.m_State1 = w.response.m_State1;
            const auto n = std::min<TxoID>(range.second, saved_.count - range.first);
            response.m_Items.clear();
            response.m_Items.reserve(n);
            auto item = saved_.commitments.lower_bound(range.first);
            for (TxoID i = 0; i < n; ++i, ++item) response.m_Items.push_back(item->second);
            return true;
        }
    } catch (const std::exception&) {}
    return false;
}
} // namespace beam::sdk
