// Reuse the already-validated synthetic node/funding/builder helpers, not its probe main.
#define main offlineSigningProbeMain
#include "offline_signing_test.cpp"
#undef main
#include "offline_context.h"
#include <cstring>
#include <fstream>

namespace beam::sdk {
struct OfflineContextTestAccess {
    static constexpr const char* Key = "beam.sdk.kmp.offline-context.v1";
    static void corruptCoverage(OfflineContext& c) { c.saved_.commitments.erase(c.saved_.commitments.begin()); }
    static void corruptEndpoint(OfflineContext& c) { c.saved_.windows.front().response.m_State1.m_pData[0] ^= 1; }
    static void incompatible(OfflineContext& c, bool network) {
        if (network) ++c.saved_.network;
        else c.saved_.rules += "changed";
    }
    static size_t plannedItems(const OfflineContext& c) {
        size_t result = 0;
        for (auto range : c.downloads_) result += range.second;
        return result;
    }
    static std::shared_ptr<const OfflineContext::Snapshot> snapshot(
        const OfflineContext::Record& record, std::vector<OfflineContext::Range> ranges) {
        // Exercises the production snapshot builder itself; the database/current fences that
        // guard it are covered by OfflineContext::snapshot on the real cryptographic fixture.
        std::sort(ranges.begin(), ranges.end());
        auto result = std::make_shared<OfflineContext::Snapshot>();
        if (!OfflineContext::build(record, ranges, *result)) return {};
        return result;
    }
    static OfflineContext::Record productionRecord(wallet::WalletDB& source, const std::vector<OfflineContext::Range>& ranges,
        TxoID count, const Block::SystemState::Full& tip) {
        OfflineContext::Record record;
        require(wallet::storage::getBlobVar(source, Key, record), "missing production record");
        record.rules = Rules::get().get_SignatureStr();
        record.checkpoint = tip;
        record.count = count;
        record.windows.clear();
        record.commitments.clear();
        for (auto range : ranges) {
            record.windows.push_back({range, {}});
            for (TxoID i = 0; i < std::min<TxoID>(range.second, count - range.first); ++i)
                record.commitments.emplace(range.first + i, ECC::Point::Storage{});
        }
        // Production schema/serializer including real proof metadata; synthetic point
        // payload at production coverage size. This is a size fixture, not chain evidence.
        return record;
    }
};
}

namespace {
using Context = beam::sdk::OfflineContext;

#include "offline_context_network_fixture.h"

void synced(WalletDB& db, NodeProcessor& node, uint64_t generation = 7) {
    db.get_History().AddStates(&node.m_Cursor.m_Full, 1);
    db.setSystemStateID(node.m_Cursor.m_hh);
    db.set_ShieldedOuts(node.m_Extra.m_ShieldedOutputs);
    RecognitionRecovery recovery;
    recovery.m_Phase = RecognitionRecovery::Complete;
    recovery.m_Generation = generation;
    recovery.m_Target = recovery.m_Cursor = node.m_Cursor.m_hh;
    storage::setBlobVar(db, RecognitionRecovery::Key, recovery);
    db.SetSelectionAllowed(true);
    db.FlushNow();
}

void realistic(const std::filesystem::path& dir, Funding funds, const Block::SystemState::Full& tip, WalletDB& source) {
    Rules rules = Rules::get();
    rules.Shielded.m_ProofMax = {2, 16};
    rules.Shielded.m_ProofMin = {2, 10};
    rules.Shielded.MaxWindowBacklog = 65536;
    rules.UpdateChecksum();
    Rules::Scope scope(rules);
    require(rules.Shielded.m_ProofMax.get_N() == 65536 && rules.Shielded.m_ProofMin.get_N() == 1024,
        "wrong production proof sizes");
    const TxoID count = 1'000'000;
    {
        std::vector<Context::Range> overlapping;
        for (TxoID start = 0; start != 256; ++start) overlapping.emplace_back(start, 65536);
        const auto record = beam::wallet::toByteBuffer(
            beam::sdk::OfflineContextTestAccess::productionRecord(source, overlapping, count, tip));
        const size_t expanded = size_t(256) * 65536, unique = 65536 + 255;
        require(record.size() < Context::MaxBytes, "overlapping model does not fit durable cache");
        std::cout << "OVERLAPPING_WINDOW_BASELINE_OK windows=256 windowItems=65536 itemStorage="
            << sizeof(ECC::Point::Storage) << " uniqueItems=" << unique << " expandedItems=" << expanded
            << " uniquePayload=" << unique * sizeof(ECC::Point::Storage)
            << " eagerPayload=" << expanded * sizeof(ECC::Point::Storage)
            << " serializedRecord=" << record.size() << " syntheticPointPayload=1\n";
    }
    {
        // Same 256 overlapping production windows through the signing snapshot: the resident
        // representation stays at the deduplicated payload and an item list exists only for
        // the range Core actually requested.
        std::vector<Context::Range> overlapping;
        for (TxoID start = 0; start != 256; ++start) overlapping.emplace_back(start, 65536);
        auto record = beam::sdk::OfflineContextTestAccess::productionRecord(source, overlapping, count, tip);
        // Distinct synthetic points and endpoints: content assertions must not pass on zeros.
        for (auto& item : record.commitments) {
            const auto id = item.first;
            std::memcpy(item.second.m_X.m_pData, &id, sizeof(id));
            item.second.m_Y.m_pData[0] = static_cast<uint8_t>(id & 7);
        }
        for (size_t i = 0; i != record.windows.size(); ++i)
            std::memcpy(record.windows[i].response.m_State1.m_pData, &i, sizeof(i));
        const auto snapshot = beam::sdk::OfflineContextTestAccess::snapshot(record, overlapping);
        const size_t expanded = size_t(256) * 65536, unique = 65536 + 255;
        require(bool(snapshot), "bounded snapshot rejected valid production coverage");
        require(snapshot->windows() == 256 && snapshot->items() == unique,
            "signing snapshot expanded overlapping windows");
        require(snapshot->bytes() < Context::MaxBytes &&
            expanded * sizeof(ECC::Point::Storage) > Context::MaxBytes,
            "snapshot bound does not separate deduplicated from eager payload");
        for (size_t i : {size_t(0), size_t(128), size_t(255)}) {
            const auto range = overlapping[i];
            proto::ShieldedList actual, expected;
            require(snapshot->materialize(range, actual), "saved production window was not served");
            expected.m_State1 = record.windows[i].response.m_State1;
            for (TxoID j = 0; j != range.second; ++j) expected.m_Items.push_back(record.commitments.at(range.first + j));
            require(actual.m_Items.size() == 65536 && encode(actual) == encode(expected),
                "materialised window is not the exact saved items and endpoint");
        }
        proto::ShieldedList rejected;
        require(!snapshot->materialize({0, 65535}, rejected) && !snapshot->materialize({1, 65535}, rejected) &&
            !snapshot->materialize({256, 65536}, rejected) && !snapshot->materialize({0, 65537}, rejected) &&
            rejected.m_Items.empty(), "fabricated or unsaved range was served");
        require(!beam::sdk::OfflineContextTestAccess::snapshot(record, {{0, 65537}}),
            "snapshot admitted a window the quorum never saved");
        auto damaged = record;
        damaged.commitments.erase(damaged.commitments.find(65000));
        require(!beam::sdk::OfflineContextTestAccess::snapshot(damaged, overlapping),
            "snapshot admitted incomplete saved coverage");
        std::cout << "OVERLAPPING_WINDOW_SNAPSHOT_OK windows=" << snapshot->windows()
            << " uniqueItems=" << snapshot->items() << " snapshotBytes=" << snapshot->bytes()
            << " eagerPayload=" << expanded * sizeof(ECC::Point::Storage)
            << " materialisedItems=65536 syntheticPointPayload=1\n";
    }
    size_t totalBytes = 0;
    unsigned scenario = 100;
    for (TxoID id : {TxoID(0), count - 100'000, count - 1}) {
        funds.shielded.m_TxoID = id;
        auto ranges = Context::coverage({funds.shielded, funds.shielded}, count);
        auto dedup = Context::coverage({funds.shielded}, count);
        require(ranges == dedup, "historical windows were not deduplicated");
        Cache cache;
        cache.count = count;
        cache.tip = tip;
        cache.rules = rules.get_SignatureStr();
        size_t bytes = 0;
        for (auto range : ranges) {
            proto::ShieldedList response;
            response.m_Items.resize(std::min<TxoID>(range.second, count - range.first));
            bytes += encode(response).size();
            cache.ranges.emplace(range, std::move(response));
        }
        totalBytes += bytes;
        Session session(dir / ("real-window-" + std::to_string(++scenario) + ".db"), funds, cache,
            Inputs::Shielded, ::Fault::Hold);
        auto recipient = chooseRecipient(dir, scenario, TxAddressType::PublicOffline);
        session.prepare(recipient, 20'000'000, scenario);
        session.sign(); // actual builder stops at its real list request, before costly proof generation
        require(session.gateway.requested.size() == 1 && session.gateway.delayed, "builder request not captured");
        auto requested = *session.gateway.requested.begin();
        require(cache.ranges.count(requested), "automatic context did not cover actual builder");
        std::cout << "REAL_BUILDER_WINDOW id=" << id << " start=" << requested.first
            << " count=" << requested.second << " exactWindowBytes=" << bytes
            << " legacyTwoPeerBytes=" << bytes * 2 << '\n';
        session.tx->Retire();
        session.gateway.delayed = {};
        const auto record = beam::wallet::toByteBuffer(
            beam::sdk::OfflineContextTestAccess::productionRecord(source, ranges, count, tip));
        require(record.size() < Context::MaxBytes, "production record exceeds bound");
        const auto path = dir / ("measured-ciphertext-" + std::to_string(id) + ".db");
        auto measured = createDb(path, 600);
        measured->FlushNow();
        const auto before = std::filesystem::file_size(path);
        measured->setVarRaw(beam::sdk::OfflineContextTestAccess::Key, record.data(), record.size());
        measured->FlushNow();
        ByteBuffer stored;
        require(measured->getBlob(beam::sdk::OfflineContextTestAccess::Key, stored) && stored == record,
            "production record measurement did not persist exact bytes");
        const auto after = std::filesystem::file_size(path);
        std::cout << "REAL_RECORD_STORAGE id=" << id << " serializedBytes=" << record.size()
            << " encryptedDbGrowth=" << after - before << " syntheticPointPayload=1 realProofMetadata=1\n";
    }
    std::cout << "REAL_WINDOW_COVERAGE_OK cases=3 bytes=" << totalBytes << " itemStorage="
        << sizeof(ECC::Point::Storage) << " cap=" << Context::MaxBytes << '\n';
}

void contextFixture(const std::filesystem::path& dir) {
    auto reactor = io::Reactor::create();
    io::Reactor::Scope rs(*reactor);
    Rules rules;
    rules.m_Consensus = Rules::Consensus::FakePoW;
    rules.AllowPublicUtxos = true;
    rules.TreasuryChecksum = Zero;
    rules.Maturity.Coinbase = 10;
    for (size_t i = 1; i + 1 < std::size(rules.pForks); ++i) rules.pForks[i].m_Height = 5 + i;
    rules.Shielded.m_ProofMax = {2, 4};
    rules.Shielded.m_ProofMin = {2, 2};
    rules.Shielded.MaxWindowBacklog = 32;
    rules.UpdateChecksum();
    Rules::Scope scope(rules);
    auto db = createDb(dir / "context.db", kSenderSeed);
    auto minerDb = createDb(dir / "miner.db", 90817);
    auto miner = minerDb->get_MasterKdf();
    NodeProcessor node;
    node.m_Horizon.SetInfinite();
    node.Initialize((dir / "node.db").string().c_str());
    Funding funds;
    while (node.m_Cursor.m_hh.m_Height < 29) mine(node, miner);
    mine(node, miner, fund(miner, db->get_MasterKdf(), 1, 30, false, 0, &funds));
    for (unsigned i = 0; i < kInitialPool; ++i)
        mine(node, miner, fund(miner, i ? miner : db->get_MasterKdf(), 2 + i, 31 + i, true, i, i ? nullptr : &funds));
    db->saveShieldedCoin(funds.shielded);
    db->saveCoin(funds.ordinary);
    std::string contextId;
    size_t initialTraffic = 0;
    {
        Wallet wallet(db);
        synced(*db, node);
        ContextNetwork network(wallet, node);
        beam::sdk::OfflineContextEvents log;
        Context context(db, 0, [&](const Context::State& state) { log.add(state); });
        context.load();
        require(context.state().phase == Context::Phase::Unavailable, "new wallet has context");
        context.prepare(network);
        require(context.state().phase == Context::Phase::Preparing, "normal sync did not prepare");
        network.finish();
        require(context.state().phase == Context::Phase::Ready, "valid quorum did not publish");
        const auto& events = log.events();
        require(events.size() >= 4 && events.front().state.reason == "no-record" &&
            events[1].state.phase == Context::Phase::Preparing && events.back().state.reason == "published",
            "context events omitted a transition or its reason");
        const auto& last = events[events.size() - 2].state;
        require(last.reason == "progress" && last.boundaryDone && last.downloadsTotal &&
            last.downloadsDone == last.downloadsTotal && last.proofsDone == last.proofsTotal && !last.awaitingSecondPeer,
            "final progress event did not reach its totals");
        bool awaiting = false;
        for (size_t i = 0; i < events.size(); ++i) {
            require(events[i].seq == events.front().seq + i, "context event sequence has a gap");
            awaiting |= events[i].state.awaitingSecondPeer;
        }
        require(awaiting, "first quorum response was not reported");
        std::cout << "CONTEXT_EVENTS_OK " << events.size() << '\n';
        contextId = context.state().contextId;
        initialTraffic = network.traffic;
        require(network.listItems == 2 * node.m_Extra.m_ShieldedOutputs,
            "cold overlapping windows downloaded duplicate commitments");
        ByteBuffer initialRecord;
        require(db->getBlob(beam::sdk::OfflineContextTestAccess::Key, initialRecord) &&
            initialRecord.size() <= Context::MaxBytes, "production record missing or oversized");
        std::cout << "CONTEXT_ACTUAL_RECORD_BYTES=" << initialRecord.size() << '\n';
        const auto saved = Context::coverage({funds.shielded}, node.m_Extra.m_ShieldedOutputs);
        for (auto range : saved) {
            proto::ShieldedList actual;
            require(context.lookup(contextId, range, actual) && encode(actual) == encode(nodeRange(node, range)),
                "exact checkpoint/items did not survive publication");
            // The bounded signing path must serve byte-identical bytes to the eager lookup.
            proto::ShieldedList served;
            const auto one = context.snapshot(contextId, {range});
            require(one && one->windows() == 1 && one->materialize(range, served) &&
                encode(served) == encode(actual), "bounded snapshot diverged from eager lookup");
        }
        proto::ShieldedList rejected;
        require(!context.lookup(contextId, {1, 1}, rejected), "fabricated subrange served");
        require(!context.lookup("stale-id", {0, 16}, rejected), "stale context served");
        require(!context.snapshot(contextId, {{1, 1}}) && !context.snapshot("stale-id", {{0, 16}}) &&
            !context.snapshot(contextId, {}), "snapshot served a fabricated subrange or stale context");
        // A captured snapshot is owned: reservation notifications clear the cache, the
        // already captured signing scope keeps serving the same bytes.
        const auto retained = context.snapshot(contextId, saved);
        require(retained && retained->windows() == saved.size(), "snapshot missed saved coverage");
        context.invalidate();
        require(context.state().phase == Context::Phase::Invalidated && !context.snapshot(contextId, saved),
            "invalidated cache still authorized a new snapshot");
        for (auto range : saved) {
            proto::ShieldedList served;
            require(retained->materialize(range, served) && encode(served) == encode(nodeRange(node, range)),
                "captured snapshot lost exact items after cache invalidation");
        }
        context.load();
        require(context.state().phase == Context::Phase::Ready && context.state().contextId == contextId,
            "reload after invalidation lost the durable context");
        std::cout << "CONTEXT_SNAPSHOT_OK windows=" << retained->windows() << " items="
            << retained->items() << " bytes=" << retained->bytes() << '\n';
        context.prepare(network);
        require(network.traffic == initialTraffic && !network.held, "unchanged sync refetched cache");
        std::cout << "CONTEXT_QUORUM_READY ownProof=verified exactWindows=verified traffic=" << initialTraffic << '\n';
    }
    db.reset();
    require(!WalletDB::isValidPassword((dir / "context.db").string(), SecString(std::string("wrong"))),
        "context database accepted wrong credential");
    db = std::dynamic_pointer_cast<WalletDB>(WalletDB::open((dir / "context.db").string(),
        SecString(std::string("synthetic-offline-probe"))));
    {
        Context reopened(db, 0, {});
        reopened.load();
        require(reopened.state().phase == Context::Phase::Ready && reopened.state().contextId == contextId,
            "encrypted restart lost context/generation");
        Context wrongNetwork(db, 1, {});
        wrongNetwork.load();
        require(wrongNetwork.state().phase == Context::Phase::Invalidated, "wrong network loaded");
        Rules changed = rules;
        changed.Shielded.MaxWindowBacklog++;
        changed.UpdateChecksum();
        { Rules::Scope different(changed); Context wrongRules(db, 0, {}); wrongRules.load();
          require(wrongRules.state().phase == Context::Phase::Invalidated, "wrong rules loaded"); }
    }
    std::cout << "CONTEXT_ENCRYPTED_RESTART_OK network/rules/generation\n";
    {
        Wallet wallet(db);
        synced(*db, node);
        ContextNetwork network(wallet, node);
        for (auto fault : {ContextNetwork::Fault::Duplicate, ContextNetwork::Fault::Items,
                ContextNetwork::Fault::State, ContextNetwork::Fault::Checkpoint,
                ContextNetwork::Fault::MissingCheckpoint, ContextNetwork::Fault::Range,
                ContextNetwork::Fault::Count, ContextNetwork::Fault::OwnProof}) {
            Context context(db, 0, {});
            context.prepare(network);
            network.reply();
            if (fault == ContextNetwork::Fault::Items || fault == ContextNetwork::Fault::State ||
                fault == ContextNetwork::Fault::Range || fault == ContextNetwork::Fault::OwnProof) {
                network.reply();
                if (fault == ContextNetwork::Fault::OwnProof)
                    while (network.held && network.held->get_Type() == proto::FlyClient::Request::ShieldedList) network.reply();
                else network.reply();
            }
            network.reply(fault);
            require(context.state().phase == Context::Phase::Invalidated, "bad response admitted");
            require(!network.held, "rejected quorum continued downloading");
            Context old(db, 0, {});
            old.load();
            require(old.state().contextId == contextId, "failed quorum replaced durable generation");
        }
        std::cout << "CONTEXT_ADVERSARIES_OK duplicate/items/state/checkpoint/missing/range/count/ownProof\n";
        for (unsigned mode = 0; mode < 4; ++mode) {
            Context context(db, 0, {});
            context.prepare(network);
            auto delayed = network.held;
            if (mode == 0) context.invalidate();
            if (mode == 1) { synced(*db, node, 8); }
            if (mode == 2) context.close();
            if (mode == 3) context.requestStop();
            network.reply();
            require(context.state().phase != Context::Phase::Ready && !network.held,
                "stale generation/close callback published");
            synced(*db, node);
        }
        {
            Context context(db, 0, {});
            context.load();
            auto fork = node.m_Cursor.m_Full;
            fork.m_Prev.m_pData[0] ^= 1;
            db->get_History().DeleteFrom(fork.get_Height());
            db->get_History().AddStates(&fork, 1);
            HeightHash forkId;
            fork.get_ID(forkId);
            db->setSystemStateID(forkId);
            require(context.state().phase == Context::Phase::Invalidated && context.state().reason == "tip-changed",
                "same-height reorg not invalidated");
            Context reopened(db, 0, {});
            reopened.load();
            require(reopened.state().phase == Context::Phase::Invalidated, "same-height reorg loaded cache");
            db->get_History().DeleteFrom(fork.get_Height());
            synced(*db, node);
        }
        std::cout << "CONTEXT_CALLBACK_DRAIN_OK invalidate/recoveryGeneration/close/stopDispatchFence/sameHeightReorg\n";
        Context context(db, 0, {});
        context.load();
        mine(node, miner);
        synced(*db, node);
        context.invalidate();
        Context stale(db, 0, {});
        stale.load();
        require(stale.state().phase == Context::Phase::Invalidated, "changed checkpoint survived reopen");
        const auto before = network.traffic;
        const auto beforeItems = network.listItems;
        const auto beforeListBytes = network.listTraffic;
        context.prepare(network);
        network.finish();
        require(context.state().phase == Context::Phase::Ready && context.state().contextId != contextId,
            "replacement context not published");
        require(network.listItems == beforeItems && network.listTraffic == beforeListBytes &&
            network.traffic - before < initialTraffic,
            "unchanged-count new block downloaded commitments or exceeded cold traffic");
        std::cout << "CONTEXT_REFRESH_OK unchangedSyncBytes=0 newTipBytes=" << network.traffic - before << '\n';

        auto exact = [&] {
            for (auto range : Context::coverage({funds.shielded}, node.m_Extra.m_ShieldedOutputs)) {
                proto::ShieldedList response;
                require(context.lookup(context.state().contextId, range, response) &&
                    encode(response) == encode(nodeRange(node, range)), "refresh lost exact builder endpoint");
            }
        };
        exact();
        for (auto fault : {ContextNetwork::Fault::State, ContextNetwork::Fault::StaleState}) {
            ByteBuffer durable, afterFault;
            require(db->getBlob(beam::sdk::OfflineContextTestAccess::Key, durable), "missing prior row");
            const auto count = node.m_Extra.m_ShieldedOutputs;
            mine(node, miner, fund(miner, miner, count + 2, node.m_Cursor.m_hh.m_Height + 1,
                true, count, nullptr));
            synced(*db, node);
            context.prepare(network);
            network.reply();
            network.reply();
            require(network.held->get_Type() == proto::FlyClient::Request::ShieldedList,
                "append did not request its endpoint");
            network.reply(fault);
            network.reply(fault); // agreeing peers cannot override a known Core sequence
            require(context.state().phase == Context::Phase::Invalidated && !network.held &&
                db->getBlob(beam::sdk::OfflineContextTestAccess::Key, afterFault) && afterFault == durable,
                "matching stale/bad endpoint replaced durable context");
            context.prepare(network);
            network.finish();
            require(context.state().phase == Context::Phase::Ready, "valid append retry failed");
            exact();
        }
        std::cout << "CONTEXT_APPEND_STATE_REJECTED matchingPeers/stale/mismatch\n";
        // Increment through latest-window sliding and large-to-historical-small selection.
        bool historical = false;
        for (unsigned i = 0; i < 48; ++i) {
            const auto count = node.m_Extra.m_ShieldedOutputs;
            const auto oldItems = network.listItems, oldTraffic = network.traffic;
            mine(node, miner, fund(miner, miner, count + 2, node.m_Cursor.m_hh.m_Height + 1,
                true, count, nullptr));
            synced(*db, node);
            context.prepare(network);
            network.finish();
            require(context.state().phase == Context::Phase::Ready, "growth context not published");
            // One appended point plus at most one newly selected historical endpoint,
            // each independently fetched twice. No full overlap download on growth.
            require(network.listItems - oldItems <= 4, "growth refetched overlapping commitments");
            exact();
            auto ranges = Context::coverage({funds.shielded}, node.m_Extra.m_ShieldedOutputs);
            historical |= std::any_of(ranges.begin(), ranges.end(), [&](Context::Range r) {
                return r.second == rules.Shielded.m_ProofMin.get_N();
            });
            if (i == 0 || i == 47) std::cout << "CONTEXT_GROWTH count=" << count + 1
                << " twoPeerItems=" << network.listItems - oldItems
                << " bytes=" << network.traffic - oldTraffic << '\n';
        }
        require(historical, "growth did not exercise historical small windows");
        {
            Funding incoming;
            const auto count = node.m_Extra.m_ShieldedOutputs;
            const auto items = network.listItems;
            mine(node, miner, fund(miner, db->get_MasterKdf(), count + 2,
                node.m_Cursor.m_hh.m_Height + 1, true, count, &incoming));
            db->saveShieldedCoin(incoming.shielded);
            synced(*db, node);
            context.prepare(network);
            network.finish();
            require(context.state().phase == Context::Phase::Ready && network.listItems - items <= 4,
                "new owned output prevented overlap reuse");
            // A wallet inventory change must not discard global immutable commitments.
            incoming.shielded.m_spentHeight = node.m_Cursor.m_hh.m_Height;
            db->saveShieldedCoin(incoming.shielded);
            db->FlushNow();
            const auto beforeSpend = network.listItems;
            context.prepare(network);
            network.finish();
            require(context.state().phase == Context::Phase::Ready && network.listItems == beforeSpend,
                "unchanged-count owned-set change refetched commitments");
            exact();
        }
        {
            mine(node, miner);
            synced(*db, node);
            Context restarted(db, 0, {});
            restarted.load();
            require(restarted.state().phase == Context::Phase::Invalidated, "restart exposed stale proofs");
            const auto items = network.listItems;
            restarted.prepare(network);
            network.finish();
            require(restarted.state().phase == Context::Phase::Ready && network.listItems == items,
                "restart could not reuse ancestor commitments");
            context.load();
            exact();
        }

        // The previous durable row remains byte-identical through refresh faults/drain.
        for (unsigned mode = 0; mode < 6; ++mode) {
            ByteBuffer durable;
            require(db->getBlob(beam::sdk::OfflineContextTestAccess::Key, durable), "missing durable row");
            mine(node, miner);
            synced(*db, node);
            context.prepare(network);
            while (network.held->get_Type() != proto::FlyClient::Request::ProofShieldedOutp) network.reply();
            network.reply(); // final own-proof quorum response remains held
            auto delayed = network.held;
            if (mode == 0) context.invalidate();
            if (mode == 1) context.requestStop();
            if (mode == 2) db->FailNextFlushForTests();
            if (mode == 3) db->SetSelectionAllowed(false);
            if (mode == 4) synced(*db, node, 8);
            if (mode == 5) context.close();
            network.reply();
            ByteBuffer afterFault;
            require(context.state().phase != Context::Phase::Ready && !network.held &&
                db->getBlob(beam::sdk::OfflineContextTestAccess::Key, afterFault) && afterFault == durable,
                "refresh fault published partial coverage/generation");
            if (mode == 5) break; // closed owners cannot resume
            context.stop();
            context.resume();
            synced(*db, node);
            context.prepare(network);
            network.finish();
            require(context.state().phase == Context::Phase::Ready, "refresh retry failed");
        }
        std::cout << "CONTEXT_INCREMENTAL_ATOMIC_OK growth/historical/invalidate/stop/flush/admission/close\n";

        // Same-height forks, changed intermediate ancestors, missing history and
        // compatibility/corruption failures must perform cold list quorum again.
        for (unsigned mode = 0; mode < 9; ++mode) {
            Context fresh(db, 0, {});
            fresh.load();
            if (fresh.state().phase != Context::Phase::Ready) {
                fresh.prepare(network);
                network.finish();
            }
            auto checkpoint = node.m_Cursor.m_Full;
            if (mode == 0) beam::sdk::OfflineContextTestAccess::corruptCoverage(fresh);
            if (mode == 1) beam::sdk::OfflineContextTestAccess::corruptEndpoint(fresh);
            if (mode >= 7) beam::sdk::OfflineContextTestAccess::incompatible(fresh, mode == 7);
            if (mode == 2) {
                synced(*db, node, 8);
            } else if (mode == 3) {
                db->get_History().DeleteFrom(checkpoint.get_Height());
                mine(node, miner);
                synced(*db, node);
            } else if (mode == 4) {
                mine(node, miner);
                auto ancestor = node.m_Cursor.m_Full;
                ancestor.m_Prev.m_pData[0] ^= 1;
                db->get_History().AddStates(&ancestor, 1);
                mine(node, miner);
                synced(*db, node);
            } else if (mode == 5) {
                // Store a different saved checkpoint at the current height.
                fresh.close();
                auto fork = checkpoint;
                fork.m_Prev.m_pData[0] ^= 1;
                db->get_History().AddStates(&fork, 1);
                HeightHash id;
                fork.get_ID(id);
                db->setSystemStateID(id);
                Context forked(db, 0, {});
                forked.load();
                require(forked.state().phase == Context::Phase::Invalidated, "fork retained readiness");
                forked.prepare(network);
                require(beam::sdk::OfflineContextTestAccess::plannedItems(forked) > 2,
                    "same-height fork did not schedule cold coverage");
                const auto start = network.listItems;
                network.reply(); // real-node response checkpoint rejects fork before list download
                require(forked.state().phase == Context::Phase::Invalidated && network.listItems == start,
                    "fork admitted wrong-checkpoint response");
                synced(*db, node);
                continue;
            } else if (mode == 6) {
                mine(node, miner);
                synced(*db, node);
            }
            const auto start = network.listItems;
            fresh.invalidate();
            fresh.prepare(network);
            network.finish();
            require(fresh.state().phase == Context::Phase::Ready, "fallback could not publish");
            if (mode != 6) require(network.listItems > start + 4, "unsafe saved coverage was reused");
            else require(network.listItems == start, "compatible ancestry failed to resume reuse");
            db->get_History().AddStates(&checkpoint, 1);
            synced(*db, node);
        }
        std::cout << "CONTEXT_INCREMENTAL_FALLBACK_OK coverage/state/recovery/unknown/ancestor/fork\n";
        Context* owner = nullptr;
        Context reentrant(db, 0, [&](const Context::State& state) {
            if (state.phase == Context::Phase::Preparing) owner->stop();
        });
        owner = &reentrant;
        reentrant.prepare(network);
        require(!network.held && reentrant.state().phase == Context::Phase::Unavailable &&
            reentrant.state().reason == "stopped", "Preparing observer stop dispatched a request after drain");
    }
    {
        auto empty = createDb(dir / "empty.db", 900);
        Wallet wallet(empty);
        synced(*empty, node);
        ContextNetwork network(wallet, node);
        Context context(empty, 0, {});
        context.prepare(network);
        network.finish();
        require(context.state().phase == Context::Phase::Ready && network.requests == 2,
            "empty shielded wallet cannot retain MW context");
        std::cout << "CONTEXT_MW_ONLY_EMPTY_POOL_COVERAGE_OK\n";
        auto unknown = funds.shielded;
        unknown.m_TxoID = ShieldedCoin::kTxoInvalidID;
        unknown.m_confirmHeight = MaxHeight;
        empty->saveShieldedCoin(unknown);
        require(context.state().phase == Context::Phase::Invalidated &&
            context.state().reason == "shielded-coins-changed", "coin mutation retained readiness");
        context.prepare(network);
        require(context.state().phase == Context::Phase::Invalidated && !network.held &&
            context.state().reason.rfind("prepare: offline_context.cpp:", 0) == 0,
            "unknown shielded data was treated as empty coverage");
        empty->clearShieldedCoins();
        context.load();
        require(context.state().phase == Context::Phase::Ready, "valid old empty coverage was lost");
        empty->get_History().DeleteFrom(node.m_Cursor.m_hh.m_Height);
        context.load();
        require(context.state().phase == Context::Phase::Invalidated &&
            context.state().reason.rfind("load: offline_context.cpp:", 0) == 0, "evicted checkpoint retained readiness");
        std::cout << "CONTEXT_UNKNOWN_AND_EVICTION_REJECTED\n";
    }
    realistic(dir, funds, node.m_Cursor.m_Full, *db);
}
}

void eventsBound() {
    beam::sdk::OfflineContextEvents log;
    Context::State state;
    for (int i = 0; i < 70; ++i) { state.reason = std::to_string(i); log.add(state); log.add(state); }
    const auto& events = log.events();
    require(events.size() == beam::sdk::OfflineContextEvents::Capacity && events.front().seq == 7 &&
        events.back().seq == 70 && events.back().state.reason == "69", "context event log bound or dedup broken");
    std::cout << "CONTEXT_EVENTS_BOUND_OK\n";
}

int main() {
    try {
        eventsBound();
        auto dir = std::filesystem::temp_directory_path() / ("beam-context-test-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        require(std::filesystem::create_directory(dir), "fixture directory exists");
        contextFixture(dir);
        std::filesystem::remove_all(dir);
        std::cout << "OFFLINE_CONTEXT_TEST_OK\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "OFFLINE_CONTEXT_TEST_FAILED: " << e.what() << '\n';
        return 1;
    }
}
