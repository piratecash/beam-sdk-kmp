// Shared prior-sync transport fixture. Include after offline_signing_test.cpp helpers.
struct ContextNetwork : proto::FlyClient::NetworkStd {
    NodeProcessor& node;
    proto::FlyClient::Request::Ptr held;
    size_t traffic = 0, requests = 0;
    size_t listItems = 0, listTraffic = 0;
    explicit ContextNetwork(Wallet& w, NodeProcessor& n) : NetworkStd(w), node(n) {}
    void PostRequestInternal(proto::FlyClient::Request& r) override {
        require(!held, "context posted overlapping requests");
        held = &r;
        ++requests;
    }
    enum class Fault { None, Duplicate, Items, State, StaleState, Checkpoint, MissingCheckpoint, Range, Count, OwnProof };
    void reply(Fault fault = Fault::None) {
        using Fly = proto::FlyClient;
        auto r = std::move(held);
        require(bool(r), "missing held context request");
        if (!r->m_pTrg) return;
        const auto a = io::Address::localhost().port(19001), b = io::Address::localhost().port(19002);
        r->m_RecoveryResponder = r->m_RecoveryExcludedAddress == a ? b : a;
        if (fault == Fault::Duplicate) r->m_RecoveryResponder = r->m_RecoveryExcludedAddress;
        r->m_ResponseCheckpoint = node.m_Cursor.m_Full;
        if (fault == Fault::Checkpoint) r->m_ResponseCheckpoint.m_Prev.m_pData[0] ^= 1;
        if (fault == Fault::MissingCheckpoint) r->m_ResponseCheckpoint = {};
        if (r->get_Type() == Fly::Request::ShieldedOutputsAt) {
            auto& request = r->As<Fly::RequestShieldedOutputsAt>();
            request.m_Res.m_ShieldedOuts = node.m_Extra.m_ShieldedOutputs;
            if (fault == Fault::Count) ++request.m_Res.m_ShieldedOuts;
            traffic += encode(request.m_Res).size();
        } else if (r->get_Type() == Fly::Request::ShieldedList) {
            auto& request = r->As<Fly::RequestShieldedList>();
            request.m_Res = nodeRange(node, {request.m_Msg.m_Id0, request.m_Msg.m_Count});
            if (fault == Fault::Range) ++request.m_Msg.m_Id0;
            if (fault == Fault::Items) request.m_Res.m_Items.front().m_X.m_pData[0] ^= 1;
            if (fault == Fault::State) request.m_Res.m_State1.m_pData[0] ^= 1;
            if (fault == Fault::StaleState) {
                require(request.m_Msg.m_Id0 != 0, "stale-state fixture needs a prior endpoint");
                node.get_DB().ShieldedStateRead(request.m_Msg.m_Id0 - 1, &request.m_Res.m_State1, 1);
            }
            listItems += request.m_Res.m_Items.size();
            listTraffic += encode(request.m_Res).size();
            traffic += encode(request.m_Res).size();
        } else {
            auto& request = r->As<Fly::RequestProofShieldedOutp>();
            NodeDB::Recordset rs;
            require(node.get_DB().UniqueFind(Blob(&request.m_Msg.m_SerialPub, sizeof(ECC::Point)), rs),
                "own serial not found in independent node");
            const auto& p = rs.get_As<NodeProcessor::ShieldedOutpPacked>(0);
            p.m_Height.Export(request.m_Res.m_Height);
            p.m_TxoID.Export(request.m_Res.m_ID);
            request.m_Res.m_Commitment = p.m_Commitment;
            TxoID index;
            p.m_MmrIndex.Export(index);
            node.m_Mmr.m_Shielded.get_Proof(request.m_Res.m_Proof, index);
            struct Proof : NodeProcessor::ProofBuilder {
                using ProofBuilder::ProofBuilder;
                bool get_Shielded(Merkle::Hash&) override { return false; }
            } proof(node, request.m_Res.m_Proof);
            proof.GenerateProof();
            if (fault == Fault::OwnProof) request.m_Res.m_Proof.clear();
            traffic += encode(request.m_Res).size();
        }
        auto handler = r->m_pTrg;
        r->m_pTrg = nullptr;
        handler->OnComplete(*r);
    }
    void finish() { while (held) reply(); }
};
