// Regressions for peer-controlled wallet input fixed by patches 0020 and 0021 (BeamMW 1d445fc0ae,
// 2f38843ed0, f175da358a, 0c7b50b7a9). The scenarios are the upstream wallet_test ones; Core's own
// tests are not built here, so they run against the shipped patch stack from this binary.

#include "wallet/core/common.h"
#include "wallet/core/wallet.h"
#include "wallet/core/wallet_network.h"
#include "wallet/core/simple_transaction.h"
#include "keykeeper/local_private_key_keeper.h"
#include "core/unittest/mini_blockchain.h"
#include "utility/test_helpers.h"
#include "node/node.h"

#include "test_helpers.h"
#include "wallet_test_node.h"

#include <boost/filesystem.hpp>
#include <iostream>

thread_local const beam::Rules* beam::Rules::s_pInstance = nullptr;

using namespace beam;
using namespace beam::wallet;
using namespace std;
using namespace ECC;

WALLET_TEST_INIT

#include "wallet_test_environment.cpp"

namespace
{
    void TestUnsolicitedSenderRole()
    {
        cout << "\nTesting incoming tx cannot make us the sender...\n";

        io::Reactor::Ptr mainReactor{ io::Reactor::create() };
        io::Reactor::Scope scope(*mainReactor);

        int completedCount = 2;
        auto f = [&completedCount, mainReactor](auto)
        {
            if (--completedCount == 0)
                mainReactor->stop();
        };

        TestNode node;
        TestWalletRig victim(createSenderWalletDB(), f);
        TestWalletRig attacker(createReceiverWalletDB(), f);

        auto attack = [&](bool decoy)
        {
            SetTxParameter msg;
            msg.m_From = attacker.m_BbsAddr;
            msg.m_TxID = GenerateTxID();
            msg.m_Type = TxType::Simple;
            if (decoy)
            {
                // IsSender=false in a sub-tx that is never applied, true in the default one
                msg.AddParameter(TxParameterID::SubTxIndex, SubTxID(2))
                    .AddParameter(TxParameterID::IsSender, false)
                    .AddParameter(TxParameterID::SubTxIndex, kDefaultSubTxID);
            }
            msg.AddParameter(TxParameterID::IsSender, true)
                .AddParameter(TxParameterID::Amount, Amount(4))
                .AddParameter(TxParameterID::Fee, Amount(2))
                .AddParameter(TxParameterID::Lifetime, Height(200))
                .AddParameter(TxParameterID::PeerProtoVersion, BaseTransaction::s_ProtoVersion);

            completedCount = 2;
            io::Timer::Ptr timer = io::Timer::create(*mainReactor);
            timer->start(10000, false, [mainReactor]() { mainReactor->stop(); });

            attacker.m_messageEndpoint->Send(victim.m_BbsAddr, msg);
            mainReactor->run();

            auto tx = victim.m_WalletDB->getTx(msg.m_TxID);
            if (tx)
                cout << "victim tx status=" << static_cast<int>(tx->m_status) << " amount=" << tx->m_amount << " sender=" << tx->m_sender << endl;
            cout << "attacker coins=" << attacker.GetCoins().size() << endl;

            WALLET_CHECK(!tx);
            WALLET_CHECK(attacker.GetCoins().empty());
        };

        attack(false);
        attack(true);

        SimpleTransaction::Creator simpleCreator(victim.m_WalletDB);
        BaseTransaction::Creator& creator = simpleCreator;
        auto pTx = creator.Create(BaseTransaction::TxContext(*victim.m_Wallet, *victim.m_Wallet, GenerateTxID()));
        WALLET_CHECK(!pTx->IsTxParameterExternalSettable(TxParameterID::IsSender, kDefaultSubTxID));
        WALLET_CHECK(pTx->IsTxParameterExternalSettable(TxParameterID::Amount, kDefaultSubTxID));
    }

    void TestShieldedCoinTxID()
    {
        cout << "\nTesting incoming shielded coin cannot modify existing txs...\n";

        io::Reactor::Ptr mainReactor{ io::Reactor::create() };
        io::Reactor::Scope scope(*mainReactor);

        auto db = createSenderWalletDB();
        EmptyTestGateway gateway;

        auto saveTx = [&](TxType type, bool isSender, bool isSelfTx)
        {
            TxDescription tr(GenerateTxID());
            tr.m_txType = type;
            tr.m_amount = 34;
            tr.m_sender = isSender;
            tr.m_selfTx = isSelfTx;
            tr.m_status = TxStatus::InProgress;
            db->saveTx(tr);
            return tr.m_txId;
        };

        uint32_t nCoin = 0;
        auto restoreCoin = [&](const TxID& packedTxID, uint8_t flags)
        {
            ShieldedTxo::DataParams pars;
            pars.m_Output.m_Value = 100;
            pars.m_Output.m_AssetID = 0;

            auto* msg = ShieldedTxo::User::ToPackedMessage(pars.m_Output.m_User);
            std::copy_n(packedTxID.begin(), packedTxID.size(), msg->m_TxID.m_pData);
            msg->m_Flags = flags;

            ShieldedCoin c;
            c.m_CoinID.m_Key.m_nIdx = 0;
            pars.ToID(c.m_CoinID);
            c.m_CoinID.m_Key.m_kSerG.m_Value = ++nCoin; // distinct coins
            c.m_TxoID = nCoin;
            c.m_confirmHeight = 1;
            c.m_spentHeight = 2;

            db->saveShieldedCoin(c);
            storage::restoreTransactionFromShieldedCoin(*db, c, gateway);
            return c;
        };

        // the peer of a real tx knows its TxID
        TxID victimID = saveTx(TxType::Simple, true, false);

        for (uint8_t flags : { uint8_t(0), kIsSelfTxBit })
        {
            auto c = restoreCoin(victimID, flags);

            bool val = false;
            WALLET_CHECK(storage::getTxParameter(*db, victimID, TxParameterID::IsSender, val) && val);
            WALLET_CHECK(storage::getTxParameter(*db, victimID, TxParameterID::IsSelfTx, val) && !val);
            TxAddressType addrType = TxAddressType::Unknown;
            WALLET_CHECK(!storage::getTxParameter(*db, victimID, TxParameterID::AddressType, addrType));

            // the coin gets a receive record of its own
            WALLET_CHECK(c.m_createTxId && *c.m_createTxId != victimID);
            auto rcv = c.m_createTxId ? db->getTx(*c.m_createTxId) : boost::none;
            WALLET_CHECK(rcv && rcv->m_txType == TxType::PushTransaction && !rcv->m_sender && !rcv->m_selfTx);

            // re-processing the coin is idempotent
            if (c.m_createTxId)
            {
                TxID rcvID = *c.m_createTxId;
                storage::restoreTransactionFromShieldedCoin(*db, c, gateway);
                WALLET_CHECK(c.m_createTxId && *c.m_createTxId == rcvID);
            }
        }

        // a coin from our own self-push is linked to that tx
        TxID selfID = saveTx(TxType::PushTransaction, true, true);
        auto c = restoreCoin(selfID, kIsSelfTxBit);
        WALLET_CHECK(c.m_createTxId && *c.m_createTxId == selfID);

        // but not to our push to someone else
        TxID pushID = saveTx(TxType::PushTransaction, true, false);
        c = restoreCoin(pushID, kIsSelfTxBit);
        WALLET_CHECK(c.m_createTxId && *c.m_createTxId != pushID);

        WALLET_CHECK(db->getTxHistory(TxType::PushTransaction).size() == 5);
    }
}

int main()
{
    const int logLevel = BEAM_LOG_LEVEL_WARNING;
    const auto path = boost::filesystem::system_complete("logs");
    auto logger = Logger::create(logLevel, logLevel, logLevel, "peer_input_test", path.string());

    PseudoRandomGenerator prg;
    prg.m_hv = 125U;

    Rules r;
    Rules::Scope scopeRules(r);
    r.m_Consensus = Rules::Consensus::FakePoW;
    r.pForks[1].m_Height = 100500;
    r.DisableForksFrom(2);
    r.UpdateChecksum();

    storage::HookErrors();

    TestUnsolicitedSenderRole();
    TestShieldedCoinTxID();

    boost::filesystem::remove(SenderWalletDB);
    boost::filesystem::remove(ReceiverWalletDB);

    if (g_failureCount == 0)
        cout << "PEER_INPUT_OK sender_role shielded_coin_txid" << endl;
    return WALLET_CHECK_RESULT;
}
