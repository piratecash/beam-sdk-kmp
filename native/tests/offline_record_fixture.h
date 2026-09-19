// Shared synthetic offline send record. Include after beam_jni.cpp. It decodes like a real record;
// its signed evidence is fake, so inventory validation still rejects Signed and Exported ones.
inline SendRecord syntheticOfflineRecord(const std::string& operationId, const TxID& txId,
    const std::string& receiver, std::uint64_t amount, std::uint64_t fee, SendState state,
    const std::string& requestHash) {
    SendRecord record;
    record.offline = true;
    record.operationId = operationId;
    record.txId = txIdString(txId);
    record.receiver = receiver;
    record.amount = amount;
    record.fee = record.explicitFee = fee;
    record.state = state;
    record.requestHash = requestHash;
    record.contextId = record.rules = "synthetic";
    record.quoteVersion = std::string(64, '0');
    if (state != SendState::Signing) {
        record.rawHex = "00";
        record.mainKernelId = record.serializedHash = std::string(64, '0');
    }
    return record;
}
