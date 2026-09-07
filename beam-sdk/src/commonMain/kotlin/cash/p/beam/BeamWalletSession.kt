package cash.p.beam

import kotlinx.coroutines.flow.StateFlow

public interface BeamWalletSession {
    public val state: StateFlow<BeamWalletState>
    public val balance: StateFlow<BeamBalance>
    public val transactions: StateFlow<List<BeamTransaction>>

    public suspend fun start()
    public suspend fun stop()
    public suspend fun close()
    public suspend fun receiveAddress(type: BeamAddressType = BeamAddressType.PublicOffline): BeamAddress
    public suspend fun transactionPage(offset: Int, limit: Int): BeamTransactionPage
    public suspend fun previewSend(request: BeamSendRequest): BeamSendPreview
    public suspend fun prepareSend(
        operationId: String,
        request: BeamSendRequest,
        previewVersion: Long,
    ): PreparedBeamSend
    public suspend fun commitSend(operationId: String): BeamSendResolution
    public suspend fun resolveSend(operationId: String): BeamSendResolution
    public suspend fun abortPrepared(operationId: String): Boolean
}
