package cash.p.beam

import kotlinx.coroutines.flow.StateFlow

public interface BeamWalletSession {
    public val state: StateFlow<BeamWalletState>
    public val offlineSigningState: StateFlow<BeamOfflineSigningState>
    public val balance: StateFlow<BeamBalance>
    public val transactions: StateFlow<List<BeamTransaction>>

    public suspend fun start()
    public suspend fun stop()
    public suspend fun close()
    public suspend fun receiveAddress(type: BeamAddressType = BeamAddressType.PublicOffline): BeamAddress
    public suspend fun transactionPage(offset: Int, limit: Int): BeamTransactionPage
    /** Core Exact/Max quote, no writes or reservations. Offline requires a stopped owner.
     * Tokens are limited to 65,536 characters; comments to 1,024 UTF-8 bytes.
     */
    public suspend fun quoteSend(request: BeamQuoteRequest): BeamSendQuote
    /** Sign using a durable context after normal sync, without network access or broadcast.
     * Retry the same operationId/request/version after cancellation, lost response or reopen.
     * The receiver and amount may be chosen after disconnecting. No bytes are returned here.
     * Offline operations never enter ordinary recovery or commit. Inputs stay reserved until
     * an explicit pre-export abort. Independent seed copies do not share this guarantee.
     */
    public suspend fun signOffline(
        operationId: String, request: BeamQuoteRequest, quoteVersion: String,
    ): BeamOfflineSignResult
    /** Flushes Exported before returning canonical bytes, including on a lost response.
     * Repeated exports are byte-identical. Closing, stopping, peer rejection and elapsed time
     * cannot revoke these bytes or release their inputs. Limit: 1 MiB, 256 per vector,
     * 1,024 aggregate elements. Foreign relay needs bytes plus network/rules, not this wallet.
     */
    public suspend fun exportSignedTransaction(operationId: String): ByteArray
    public suspend fun previewSend(request: BeamSendRequest): BeamSendPreview
    public suspend fun prepareSend(
        operationId: String,
        request: BeamSendRequest,
        previewVersion: Long,
    ): PreparedBeamSend
    public suspend fun commitSend(operationId: String): BeamSendResolution
    public suspend fun resolveSend(operationId: String): BeamSendResolution
    /** Local observation, including terminal operations for reorg tracking. Works while stopped.
     * Does not submit sends. Throws if the durable inventory cannot be safely interpreted.
     */
    public suspend fun sendOperations(): List<BeamSendOperation>
    /** Explicit Ready-only recovery. May submit previously prepared sends using their stored TxIDs.
     * Conflicting unresolved operations defer recovery; opening or listing never triggers it.
     * Ownership is local to this database, not shared across restored copies or devices.
     */
    public suspend fun recoverSendOperations(): List<BeamSendOperation>
    /** Aborts online Prepared without a Core row, or offline Signing/Signed after owner drain.
     * Offline abort requires Stopped and atomically removes reservations and the unexported row.
     * Returns false for Exported, including a lost export response; it can never revoke bytes.
     */
    public suspend fun abortPrepared(operationId: String): Boolean
}
