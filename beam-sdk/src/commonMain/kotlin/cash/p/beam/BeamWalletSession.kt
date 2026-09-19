package cash.p.beam

import kotlinx.coroutines.flow.StateFlow

public interface BeamWalletSession {
    public val state: StateFlow<BeamWalletState>
    public val offlineSigningState: StateFlow<BeamOfflineSigningState>
    public val balance: StateFlow<BeamBalance>
    /** An offline send appears only once its kernel is observed on chain, as Completed. */
    public val transactions: StateFlow<List<BeamTransaction>>

    public suspend fun start()
    public suspend fun stop()
    public suspend fun close()
    public suspend fun receiveAddress(type: BeamAddressType = BeamAddressType.PublicOffline): BeamAddress
    /** Same view as [transactions]. */
    public suspend fun transactionPage(offset: Int, limit: Int): BeamTransactionPage
    /** Core Exact/Max quote, no writes or reservations. Offline requires a stopped owner.
     * Tokens are limited to 65,536 characters; comments to 1,024 UTF-8 bytes.
     */
    public suspend fun quoteSend(request: BeamQuoteRequest): BeamSendQuote
    /** Sign using a durable context after normal sync, without network access or broadcast.
     * Retry the same operationId/request/version after cancellation, lost response or reopen.
     * The receiver and amount may be chosen after disconnecting. No bytes are returned here.
     * Offline operations never enter ordinary recovery or commit. Coins are reserved only while
     * signing runs: a successful sign releases its inputs and drops the expected change, like an
     * unbroadcast Bitcoin transaction, so whoever spends them first on chain wins. An interrupted
     * sign is discarded by the next open, start or signOffline for another operationId; a retry
     * then signs anew and may need a fresh quote (STALE_QUOTE). Offline and MaxPrivacy tokens carry
     * a limited number of vouchers; a discarded or aborted sign consumes one, so once they run out
     * signing to that token fails offline and the receiver has to provide a new token.
     */
    public suspend fun signOffline(
        operationId: String, request: BeamQuoteRequest, quoteVersion: String,
    ): BeamOfflineSignResult
    /** Flushes Exported before returning canonical bytes, including on a lost response.
     * Repeated exports are byte-identical. Closing, stopping, peer rejection and elapsed time
     * cannot revoke these bytes; they stay valid until their coins are spent elsewhere. Limit: 1 MiB, 256 per vector,
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
     * Offline abort requires Stopped and atomically removes the unexported row and any reservation.
     * Returns false for Exported, including a lost export response; it can never revoke bytes.
     */
    public suspend fun abortPrepared(operationId: String): Boolean
}
