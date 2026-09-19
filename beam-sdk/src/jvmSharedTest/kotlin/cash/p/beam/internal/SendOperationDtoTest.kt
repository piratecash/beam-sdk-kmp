package cash.p.beam.internal

import cash.p.beam.BeamOfflineSendState
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SendOperationDtoTest {

    private fun operation(extra: String) = """
        [{
          "operationId": "op",
          "transactionId": "0102030405060708090a0b0c0d0e0f10",
          "requestHash": "hash",
          "amount": 1000,
          "fee": 100,
          "resolution": {"kind": "Prepared", "transactionId": "0102030405060708090a0b0c0d0e0f10"},
          "deliveryMode": "Offline",
          "offlineState": "Exported"$extra
        }]
    """.trimIndent()

    @Test
    fun `creation time is carried through to the domain model`() {
        val decoded = operation(""", "createdAtEpochSeconds": 1700000000""").toSendOperations().single()

        assertEquals(1_700_000_000L, decoded.createdAtEpochSeconds)
        assertEquals(BeamOfflineSendState.Exported, decoded.offlineState)
    }

    @Test
    fun `missing or null creation time decodes to null`() {
        assertNull(operation("").toSendOperations().single().createdAtEpochSeconds)
        assertNull(operation(""", "createdAtEpochSeconds": null""").toSendOperations().single().createdAtEpochSeconds)
    }
}
