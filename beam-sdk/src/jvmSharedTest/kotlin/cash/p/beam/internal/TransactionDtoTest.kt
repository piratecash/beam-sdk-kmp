package cash.p.beam.internal

import cash.p.beam.BeamTransactionDirection
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class TransactionDtoTest {

    private val json = Json { ignoreUnknownKeys = true }

    @Test
    fun `payload without a counterparty decodes to null`() {
        val dto = json.decodeFromString<TransactionDto>(
            """
            {
              "id": "0102030405060708090a0b0c0d0e0f10",
              "direction": "Incoming",
              "amount": 100000000,
              "fee": 0,
              "createdAtEpochSeconds": 1700000000,
              "status": "Completed"
            }
            """.trimIndent()
        )

        assertNull(dto.counterparty)
        assertNull(dto.toDomain().counterparty)
    }

    @Test
    fun `counterparty is carried through to the domain model`() {
        val endpoint = "3ZkVRpXZ7dJqQFHRzWq2SbWUzB1xJz9Mk6uQnD4Yc7Tt"
        val dto = json.decodeFromString<TransactionDto>(
            """
            {
              "id": "0102030405060708090a0b0c0d0e0f10",
              "direction": "Incoming",
              "amount": 100000000,
              "fee": 0,
              "createdAtEpochSeconds": 1700000000,
              "status": "Completed",
              "counterparty": "$endpoint"
            }
            """.trimIndent()
        )

        assertEquals(endpoint, dto.counterparty)
        val domain = dto.toDomain()
        assertEquals(endpoint, domain.counterparty)
        assertEquals(BeamTransactionDirection.Incoming, domain.direction)
    }

    @Test
    fun `an explicit null counterparty decodes to null`() {
        val dto = json.decodeFromString<TransactionDto>(
            """
            {
              "id": "0102030405060708090a0b0c0d0e0f10",
              "direction": "Outgoing",
              "amount": 100000000,
              "fee": 1100000,
              "createdAtEpochSeconds": 1700000000,
              "status": "Completed",
              "counterparty": null
            }
            """.trimIndent()
        )

        assertNull(dto.toDomain().counterparty)
    }
}
