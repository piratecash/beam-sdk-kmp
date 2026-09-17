package cash.p.beam.internal

import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The snapshot decoder is strict: an unexpected payload shape becomes a permanent error phase
 * rather than a recoverable hiccup. These cases pin the wire contract that keeps an older native
 * library working, so they rebuild the production configuration rather than a lenient one.
 */
class SnapshotDtoTest {

    private val json = Json {
        ignoreUnknownKeys = false
        explicitNulls = false
    }

    @Test
    fun `a payload from an older native library decodes with safe defaults`() {
        // No balanceLoaded, no syncDone, no syncTotal: the keys this change introduces. A library
        // that predates them must keep working rather than throwing.
        val dto = json.decodeFromString<SnapshotDto>(
            """
            {
              "phase": "Syncing",
              "currentHeight": 100,
              "targetHeight": 200,
              "balance": { "available": 500 }
            }
            """.trimIndent()
        )

        assertFalse(dto.balanceLoaded)
        assertNull(dto.syncDone)
        assertNull(dto.syncTotal)

        val snapshot = dto.toDomain()
        assertEquals(500, snapshot.balance.available)
        // Not Ready, and the old library never reported a load, so nothing may claim the amounts.
        assertFalse(snapshot.balance.isLoaded)
        assertNull(snapshot.syncDone)
        assertNull(snapshot.syncTotal)
    }

    @Test
    fun `a syncing payload carries the loaded balance and the scan counters`() {
        val dto = json.decodeFromString<SnapshotDto>(
            """
            {
              "phase": "Syncing",
              "currentHeight": 100,
              "targetHeight": 200,
              "balance": { "available": 500, "receiving": 20 },
              "balanceLoaded": true,
              "syncDone": 1173,
              "syncTotal": 1226
            }
            """.trimIndent()
        )

        val snapshot = dto.toDomain()

        assertEquals(500, snapshot.balance.available)
        assertEquals(20, snapshot.balance.receiving)
        // The point of the whole change: real amounts while still syncing, not spendable.
        assertTrue(snapshot.balance.isLoaded)
        assertFalse(snapshot.balance.isAuthoritative)
        assertEquals(1173, snapshot.syncDone)
        assertEquals(1226, snapshot.syncTotal)
    }

    @Test
    fun `the quorum counter defaults to zero and decodes when present`() {
        // It never reaches BackendSnapshot: it exists only to make the body-pack re-request loop
        // visible in the log, so the DTO is the only place it can be asserted.
        val absent = json.decodeFromString<SnapshotDto>(
            """
            { "phase": "Syncing", "balance": {} }
            """.trimIndent()
        )
        assertEquals(0, absent.quorumRequests)

        val present = json.decodeFromString<SnapshotDto>(
            """
            {
              "phase": "Syncing",
              "balance": {},
              "syncDone": 10,
              "syncTotal": 1226,
              "quorumRequests": 47
            }
            """.trimIndent()
        )
        assertEquals(47, present.quorumRequests)
    }

    @Test
    fun `a ready payload is authoritative`() {
        val snapshot = json.decodeFromString<SnapshotDto>(
            """
            {
              "phase": "Ready",
              "currentHeight": 200,
              "targetHeight": 200,
              "balance": { "available": 500 },
              "balanceLoaded": true
            }
            """.trimIndent()
        ).toDomain()

        assertTrue(snapshot.balance.isAuthoritative)
        assertTrue(snapshot.balance.isLoaded)
    }

    @Test
    fun `a ready payload from an older native library is still authoritative and loaded`() {
        // balanceLoaded absent: isLoaded is derived from authority, so a synced wallet on an old
        // library still reports a usable balance.
        val snapshot = json.decodeFromString<SnapshotDto>(
            """
            {
              "phase": "Ready",
              "currentHeight": 200,
              "targetHeight": 200,
              "balance": { "available": 500 }
            }
            """.trimIndent()
        ).toDomain()

        assertTrue(snapshot.balance.isAuthoritative)
        assertTrue(snapshot.balance.isLoaded)
    }
}
