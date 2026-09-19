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

    @Test
    fun `offline context events decode and older payloads default to none`() {
        val dto = json.decodeFromString<SnapshotDto>(
            """
            {
              "phase": "Ready",
              "offlineSigning": {
                "phase": "Preparing", "contextId": "", "height": 0, "shieldedCount": 0,
                "events": [{
                  "seq": 3, "phase": "Preparing", "reason": "progress", "height": 0, "shieldedCount": 0,
                  "boundaryDone": true, "downloadsDone": 1, "downloadsTotal": 2, "proofsDone": 0,
                  "proofsTotal": 1, "awaitingSecondPeer": true
                }]
              }
            }
            """.trimIndent()
        )

        assertEquals(
            "offline context seq=3 phase=Preparing reason=progress height=0 shielded=0 boundary=true" +
                " downloads=1/2 proofs=0/1 awaitingPeer=true",
            dto.offlineSigning.events.single().logLine(),
        )
        val older = json.decodeFromString<SnapshotDto>("""{ "phase": "Ready", "offlineSigning": { "phase": "Ready" } }""")
        assertTrue(older.offlineSigning.events.isEmpty())
    }

    @Test
    fun `offline context lines advance the cursor, report drops and never rewind`() {
        fun events(vararg seqs: Long) = seqs.map { OfflineSigningEventDto(seq = it, reason = "r$it") }

        val (first, firstLines) = offlineEventLines(events(1, 2), lastSeq = 0)
        assertEquals(2, first)
        assertEquals(events(1, 2).map { it.logLine() }, firstLines)

        assertEquals(2L to emptyList(), offlineEventLines(events(1, 2), lastSeq = 2))
        assertEquals(2L to emptyList(), offlineEventLines(emptyList(), lastSeq = 2))

        val (dropped, droppedLines) = offlineEventLines(events(6, 7), lastSeq = 2)
        assertEquals(7, dropped)
        assertEquals(listOf("offline context events dropped=3") + events(6, 7).map { it.logLine() }, droppedLines)

        // A snapshot taken before a concurrent, already logged one must not re-emit its events.
        assertEquals(7L to emptyList(), offlineEventLines(events(4, 5), lastSeq = 7))
    }
}
