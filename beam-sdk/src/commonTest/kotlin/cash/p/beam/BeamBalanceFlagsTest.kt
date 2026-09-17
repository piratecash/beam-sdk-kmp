package cash.p.beam

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * [BeamBalance.isLoaded] separates "these amounts are real but possibly behind the chain" from
 * "we know nothing yet". The display path may use the former; only [BeamBalance.isAuthoritative]
 * may fund a spend.
 */
class BeamBalanceFlagsTest {

    @Test
    fun `a balance carrying nothing is neither loaded nor authoritative`() {
        val balance = BeamBalance()

        assertFalse(balance.isLoaded)
        assertFalse(balance.isAuthoritative)
    }

    @Test
    fun `a database-loaded balance is loaded but not spendable`() {
        val balance = BeamBalance(available = 100, loadedFromDatabase = true)

        assertTrue(balance.isLoaded)
        assertFalse(balance.isAuthoritative)
    }

    @Test
    fun `an authoritative balance is loaded even when the native library never said so`() {
        // A native library that predates balanceLoaded leaves loadedFromDatabase false. Deriving
        // isLoaded keeps such a build behaving exactly as it does today instead of never
        // publishing a balance at all.
        val balance = BeamBalance(available = 100, isAuthoritative = true)

        assertTrue(balance.isLoaded)
    }

    @Test
    fun `dropping authority keeps the amounts and keeps them loaded`() {
        // The stop path and the three re-sync paths all narrow authority with copy(). None of
        // them may claim the amounts stopped being real.
        val authoritative = BeamBalance(
            available = 100,
            receiving = 20,
            loadedFromDatabase = true,
            isAuthoritative = true,
        )

        val narrowed = authoritative.copy(isAuthoritative = false)

        assertEquals(100, narrowed.available)
        assertEquals(20, narrowed.receiving)
        assertTrue(narrowed.isLoaded)
        assertFalse(narrowed.isAuthoritative)
    }

    @Test
    fun `authoritative implies loaded for every combination`() {
        // The invariant the display and spend split rests on, asserted exhaustively rather than
        // by example: it cannot be violated by construction because isLoaded is derived.
        for (loadedFromDatabase in listOf(false, true)) {
            val balance = BeamBalance(
                loadedFromDatabase = loadedFromDatabase,
                isAuthoritative = true,
            )
            assertTrue(balance.isLoaded, "authoritative balance must report isLoaded")
        }
    }
}
