package cash.p.beam.sample

import java.nio.file.Files
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DemoSendJournalTest {
    @Test
    fun operationId_survivesControllerRestartUntilCleared() {
        val directory = createTempDirectory("beam-demo-journal")
        val owner = ActiveWalletIdentity(cash.p.beam.BeamNetwork.Mainnet, directory.resolve("recovery/full").toString())
        try {
            assertTrue(DemoSendJournal.claim(directory.toString(), owner, OPERATION_ID))

            assertEquals(
                DemoPendingSend(owner, OPERATION_ID),
                DemoSendJournal.load(directory.toString(), cash.p.beam.BeamNetwork.Testnet),
            )
            assertFalse(DemoSendJournal.claim(directory.toString(), owner, OTHER_OPERATION_ID))
            assertEquals(
                DemoPendingSend(owner, OPERATION_ID),
                DemoSendJournal.load(directory.toString(), cash.p.beam.BeamNetwork.Testnet),
            )

            DemoSendJournal.clear(directory.toString(), owner, OTHER_OPERATION_ID)
            assertEquals(
                DemoPendingSend(owner, OPERATION_ID),
                DemoSendJournal.load(directory.toString(), cash.p.beam.BeamNetwork.Testnet),
            )
            DemoSendJournal.clear(directory.toString(), owner, OPERATION_ID)
            assertNull(DemoSendJournal.load(directory.toString(), cash.p.beam.BeamNetwork.Testnet))
        } finally {
            Files.walk(directory).use { paths ->
                paths.sorted(Comparator.reverseOrder()).forEach(Files::deleteIfExists)
            }
        }
    }

    @Test
    fun emptyPublishedJournal_failsClosed() {
        val directory = createTempDirectory("beam-demo-empty-journal")
        try {
            Files.createFile(directory.resolve(".demo-active-send"))

            assertFailsWith<IllegalArgumentException> {
                DemoSendJournal.load(directory.toString(), cash.p.beam.BeamNetwork.Mainnet)
            }
            assertFalse(
                DemoSendJournal.claim(
                    directory.toString(),
                    ActiveWalletIdentity(cash.p.beam.BeamNetwork.Mainnet, directory.toString()),
                    OPERATION_ID,
                ),
            )
        } finally {
            Files.walk(directory).use { paths ->
                paths.sorted(Comparator.reverseOrder()).forEach(Files::deleteIfExists)
            }
        }
    }

    @Test
    fun windowsPublicationPath_forcesTheJournalFileWithoutOpeningTheDirectory() {
        val directory = createTempDirectory("beam-demo-windows-journal")
        val originalOsName = System.getProperty("os.name")
        val owner = ActiveWalletIdentity(cash.p.beam.BeamNetwork.Mainnet, directory.toString())
        try {
            System.setProperty("os.name", "Windows 11")

            assertTrue(DemoSendJournal.claim(directory.toString(), owner, OPERATION_ID))
            assertEquals(
                DemoPendingSend(owner, OPERATION_ID),
                DemoSendJournal.load(directory.toString(), cash.p.beam.BeamNetwork.Mainnet),
            )
            DemoSendJournal.clear(directory.toString(), owner, OPERATION_ID)
            assertNull(DemoSendJournal.load(directory.toString(), cash.p.beam.BeamNetwork.Mainnet))
        } finally {
            System.setProperty("os.name", originalOsName)
            Files.walk(directory).use { paths ->
                paths.sorted(Comparator.reverseOrder()).forEach(Files::deleteIfExists)
            }
        }
    }

    private companion object {
        const val OPERATION_ID = "123e4567-e89b-42d3-a456-426614174000"
        const val OTHER_OPERATION_ID = "223e4567-e89b-42d3-a456-426614174000"
    }
}
