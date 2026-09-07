package cash.p.beam.sample

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import cash.p.beam.BeamWalletState

@Composable
internal fun BeamWalletScreen(state: DemoState, controller: BeamDemoController) {
    val canStart = state.activeWallet != null && state.walletState is BeamWalletState.Stopped && !state.busy
    val canStop = state.activeWallet != null &&
        state.walletState !is BeamWalletState.Stopped &&
        state.walletState !is BeamWalletState.Closed
    Column(
        modifier = Modifier.fillMaxWidth().verticalScroll(rememberScrollState()).padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp),
    ) {
        Text("Wallet status", style = MaterialTheme.typography.titleLarge)
        Card(Modifier.fillMaxWidth()) {
            Column(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
                InfoRow("Network", state.activeWallet?.network?.name ?: state.network.name)
                InfoRow("State", state.walletState.summary())
                InfoRow("Spendable balance", state.balance.available.beam())
                if (state.balance.shielded != 0L) {
                    InfoRow("Shielded (included)", state.balance.shielded.beam())
                }
                if (state.balance.receiving != 0L) {
                    InfoRow("Receiving", state.balance.receiving.beam())
                }
                if (state.balance.sending != 0L) {
                    InfoRow("Sending", state.balance.sending.beam())
                }
                if (state.balance.maturing != 0L) {
                    InfoRow("Maturing", state.balance.maturing.beam())
                }
                if (!state.balance.isAuthoritative) {
                    Text(
                        "Balance becomes final when blockchain sync completes.",
                        style = MaterialTheme.typography.bodySmall,
                    )
                }
            }
        }
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            Button(controller::start, enabled = canStart) { Text("Start") }
            Button(controller::stop, enabled = canStop) { Text("Stop") }
        }

        HorizontalDivider()
        Text("Recovery", style = MaterialTheme.typography.titleMedium)
        DemoField("Restore height", state.restoreHeight) { value -> controller.edit { copy(restoreHeight = value) } }
        DemoField("Restore date (YYYY-MM-DD)", state.restoreDate) { value ->
            controller.edit { copy(restoreDate = value) }
        }
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            Button(controller::restoreHeight, enabled = !state.busy) { Text("By height") }
            Button(controller::restoreDate, enabled = !state.busy) { Text("By date") }
            Button(controller::restoreFull, enabled = !state.busy) { Text("Full scan") }
        }
        DemoField("Snapshot URL (blank = official)", state.snapshotUrl) { value ->
            controller.edit { copy(snapshotUrl = value) }
        }
        DemoField("Trusted snapshot SHA-256 (optional)", state.snapshotSha256) { value ->
            controller.edit { copy(snapshotSha256 = value) }
        }
        Button(controller::restoreSnapshot, enabled = !state.busy) { Text("Snapshot then scan") }

        if (state.walletState !is BeamWalletState.Ready && state.activeWallet != null) {
            Text(
                "Receive tokens are available while syncing. Sending unlocks after Ready.",
                style = MaterialTheme.typography.bodySmall,
            )
        }
    }
}
