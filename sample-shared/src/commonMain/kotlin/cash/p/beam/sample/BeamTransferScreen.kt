package cash.p.beam.sample

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.FilterChip
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.platform.LocalClipboardManager
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.unit.dp
import cash.p.beam.BeamAddressType
import cash.p.beam.BeamWalletState

@Composable
internal fun BeamTransferScreen(state: DemoState, controller: BeamDemoController) {
    val clipboard = LocalClipboardManager.current
    val canReceive = state.activeWallet != null && !state.busy
    val canSend = state.walletState is BeamWalletState.Ready && !state.busy
    Column(
        modifier = Modifier.fillMaxWidth().verticalScroll(rememberScrollState()).padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp),
    ) {
        InfoRow("State", state.walletState.summary())
        InfoRow("Spendable balance", state.balance.available.beam())

        Text("Receive", style = MaterialTheme.typography.titleLarge)
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            BeamAddressType.entries.forEach { type ->
                FilterChip(
                    selected = state.receiveAddressType == type,
                    onClick = { controller.selectReceiveAddressType(type) },
                    enabled = !state.busy,
                    label = { Text(type.label()) },
                )
            }
        }
        Text(state.receiveAddressType.description(), style = MaterialTheme.typography.bodySmall)
        state.address?.let { address ->
            OutlinedTextField(
                value = address.token,
                onValueChange = {},
                label = { Text("${address.type.label()} token · select and copy") },
                modifier = Modifier.fillMaxWidth(),
                readOnly = true,
                minLines = 3,
                maxLines = 3,
            )
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                Button(onClick = { clipboard.setText(AnnotatedString(address.token)) }) {
                    Text("Copy token")
                }
                Button(controller::receiveAddress, enabled = canReceive) {
                    Text("Generate another")
                }
            }
        } ?: Text(
            if (canReceive) "Generating receive token…" else "Wallet is initializing…",
            style = MaterialTheme.typography.bodySmall,
        )

        Text("Send", style = MaterialTheme.typography.titleLarge)
        if (!canSend) {
            Text("Sending is available when the wallet reaches Ready.", style = MaterialTheme.typography.bodySmall)
        }
        DemoField("Receiver token", state.receiver) { value ->
            controller.edit { copy(receiver = value, preview = null) }
        }
        DemoField("Amount in atomic units", state.amount) { value ->
            controller.edit { copy(amount = value, preview = null) }
        }
        DemoField("Comment", state.comment) { value ->
            controller.edit { copy(comment = value, preview = null) }
        }
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            Button(controller::previewSend, enabled = canSend) { Text("Preview") }
            Button(controller::confirmSend, enabled = canSend && state.preview != null) {
                Text("Confirm and send")
            }
        }
        state.preview?.let {
            Text("Fee ${it.fee.beam()} · total ${it.total.beam()} · ${it.receiverType.label()}")
        }
        state.sendResolution?.let { Text("Send: $it") }
    }
}

private fun BeamAddressType.label(): String = when (this) {
    BeamAddressType.PublicOffline -> "Public offline"
    BeamAddressType.Offline -> "Offline"
    BeamAddressType.MaxPrivacy -> "Max privacy"
}

private fun BeamAddressType.description(): String = when (this) {
    BeamAddressType.PublicOffline ->
        "Permanent reusable token with lower privacy. Confirm that the withdrawal provider accepts BEAM payment tokens."
    BeamAddressType.Offline ->
        "One-sided token containing vouchers for up to 10 payments while the receiver is offline."
    BeamAddressType.MaxPrivacy ->
        "Single-payment token using the maximum anonymity set; settlement can take up to 72 hours."
}
