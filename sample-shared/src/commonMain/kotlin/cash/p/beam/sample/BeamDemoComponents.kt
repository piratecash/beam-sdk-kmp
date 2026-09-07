package cash.p.beam.sample

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.input.PasswordVisualTransformation
import cash.p.beam.BEAM_DECIMALS
import cash.p.beam.BeamWalletState

@Composable
internal fun InfoRow(label: String, value: String) {
    Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
        Text(label)
        Text(value)
    }
}

@Composable
internal fun DemoField(
    label: String,
    value: String,
    sensitive: Boolean = false,
    update: (String) -> Unit,
) {
    OutlinedTextField(
        value = value,
        onValueChange = update,
        label = { Text(label) },
        modifier = Modifier.fillMaxWidth(),
        singleLine = true,
        visualTransformation = if (sensitive) PasswordVisualTransformation() else androidx.compose.ui.text.input.VisualTransformation.None,
    )
}

internal fun BeamWalletState.summary(): String = when (this) {
    BeamWalletState.Closed -> "Closed"
    BeamWalletState.Stopped -> "Stopped"
    BeamWalletState.Connecting -> "Connecting to official nodes"
    is BeamWalletState.Offline -> "Offline · last height ${lastKnownHeight ?: "unknown"}"
    is BeamWalletState.Ready -> "Ready · height $height"
    is BeamWalletState.Syncing -> "Syncing blockchain $currentHeight / $targetHeight"
    is BeamWalletState.Error -> "Error · ${failure.message}"
    is BeamWalletState.Restoring -> buildString {
        append("Restoring · ${progress.phase}")
        if (progress.currentHeight != null || progress.targetHeight != null) {
            append(" · ${progress.currentHeight ?: 0} / ${progress.targetHeight ?: "?"}")
        }
        if (progress.downloadedBytes != null) {
            append(" · ${progress.downloadedBytes}")
            progress.totalBytes?.let { append(" / $it bytes") }
        }
    }
}

internal fun Long.beam(): String {
    val negative = this < 0
    val magnitude = if (this == Long.MIN_VALUE) "9223372036854775808" else kotlin.math.abs(this).toString()
    val padded = magnitude.padStart(BEAM_DECIMALS + 1, '0')
    val whole = padded.dropLast(BEAM_DECIMALS)
    val fraction = padded.takeLast(BEAM_DECIMALS).trimEnd('0')
    return buildString {
        if (negative) append('-')
        append(whole)
        if (fraction.isNotEmpty()) append('.').append(fraction)
        append(" BEAM")
    }
}
