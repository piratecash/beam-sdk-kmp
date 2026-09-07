package cash.p.beam.sample

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.itemsIndexed
import androidx.compose.material3.Card
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import cash.p.beam.BeamTransaction

@Composable
internal fun BeamTransactionsScreen(transactions: List<BeamTransaction>) {
    if (transactions.isEmpty()) {
        Column(Modifier.fillMaxWidth().padding(16.dp)) {
            Text("Transactions", style = MaterialTheme.typography.titleLarge)
            Text("No BEAM transactions yet.")
        }
        return
    }
    LazyColumn(
        modifier = Modifier.fillMaxWidth(),
        verticalArrangement = Arrangement.spacedBy(8.dp),
        contentPadding = PaddingValues(16.dp),
    ) {
        itemsIndexed(transactions, key = { _, transaction -> transaction.id }) { index, transaction ->
            TransactionRow(index, transaction)
        }
    }
}

@Composable
private fun TransactionRow(index: Int, transaction: BeamTransaction) {
    Card(Modifier.fillMaxWidth()) {
        Column(Modifier.padding(12.dp), verticalArrangement = Arrangement.spacedBy(4.dp)) {
            Text("#${index + 1} · ${transaction.direction} · ${transaction.status}")
            InfoRow("Amount", transaction.amount.beam())
            InfoRow("Fee", transaction.fee.beam())
            transaction.proofHeight?.let { InfoRow("Proof height", it.toString()) }
            Text(transaction.id, style = MaterialTheme.typography.bodySmall)
            transaction.failureReason?.let { Text("Failure: $it", color = MaterialTheme.colorScheme.error) }
        }
    }
}
