package cash.p.beam.sample

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.NavigationBar
import androidx.compose.material3.NavigationBarItem
import androidx.compose.material3.Scaffold
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.collectLatest

private enum class DemoDestination(val title: String, val glyph: String) {
    Wallet("Wallet", "◎"),
    Transfer("Transfer", "⇅"),
    Transactions("Transactions", "≡"),
}

public class BeamDemoHandle internal constructor(
    internal val controller: BeamDemoController,
) {
    private var initialized: Boolean = false

    internal fun initializeOnce() {
        if (initialized) return
        initialized = true
        controller.openOrCreate()
    }

    public fun close() {
        controller.close()
    }

    public suspend fun closeAndJoin() {
        controller.closeAndJoin()
    }
}

public fun createBeamDemoHandle(initialStoragePath: String): BeamDemoHandle = BeamDemoHandle(
    BeamDemoController(
        initialStoragePath = initialStoragePath,
        initialState = configuredDemoState(initialStoragePath),
    ),
)

@OptIn(ExperimentalMaterial3Api::class)
@Composable
public fun BeamDemoApp(
    initialStoragePath: String,
    hostForeground: StateFlow<Boolean>? = null,
) {
    val handle = remember(initialStoragePath) { createBeamDemoHandle(initialStoragePath) }
    DisposableEffect(handle) { onDispose { handle.close() } }
    BeamDemoApp(handle, hostForeground)
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
public fun BeamDemoApp(
    handle: BeamDemoHandle,
    hostForeground: StateFlow<Boolean>? = null,
) {
    val controller = handle.controller
    val state by controller.state.collectAsState()
    var destination by remember { mutableStateOf(DemoDestination.Wallet) }
    val snackbarHostState = remember { SnackbarHostState() }

    LaunchedEffect(controller, hostForeground) {
        hostForeground?.collectLatest { foreground ->
            if (foreground) controller.onHostForeground() else controller.onHostBackground()
        }
    }
    LaunchedEffect(handle) {
        handle.initializeOnce()
    }
    LaunchedEffect(state.message) {
        val message = state.message ?: return@LaunchedEffect
        snackbarHostState.showSnackbar(message)
        controller.dismissMessage()
    }
    MaterialTheme {
        Scaffold(
            topBar = {
                TopAppBar(
                    title = { Text("Beam Kit demo") },
                )
            },
            bottomBar = {
                NavigationBar {
                    DemoDestination.entries.forEach { item ->
                        NavigationBarItem(
                            selected = destination == item,
                            onClick = { destination = item },
                            icon = { Text(item.glyph) },
                            label = { Text(item.title) },
                        )
                    }
                }
            },
            snackbarHost = { SnackbarHost(snackbarHostState) },
        ) { padding ->
            Box(Modifier.padding(padding)) {
                when (destination) {
                    DemoDestination.Wallet -> BeamWalletScreen(state, controller)
                    DemoDestination.Transfer -> BeamTransferScreen(state, controller)
                    DemoDestination.Transactions -> BeamTransactionsScreen(state.transactions)
                }
                if (state.busy) LinearProgressIndicator()
            }
        }
    }
}
