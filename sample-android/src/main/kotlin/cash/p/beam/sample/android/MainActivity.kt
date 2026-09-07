package cash.p.beam.sample.android

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewmodel.compose.viewModel
import cash.p.beam.sample.BeamDemoApp
import cash.p.beam.sample.BeamDemoHandle
import cash.p.beam.sample.createBeamDemoHandle
import kotlinx.coroutines.flow.MutableStateFlow

public class MainActivity : ComponentActivity() {
    private val hostForeground = MutableStateFlow(false)

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val storagePath = filesDir.resolve("beam-testnet").absolutePath
        setContent {
            val demoViewModel: BeamDemoViewModel = viewModel { BeamDemoViewModel(storagePath) }
            BeamDemoApp(demoViewModel.handle, hostForeground)
        }
    }

    override fun onStart() {
        super.onStart()
        hostForeground.value = true
    }

    override fun onStop() {
        hostForeground.value = false
        super.onStop()
    }
}

private class BeamDemoViewModel(storagePath: String) : ViewModel() {
    val handle: BeamDemoHandle = createBeamDemoHandle(storagePath)

    override fun onCleared() {
        handle.close()
    }
}
