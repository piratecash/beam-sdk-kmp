package cash.p.beam.internal

internal actual object NativeLibraryLoader {
    @Volatile
    private var loaded = false

    actual fun load() {
        if (loaded) return
        synchronized(this) {
            if (loaded) return
            System.loadLibrary("beam_sdk_kmp")
            loaded = true
        }
    }
}
