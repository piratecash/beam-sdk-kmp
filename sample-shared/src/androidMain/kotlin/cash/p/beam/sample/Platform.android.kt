package cash.p.beam.sample

import java.util.UUID

internal actual fun newOperationId(): String = UUID.randomUUID().toString()
