package cash.p.beam.internal

import cash.p.beam.BeamAddressType
import cash.p.beam.BeamInspectedTransaction
import cash.p.beam.BeamNetwork
import cash.p.beam.BeamParsedToken
import cash.p.beam.BeamTransactionHeightRange
import cash.p.beam.BeamTransactionRules
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

internal actual object BeamCodecPlatform {
    actual fun parseToken(token: String): BeamParsedToken {
        NativeLibraryLoader.load() // Existing synchronized loader is shared with session JNI.
        val result = Json.parseToJsonElement(BeamCodecNative.parseToken(token)).jsonObject
        return BeamParsedToken(
            BeamAddressType.valueOf(result.getValue("type").jsonPrimitive.content),
            result.getValue("voucherCount").jsonPrimitive.content.toInt(),
        )
    }

    actual fun rules(network: BeamNetwork): BeamTransactionRules {
        NativeLibraryLoader.load()
        return BeamTransactionRules(network, BeamCodecNative.rules(network.ordinal))
    }

    actual fun inspect(bytes: ByteArray, rules: BeamTransactionRules): BeamInspectedTransaction {
        NativeLibraryLoader.load()
        val result = Json.parseToJsonElement(BeamCodecNative.inspect(bytes, rules.network.ordinal, rules.signature)).jsonObject
        fun value(key: String): String = result.getValue(key).jsonPrimitive.content
        return BeamInspectedTransaction(
            rules = rules,
            serializedTransactionHash = value("serializedHash"),
            mainKernelId = value("mainKernelId"),
            mainKernelHeight = BeamTransactionHeightRange(value("mainMin").toULong(), value("mainMax").toULong()),
            validHeight = BeamTransactionHeightRange(value("validMin").toULong(), value("validMax").toULong()),
            ordinaryInputCount = value("ordinaryInputs").toInt(),
            ordinaryOutputCount = value("ordinaryOutputs").toInt(),
            shieldedInputCount = value("shieldedInputs").toInt(),
            kernelCount = value("kernelCount").toInt(),
            serializedSize = bytes.size,
        )
    }
}

internal object BeamCodecNative {
    external fun parseToken(token: String): String
    external fun rules(network: Int): String
    external fun inspect(bytes: ByteArray, network: Int, rules: String): String
}
