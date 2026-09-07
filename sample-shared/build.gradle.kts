import java.util.Properties
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.android.kmp.library)
    alias(libs.plugins.compose.multiplatform)
    alias(libs.plugins.compose.compiler)
}

// local.properties is ignored by Git and generates source only under build/.
// No mnemonic is kept in repository sources.
val generateDemoConfig = tasks.register<GenerateDemoConfig>("generateDemoConfig") {
    localProperties.from(rootProject.layout.projectDirectory.file("local.properties"))
    outputDirectory.set(layout.buildDirectory.dir("generated/demo"))
}

kotlin {
    androidLibrary {
        namespace = "cash.p.beam.sample.shared"
        compileSdk = 36
        minSdk = 27
        androidResources.enable = true
        compilerOptions { jvmTarget.set(JvmTarget.JVM_17) }
    }

    jvm("desktop") {
        compilerOptions { jvmTarget.set(JvmTarget.JVM_21) }
    }

    applyDefaultHierarchyTemplate()

    sourceSets {
        val jvmSharedMain by creating {
            dependsOn(commonMain.get())
        }
        getByName("androidMain").dependsOn(jvmSharedMain)
        getByName("desktopMain").dependsOn(jvmSharedMain)

        commonMain {
            kotlin.srcDir(generateDemoConfig)
            dependencies {
                api(project(":beam-sdk"))
                implementation(libs.compose.runtime)
                implementation(libs.compose.foundation)
                implementation(libs.compose.material3)
                implementation(libs.kotlinx.coroutines.core)
                implementation(libs.hd.wallet.kit)
                implementation(libs.kermit)
            }
        }
        commonTest.dependencies {
            implementation(kotlin("test"))
            implementation(libs.kotlinx.coroutines.test)
        }
    }
}

abstract class GenerateDemoConfig : DefaultTask() {
    @get:InputFiles
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val localProperties: ConfigurableFileCollection

    @get:OutputDirectory
    abstract val outputDirectory: DirectoryProperty

    @TaskAction
    fun generate() {
        val properties = Properties()
        localProperties.files.filter { it.isFile }.forEach { file ->
            file.inputStream().use(properties::load)
        }

        fun literal(key: String): String = properties.getProperty(key).orEmpty().trim()
            .removeSurrounding("\"")
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")

        val directory = outputDirectory.get().asFile.resolve("cash/p/beam/sample")
        directory.mkdirs()
        directory.resolve("DemoConfig.kt").writeText(
            """
            package cash.p.beam.sample

            internal object DemoConfig {
                const val WORDS: String = "${literal("words")}"
                const val DATABASE_KEY_HEX: String = "${literal("beam.databaseKey")}"
                const val NETWORK: String = "${literal("beam.network")}"
                const val STORAGE_PATH: String = "${literal("beam.storagePath")}"
            }

            """.trimIndent(),
        )
    }
}
