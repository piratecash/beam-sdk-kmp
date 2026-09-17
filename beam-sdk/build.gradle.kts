import com.android.build.api.variant.KotlinMultiplatformAndroidComponentsExtension
import org.gradle.api.DefaultTask
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.testing.Test
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

abstract class VerifyNativeArtifactsTask : DefaultTask() {
    @get:InputDirectory
    @get:PathSensitive(PathSensitivity.RELATIVE)
    abstract val prebuiltDirectory: DirectoryProperty

    @get:Input
    abstract val androidArtifacts: ListProperty<String>

    @get:Input
    abstract val desktopArtifacts: ListProperty<String>

    @TaskAction
    fun verifyArtifacts() {
        val root = prebuiltDirectory.get()
        fun missing(paths: List<String>): List<String> = paths.filterNot { relativePath ->
            val artifact = root.file(relativePath).asFile
            artifact.isFile && artifact.length() > 0L
        }

        val androidMissing = missing(androidArtifacts.get())
        val desktopMissing = missing(desktopArtifacts.get())
        check(androidMissing.isEmpty() && desktopMissing.isEmpty()) {
            "Missing native artifacts: Android=$androidMissing desktop=$desktopMissing"
        }
    }
}

plugins {
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.android.kmp.library)
    alias(libs.plugins.kotlin.serialization)
    `maven-publish`
}

group = rootProject.group
version = rootProject.version

val prebuiltDesktop = layout.projectDirectory.dir("prebuilt/desktop")
val prebuiltAndroid = layout.projectDirectory.dir("prebuilt/android/native")

kotlin {
    androidLibrary {
        namespace = "cash.p.beam"
        compileSdk = 36
        minSdk = 27
        withHostTest { }

        compilerOptions {
            jvmTarget.set(JvmTarget.JVM_17)
        }
    }

    jvm("desktop") {
        compilerOptions {
            jvmTarget.set(JvmTarget.JVM_21)
        }
    }

    applyDefaultHierarchyTemplate()

    sourceSets {
        val jvmSharedMain by creating {
            dependsOn(commonMain.get())
            dependencies {
                implementation(libs.ktor.client.core)
            }
        }
        val jvmSharedTest by creating {
            dependsOn(commonTest.get())
        }

        getByName("androidMain").apply {
            dependsOn(jvmSharedMain)
            dependencies {
                implementation(libs.ktor.client.okhttp)
            }
        }
        getByName("desktopMain").apply {
            dependsOn(jvmSharedMain)
            dependencies {
                implementation(libs.ktor.client.cio)
            }
            resources.srcDir(prebuiltDesktop)
        }
        getByName("androidHostTest").dependsOn(jvmSharedTest)
        getByName("desktopTest").dependsOn(jvmSharedTest)

        commonMain {
            dependencies {
                api(libs.kotlinx.coroutines.core)
                api(libs.kotlinx.datetime)
                implementation(libs.kotlinx.serialization.json)
                implementation(libs.kermit)
            }
        }
        commonTest {
            dependencies {
                implementation(kotlin("test"))
                implementation(libs.kotlinx.coroutines.test)
                implementation(libs.ktor.client.mock)
            }
        }
    }
}

extensions.configure<KotlinMultiplatformAndroidComponentsExtension> {
    onVariants { variant ->
        if (prebuiltAndroid.asFile.isDirectory) {
            variant.sources.jniLibs?.addStaticSourceDirectory(prebuiltAndroid.asFile.absolutePath)
        }
    }
}

val requiredAndroidAbis = providers.gradleProperty("beamSdk.androidAbis")
    .get()
    .split(',')
    .map(String::trim)

tasks.register<VerifyNativeArtifactsTask>("verifyNativeArtifacts") {
    group = "verification"
    description = "Fails unless the complete Android and desktop Beam native matrix is staged."
    prebuiltDirectory.set(layout.projectDirectory.dir("prebuilt"))
    androidArtifacts.set(requiredAndroidAbis.map { "android/native/$it/libbeam_sdk_kmp.so" })
    desktopArtifacts.set(
        listOf(
            "desktop/native/aarch64-apple-darwin/libbeam_sdk_kmp.dylib",
            "desktop/native/x86_64-unknown-linux-gnu/libbeam_sdk_kmp.so",
            "desktop/native/x86_64-pc-windows-msvc/beam_sdk_kmp.dll",
        ),
    )
}

// Each of these opens a Beam session and asserts on the process-global logger, so each needs a
// JVM of its own. Kept in one place because desktopTest excludes exactly what desktopLoggingTest
// includes; a class in neither, or in both, would silently lose the isolation.
val BEAM_LOGGING_TEST_CLASSES = listOf(
    "cash.p.beam.internal.CoreLoggingDisabledNativeTest",
    "cash.p.beam.internal.CoreLoggingEnabledNativeTest",
    "cash.p.beam.internal.CoreLoggingFirstOpenWinsNativeTest",
)

tasks.named<Test>("desktopTest") {
    val externalFixtureConfigured =
        providers.environmentVariable("BEAM_SNAPSHOT_REORG_FIXTURE").orNull?.isNotBlank() == true ||
            providers.environmentVariable("BEAM_SEND_ADMISSION_FIXTURE").orNull?.isNotBlank() == true ||
            providers.environmentVariable("BEAM_OFFLINE_HISTORY_FIXTURE").orNull?.isNotBlank() == true ||
            providers.environmentVariable("BEAM_EXPECT_SNAPSHOT_REORG_FIXTURE").orNull == "1" ||
            providers.environmentVariable("BEAM_EXPECT_SEND_ADMISSION_FIXTURE").orNull == "1" ||
            providers.environmentVariable("BEAM_EXPECT_OFFLINE_HISTORY_FIXTURE").orNull == "1"
    if (externalFixtureConfigured) {
        // External JNI fixtures are neither source-set inputs nor task outputs. Always execute the
        // configured tests so replacing a library cannot leave a stale UP-TO-DATE or cached result.
        outputs.upToDateWhen { false }
        outputs.doNotCacheIf("A desktop JNI fixture test is externally configured") { true }
    }
    // Beam core freezes its logger on the first open in a process, so a logging assertion is only
    // meaningful in a JVM no other session has opened. These classes run in desktopLoggingTest.
    filter { BEAM_LOGGING_TEST_CLASSES.forEach(::excludeTestsMatching) }
}

// One scenario per class AND one JVM per class: forkEvery alone forks per class, so two scenarios
// sharing a class would still share a frozen logger.
val desktopLoggingTest by tasks.registering(Test::class) {
    val desktop = tasks.named<Test>("desktopTest").get()
    description = "Runs the Beam core logging tests, each in its own JVM."
    group = "verification"
    testClassesDirs = desktop.testClassesDirs
    classpath = desktop.classpath
    forkEvery = 1
    filter {
        BEAM_LOGGING_TEST_CLASSES.forEach(::includeTestsMatching)
        isFailOnNoMatchingTests = true
    }
    // The native library is neither a source-set input nor a task output here.
    outputs.upToDateWhen { false }
    outputs.doNotCacheIf("Loads an externally built native library") { true }
}

tasks.named("check") { dependsOn(desktopLoggingTest) }
