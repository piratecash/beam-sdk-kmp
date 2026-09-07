import com.android.build.api.variant.KotlinMultiplatformAndroidComponentsExtension
import org.gradle.api.DefaultTask
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
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
