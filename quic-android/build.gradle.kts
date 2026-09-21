plugins {
    id("java-platform")
    id("io.github.sgtsilvio.gradle.maven-central-publishing")
}

/* ******************** metadata ******************** */

description = "Adds Android AAR dependencies for MQTT over QUIC"

metadata {
    moduleName.set("com.hivemq.client.mqtt.quic.android")
    readableName.set("HiveMQ MQTT Client QUIC Android module")
}

/* ******************** dependencies ******************** */

javaPlatform {
    allowDependencies()
}

dependencies {
    api(rootProject)
    "runtime"(libs.netty.codec.classes.quic)
    // Fat AAR: jni/arm64-v8a + jni/armeabi-v7a. Do not also pull linux-aarch_64 (glibc).
    "runtime"("io.netty:netty-codec-native-quic:${libs.versions.netty.get()}:android@aar")
}
