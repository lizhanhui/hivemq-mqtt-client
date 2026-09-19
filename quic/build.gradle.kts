plugins {
    id("java-platform")
    id("io.github.sgtsilvio.gradle.maven-central-publishing")
}

/* ******************** metadata ******************** */

description = "Adds dependencies for the HiveMQ MQTT Client QUIC module"

metadata {
    moduleName.set("com.hivemq.client.mqtt.quic")
    readableName.set("HiveMQ MQTT Client QUIC module")
}

/* ******************** dependencies ******************** */

javaPlatform {
    allowDependencies()
}

dependencies {
    api(rootProject)
    "runtime"(libs.netty.codec.classes.quic)
    for (classifierName in listOf("linux-x86_64", "linux-aarch_64", "osx-x86_64", "osx-aarch_64", "windows-x86_64")) {
        "runtime"(variantOf(libs.netty.codec.native.quic) { classifier(classifierName) })
    }
}
