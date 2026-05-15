plugins {
    java
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "io.aiven.kafka.connect"
version = "0.1.0"

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

repositories {
    mavenCentral()
}

dependencies {
    // Kafka Connect API — provided at runtime by Connect
    compileOnly("org.apache.kafka:connect-api:3.9.0")
    compileOnly("org.apache.kafka:connect-transforms:3.9.0")

    // Jackson for JSON serialization
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.17.2")

    // HTTP client (Java 11 built-in, no extra dep needed)
    // SLF4J for logging (provided by Connect runtime)
    compileOnly("org.slf4j:slf4j-api:2.0.13")

    // Test dependencies
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.10.3")
    testImplementation("org.apache.kafka:connect-api:3.9.0")
    testImplementation("org.apache.kafka:connect-transforms:3.9.0")
    testImplementation("org.slf4j:slf4j-api:2.0.13")
    testImplementation("org.slf4j:slf4j-simple:2.0.13")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
}

tasks.test {
    useJUnitPlatform()
}

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    archiveBaseName.set("openlineage-smt")
    archiveClassifier.set("all")
    archiveVersion.set("")

    // Don't include Kafka Connect API (provided at runtime)
    dependencies {
        exclude(dependency("org.apache.kafka:.*"))
        exclude(dependency("org.slf4j:.*"))
    }

    mergeServiceFiles()
}

tasks.named("build") {
    dependsOn("shadowJar")
}
