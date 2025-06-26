plugins {
    id("java")
}

group = "com.alshubaily.chess"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")

    val btVersion = "1.10"
    implementation("com.github.atomashpolskiy:bt-core:${btVersion}")
    implementation("com.github.atomashpolskiy:bt-http-tracker-client:${btVersion}")
    implementation("com.github.atomashpolskiy:bt-dht:${btVersion}")

    implementation("software.amazon.awssdk:s3:2.25.13")
    implementation("software.amazon.awssdk:s3-transfer-manager:2.31.65")

    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.1")
    implementation("com.github.luben:zstd-jni:1.5.5-10")
    implementation("org.apache.avro:avro:1.11.3")
    implementation("org.xerial.snappy:snappy-java:1.1.10.1")

    implementation("io.github.cdimascio:dotenv-java:3.0.0")
}

tasks.test {
    useJUnitPlatform()
}