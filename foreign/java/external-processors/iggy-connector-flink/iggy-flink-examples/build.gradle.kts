/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

plugins {
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

repositories {
    mavenCentral()
}

val flinkVersion = "2.1.0"

application {
    mainClass.set("org.apache.iggy.flink.example.StreamTransformJob")
}

dependencies {
    // Depends on the connector library
    implementation(project(":iggy-connector-library"))

    // Flink runtime (provided by cluster in production)
    compileOnly("org.apache.flink:flink-streaming-java:${flinkVersion}")
    compileOnly("org.apache.flink:flink-clients:${flinkVersion}")

    // For local development/testing, include Flink at runtime
    runtimeOnly("org.apache.flink:flink-streaming-java:${flinkVersion}")
    runtimeOnly("org.apache.flink:flink-clients:${flinkVersion}")

    // Jackson for JSON serialization in model classes
    implementation("com.fasterxml.jackson.core:jackson-databind:2.20.1")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.20.1")

    // Include in fat jar for standalone execution
    implementation("org.slf4j:slf4j-simple:2.0.16")
    implementation("com.typesafe:config:1.4.3")

    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter:5.14.1")
    testImplementation("org.assertj:assertj-core:3.27.6")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}

tasks.withType<Test> {
    useJUnitPlatform()
    // Exclude integration tests that require a running Iggy server
    // These should be run separately with: ./gradlew integrationTest
    exclude("**/AsyncTcp*Test.class", "**/SendTextDataTest.class")
}

tasks.shadowJar {
    archiveBaseName.set("flink-iggy-examples")
    archiveVersion.set("")
    archiveClassifier.set("")

    // Merge service files
    mergeServiceFiles()

    // Relocate conflicting dependencies if needed
    // relocate("com.google", "org.apache.iggy.shaded.com.google")
}

java {
    // Target Java 17 for CI compatibility (Java 21 Flink Docker can run Java 17 bytecode)
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}

tasks.withType<Javadoc> {
    options.encoding = "UTF-8"
    (options as StandardJavadocDocletOptions).apply {
        addStringOption("Xdoclint:none", "-quiet")
        addBooleanOption("html5", true)
    }
}

tasks.withType<Jar> {
    manifest {
        attributes["Main-Class"] = application.mainClass.get()
    }
}
