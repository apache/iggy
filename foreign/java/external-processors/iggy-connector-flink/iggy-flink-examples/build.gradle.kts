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
    id("iggy.java-application-conventions")
    alias(libs.plugins.shadow)
}

application {
    mainClass.set("org.apache.iggy.flink.example.StreamTransformJob")
}

dependencies {
    // Depends on the connector library
    implementation(project(":iggy-connector-library"))

    // Flink runtime (provided by cluster in production)
    compileOnly(libs.flink.streaming.java)
    compileOnly(libs.flink.clients)

    // For local development/testing, include Flink at runtime
    runtimeOnly(libs.flink.streaming.java)
    runtimeOnly(libs.flink.clients)

    // Jackson for JSON serialization in model classes
    implementation(libs.jackson.databind)

    // Include in fat jar for standalone execution
    implementation(libs.slf4j.simple)
    implementation(libs.typesafe.config)

    // Testing
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testRuntimeOnly(libs.slf4j.simple)
}

tasks.withType<Test> {
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
