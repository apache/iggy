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

import java.time.Instant

plugins {
    id("iggy.java-library-conventions")
}

val gitCommit: Provider<String> = providers.exec {
    commandLine("git", "rev-parse", "--short", "HEAD")
    isIgnoreExitValue = true
}.standardOutput.asText.map { it.trim() }.orElse("unknown")

val expandProperties = mapOf(
    "version" to version.toString(),
    "buildTime" to Instant.now().toString(),
    "gitCommit" to gitCommit.get()
)

tasks.processResources {
    inputs.properties(expandProperties)
    expand(expandProperties)
}

dependencies {
    implementation(libs.httpclient5)
    implementation(libs.jackson.databind)
    implementation(libs.commons.lang3)
    implementation(libs.slf4j.api)
    implementation(libs.spotbugs.annotations)
    implementation(libs.reactor.core)
    implementation(libs.reactor.netty.core)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testRuntimeOnly(libs.logback.classic)
    testRuntimeOnly(libs.netty.dns.macos) { artifact { classifier = "osx-aarch_64" } }
}

publishing {
    publications {
        named<MavenPublication>("maven") {
            pom {
                name = "Apache Iggy Java Client SDK"
                description = "Official Java client SDK for Apache Iggy.\n" +
                        "Apache Iggy (Incubating) is an effort undergoing incubation at the Apache Software Foundation (ASF), " +
                        "sponsored by the Apache Incubator PMC."
                packaging = "jar"
            }
        }
    }
}
