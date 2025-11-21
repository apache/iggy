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
    id("java-library")
    id("maven-publish")
}

repositories {
    mavenCentral()
}

java {
    // Keep Java 17 for SDK compatibility with broader ecosystem
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17

    withJavadocJar()
    withSourcesJar()
}

dependencies {
    implementation("org.apache.httpcomponents.client5:httpclient5:5.5.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.20.1")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.20.1")
    implementation("org.apache.commons:commons-lang3:3.20.0")
    implementation("org.slf4j:slf4j-api:2.0.17")
    implementation("com.github.spotbugs:spotbugs-annotations:4.9.8")
    implementation("io.projectreactor:reactor-core:3.8.0")
    implementation("io.projectreactor.netty:reactor-netty-core:1.3.0")
    testImplementation("org.testcontainers:testcontainers:1.21.3")
    testImplementation("org.testcontainers:junit-jupiter:1.21.3")
    testImplementation(platform("org.junit:junit-bom:5.14.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.27.6")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("ch.qos.logback:logback-classic:1.5.21")
    testRuntimeOnly("io.netty:netty-resolver-dns-native-macos:4.2.7.Final:osx-aarch_64")
}

tasks.withType<Test> {
    useJUnitPlatform()
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
