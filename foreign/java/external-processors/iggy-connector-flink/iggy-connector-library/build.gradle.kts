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
    id("checkstyle")
}

group = "org.apache.iggy"
version = "0.6.0-SNAPSHOT"

repositories {
    mavenCentral()
}

java {
    // Target Java 17 for CI compatibility (Java 21 Flink Docker can run Java 17 bytecode)
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17

    withJavadocJar()
    withSourcesJar()
}

checkstyle {
    toolVersion = "10.23.1"
    configFile = file("../../../dev-support/checkstyle/checkstyle.xml")
}

val flinkVersion = "2.1.0"
val iggyVersion = "0.6.0-SNAPSHOT"

dependencies {
    // Iggy SDK - use local project when building within Iggy repository
    api(project(":iggy"))

    // Flink dependencies (provided - not bundled with connector)
    compileOnly("org.apache.flink:flink-connector-base:${flinkVersion}")
    compileOnly("org.apache.flink:flink-streaming-java:${flinkVersion}")

    // Serialization support
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.18.0")

    // Logging
    compileOnly("org.slf4j:slf4j-api:2.0.16")

    // Testing
    testImplementation("org.apache.flink:flink-test-utils:${flinkVersion}")
    testImplementation("org.apache.flink:flink-runtime:${flinkVersion}:tests")
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.16")
}

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.add("-parameters")
}

tasks.withType<Javadoc> {
    options.encoding = "UTF-8"
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "org.apache.iggy"
            artifactId = "iggy-connector-library"
            version = "0.6.0-SNAPSHOT"
            from(components["java"])

            pom {
                name = "Apache Iggy Connector Library"
                description = "Core connector library for Apache Iggy stream processors (Flink, Spark, etc.)"
                url = "https://github.com/apache/iggy"
                licenses {
                    license {
                        name = "Apache License, Version 2.0"
                        url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                    }
                }
                developers {
                    developer {
                        name = "Apache Iggy"
                        email = "dev@iggy.apache.org"
                    }
                }
                scm {
                    url = "https://github.com/apache/iggy"
                    connection = "scm:git:git://github.com/apache/iggy.git"
                    developerConnection = "scm:git:git://github.com/apache/iggy.git"
                }
            }
        }
    }
}
