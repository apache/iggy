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
    id("org.jreleaser") version ("1.14.0")
}

group = "rs.iggy"
version = "0.1.1"

repositories {
    mavenCentral()
}

java {
    withJavadocJar()
    withSourcesJar()
}

dependencies {
    implementation("org.apache.httpcomponents.client5:httpclient5:5.4")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.18.0")
    implementation("org.apache.commons:commons-lang3:3.17.0")
    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation("com.google.code.findbugs:jsr305:3.0.2")
    implementation("io.projectreactor:reactor-core:3.6.11")
    implementation("io.projectreactor.netty:reactor-netty-core:1.1.23")
    testImplementation("org.testcontainers:testcontainers:1.20.3")
    testImplementation("org.testcontainers:junit-jupiter:1.20.3")
    testImplementation(platform("org.junit:junit-bom:5.11.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testRuntimeOnly("ch.qos.logback:logback-classic:1.5.11")
}

tasks.withType<Test> {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "iggy-java-sdk"

            from(components["java"])

            pom {
                name = "Apache Iggy Java Client SDK"
                description = "Official Java client SDK for Apache Iggy message streaming"
                url = "https://github.com/iggy-rs/iggy-java-client"
                licenses {
                    license {
                        name = "Apache License, Version 2.0"
                        url = "https://github.com/iggy-rs/iggy-java-client/blob/main/LICENSE"
                    }
                }
                developers {
                    developer {
                        id = "mmodzelewski"
                        name = "Maciej Modzelewski"
                        email = "maciej@modzelewski.dev"
                    }
                }
                scm {
                    url = "https://github.com/iggy-rs/iggy-java-client"
                    connection = "scm:git:git://github.com/iggy-rs/iggy-java-client.git"
                    developerConnection = "scm:git:git://github.com/iggy-rs/iggy-java-client.git"
                }
            }
        }
    }

    repositories {
        maven {
            url = uri(layout.buildDirectory.dir("staging-deploy"))
        }
    }
}

jreleaser {
    signing {
        setActive("ALWAYS")
        armored = true
    }
    deploy {
        maven {
            mavenCentral {
                create("sonatype") {
                    setActive("ALWAYS")
                    url = "https://central.sonatype.com/api/v1/publisher"
                    stagingRepository("build/staging-deploy")
                }
            }
        }
    }
}
