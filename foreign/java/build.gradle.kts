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

import com.diffplug.gradle.spotless.SpotlessExtension

plugins {
    id("com.diffplug.spotless") version "8.0.0" apply false
}

subprojects {
    apply(plugin = "com.diffplug.spotless")

    configure<SpotlessExtension> {
        java {
            palantirJavaFormat()
            endWithNewline()
            trimTrailingWhitespace()
            importOrder("", "\n", "javax|java", "\n", "\\#")
            removeUnusedImports()
            forbidWildcardImports()
            formatAnnotations()
        }
    }

    plugins.withType<JavaPlugin> {
        apply(plugin = "checkstyle")

        configure<CheckstyleExtension> {
            toolVersion = "10.23.1"
            configFile = file("${project.rootDir}/dev-support/checkstyle/checkstyle.xml")
            configDirectory = file("${project.rootDir}/dev-support/checkstyle")
        }
    }

    // Configure publishing for modules that publish to Maven
    plugins.withId("maven-publish") {
        apply(plugin = "signing")

        configure<PublishingExtension> {
            publications {
                create<MavenPublication>("maven") {
                    from(components["java"])

                    pom {
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

            repositories {
                maven {
                    val releasesRepoUrl = "https://repository.apache.org/service/local/staging/deploy/maven2"
                    val snapshotsRepoUrl = "https://repository.apache.org/content/repositories/snapshots/"

                    url = uri(if ((version as String).endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl)

                    credentials {
                        username = System.getenv("NEXUS_USER")
                        password = System.getenv("NEXUS_PW")
                    }
                }
            }
        }

        configure<SigningExtension> {
            val signingKey: String? by project
            val signingPassword: String? by project
            useInMemoryPgpKeys(signingKey, signingPassword)
            sign(the<PublishingExtension>().publications)
        }
    }
}
