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
    id("iggy.java-library-conventions")
}

dependencies {
    // Iggy SDK - use local project when building within Iggy repository
    api(project(":iggy"))

    // Apache Pinot dependencies (provided - not bundled with connector)
    compileOnly(libs.pinot.spi)

    // Serialization support - use Jackson 2.x for Pinot compatibility
    implementation(libs.jackson2.databind) {
        exclude(group = "tools.jackson.core")
    }

    // Apache Commons
    implementation(libs.commons.lang3)

    // Logging
    compileOnly(libs.slf4j.api)

    // Testing
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testImplementation(libs.pinot.spi) // Need Pinot SPI for tests
    testRuntimeOnly(libs.slf4j.simple)
}

// Assemble connector plugin with all dependencies for Docker deployment
tasks.register<Copy>("assemblePlugin") {
    from(tasks.named("jar"))
    from(configurations.runtimeClasspath)
    into(layout.buildDirectory.dir("plugin"))
}

tasks.named("jar") {
    finalizedBy("assemblePlugin")
}

publishing {
    publications {
        named<MavenPublication>("maven") {
            artifactId = "pinot-connector"

            pom {
                name = "Apache Iggy - Pinot Connector"
                description = "Apache Iggy connector plugin for Apache Pinot stream ingestion"
            }
        }
    }
}
