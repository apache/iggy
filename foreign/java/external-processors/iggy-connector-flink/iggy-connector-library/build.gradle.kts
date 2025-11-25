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

    // Flink dependencies (provided - not bundled with connector)
    compileOnly(libs.flink.connector.base)
    compileOnly(libs.flink.streaming.java)

    // Serialization support
    implementation(libs.jackson.databind)

    // Logging
    compileOnly(libs.slf4j.api)

    // Testing
    testImplementation(libs.flink.test.utils)
    testImplementation(libs.flink.runtime.tests) { artifact { classifier = "tests" } }
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.testing)
    testRuntimeOnly(libs.slf4j.simple)
}

publishing {
    publications {
        named<MavenPublication>("maven") {
            artifactId = "flink-connector"

            pom {
                name = "Apache Iggy - Flink Connector"
                description = "Apache Iggy connector library supporting Apache Flink"
            }
        }
    }
}
