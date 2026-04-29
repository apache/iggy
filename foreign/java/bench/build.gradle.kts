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
}

application {
    mainClass = "org.apache.iggy.bench.IggyBench"

    // -Xms2g starts the JVM heap at 2 GB.
    // -Xmx2g caps the JVM heap at 2 GB.
    // -XX:+UseG1GC pins the garbage collector across runs.
    // -XX:+AlwaysPreTouch commits heap pages up front to reduce benchmark jitter.
    applicationDefaultJvmArgs = listOf("-Xms2g", "-Xmx2g", "-XX:+UseG1GC", "-XX:+AlwaysPreTouch")
}

dependencies {
    implementation(project(":iggy"))
    implementation(libs.picocli)
    implementation(libs.slf4j.api)

    runtimeOnly(libs.logback.classic)
}
