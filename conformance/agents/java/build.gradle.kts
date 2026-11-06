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
    id("com.diffplug.spotless") version "8.10.4"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.iggy:iggy")
    implementation(libs.jackson.databind)
    implementation(libs.slf4j.api)
    runtimeOnly("org.slf4j:slf4j-simple:${libs.versions.slf4j.get()}")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.release = 17
}

application {
    mainClass = "org.apache.iggy.conformance.Agent"
    // stdout carries the protocol, so the SDK logs go to stderr, and only its warnings.
    applicationDefaultJvmArgs = listOf(
        "-Dorg.slf4j.simpleLogger.logFile=System.err",
        "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn",
    )
}

spotless {
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
