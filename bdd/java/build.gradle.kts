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
    java
    id("com.diffplug.spotless") version "8.1.0"
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.apache.iggy:iggy")
    testImplementation("io.cucumber:cucumber-java:7.33.0")
    testImplementation("io.cucumber:cucumber-junit-platform-engine:7.33.0")
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.0")
    testImplementation("org.junit.platform:junit-platform-suite:1.11.0")
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

tasks.test {
    useJUnitPlatform()
}
