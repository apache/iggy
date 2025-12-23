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
    checkstyle
}

repositories {
      mavenCentral()
}

dependencies {
    implementation("org.apache.iggy:iggy:local-dev")
    implementation("org.slf4j:slf4j-simple:2.0.13")
    implementation("tools.jackson.core:jackson-databind:3.0.3")
}

spotless {
    java {
        palantirJavaFormat()
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
        formatAnnotations()
        importOrder("", "\n", "javax|java", "\n", "\\#")
        toggleOffOn()
    }
}

checkstyle {
    toolVersion = "12.2.0"
}

tasks.withType<JavaCompile> {
    dependsOn("spotlessApply")
}

tasks.register<JavaExec>("runGettingStartedProducer") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.apache.iggy.examples.gettingstarted.producer.GettingStartedProducer")
}

tasks.register<JavaExec>("runGettingStartedConsumer") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.apache.iggy.examples.gettingstarted.consumer.GettingStartedConsumer")
}

tasks.register<JavaExec>("runMessageEnvelopeProducer") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.apache.iggy.examples.messageenvelope.producer.MessageEnvelopeProducer")
}

tasks.register<JavaExec>("runMessageEnvelopeConsumer") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.apache.iggy.examples.messageenvelope.consumer.MessageEnvelopeConsumer")
}

tasks.register<JavaExec>("runSinkDataProducer") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.apache.iggy.examples.sinkdataproducer.SinkDataProducer")
}


tasks.register<JavaExec>("runMultiTenantProducer") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.apache.iggy.examples.multitenant.producer.MultiTenantProducer")
}

tasks.register<JavaExec>("runMultiTenantConsumer") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.apache.iggy.examples.multitenant.consumer.MultiTenantConsumer")
}

tasks.register<JavaExec>("runStreamBasic") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.apache.iggy.examples.streambuilder.StreamBasic")
}

tasks.register<JavaExec>("runAsyncProducer") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.apache.iggy.examples.async.AsyncProducer")
}

tasks.register<JavaExec>("runAsyncConsumerExample") {
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.apache.iggy.examples.async.AsyncConsumerExample")
}
