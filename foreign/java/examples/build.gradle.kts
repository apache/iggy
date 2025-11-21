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
    id("java")
    id("application")
}

repositories {
    mavenCentral()
}

application {
    mainClass.set("org.apache.iggy.consumer.SimpleConsumer")
}

dependencies {
    implementation(project(":iggy"))
    implementation("org.slf4j:slf4j-api:2.0.17")
    runtimeOnly("ch.qos.logback:logback-classic:1.5.21")
    runtimeOnly("io.netty:netty-resolver-dns-native-macos:4.2.7.Final:osx-aarch_64")
}

// Task for running async consumer example
tasks.register<JavaExec>("runAsyncConsumer") {
    group = "application"
    description = "Run the Async Consumer example with Netty"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.apache.iggy.async.AsyncConsumerExample")
}

// Task for running simple consumer
tasks.register<JavaExec>("runSimpleConsumer") {
    group = "application"
    description = "Run the Simple Consumer example"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.apache.iggy.consumer.SimpleConsumer")
}

// Task for running simple producer
tasks.register<JavaExec>("runSimpleProducer") {
    group = "application"
    description = "Run the Simple Producer example"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.apache.iggy.producer.SimpleProducer")
}

// Task for running simple async test
tasks.register<JavaExec>("runSimpleAsyncTest") {
    group = "application"
    description = "Run the Simple Async Test for debugging"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.apache.iggy.async.SimpleAsyncTest")
}

// Task for running async producer
tasks.register<JavaExec>("runAsyncProducer") {
    group = "application"
    description = "Run the Async Producer example"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.apache.iggy.async.AsyncProducer")
}
