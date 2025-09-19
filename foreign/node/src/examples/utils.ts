/**
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


<<<<<<<< HEAD:foreign/node/src/examples/utils.ts
import { Client } from '../index.js';
import { getIggyAddress } from '../tcp.sm.utils.js';
========
group = "org.apache.iggy"
version = "0.5.0-SNAPSHOT"
>>>>>>>> master:foreign/java/examples/build.gradle.kts


<<<<<<<< HEAD:foreign/node/src/examples/utils.ts
export const getClient = () => {
  const [host, port] = getIggyAddress();
  const credentials = { username: 'iggy', password: 'iggy' };

  const opt = {
    transport: 'TCP' as const,
    options: { host, port },
    credentials
  };

  return new Client(opt);
};
========
dependencies {
    implementation(project(":iggy"))
    implementation("org.slf4j:slf4j-api:2.0.9")
    runtimeOnly("ch.qos.logback:logback-classic:1.4.12")
    runtimeOnly("io.netty:netty-resolver-dns-native-macos:4.2.1.Final:osx-aarch_64")
}
>>>>>>>> master:foreign/java/examples/build.gradle.kts
