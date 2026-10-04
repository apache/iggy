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

package org.apache.iggy.bdd;

/**
 * Endpoints and credentials the BDD suite runs against.
 *
 * <p>A default here would turn a dropped compose variable into a run against whatever happens to
 * listen on the fallback address, so a missing value aborts the suite instead.
 */
final class TestEnvironment {

    private TestEnvironment() {}

    static String require(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " must be set; run the suite via scripts/run-bdd-tests.sh");
        }
        return value;
    }

    static String serverAddress() {
        return require("IGGY_TCP_ADDRESS");
    }

    static String leaderAddress() {
        return require("IGGY_TCP_ADDRESS_LEADER");
    }

    static String followerAddress() {
        return require("IGGY_TCP_ADDRESS_FOLLOWER");
    }

    static String rootUsername() {
        return require("IGGY_ROOT_USERNAME");
    }

    static String rootPassword() {
        return require("IGGY_ROOT_PASSWORD");
    }
}
