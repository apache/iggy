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

package org.apache.iggy.client.blocking.http;

import org.testcontainers.containers.GenericContainer;

import static org.apache.iggy.client.blocking.IntegrationTest.HTTP_PORT;

final class HttpClientFactory {

    private HttpClientFactory() {}

    static IggyHttpClient create(GenericContainer<?> iggyServer) {
        if (iggyServer == null) {
            // Server is running externally
            return new IggyHttpClient("http://127.0.0.1:" + HTTP_PORT);
        }
        String address = iggyServer.getHost();
        Integer port = iggyServer.getMappedPort(HTTP_PORT);
        return new IggyHttpClient("http://" + address + ":" + port);
    }
}
