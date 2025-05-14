// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#define CATCH_CONFIG_MAIN
#include "../../sdk/net/iggy.h"
#include "unit_testutils.h"

TEST_CASE("Iggy protocols", UT_TAG) {
    icp::net::IggyProtocolProvider provider;

    SECTION("enumerate supported protocols") {
        REQUIRE(provider.getSupportedProtocols().size() == 5);
    }

    SECTION("check supported protocol definitions") {
        auto [protocolName, tlsSupported] =
            GENERATE(table<std::string, bool>({{"quic", true}, {"tcp", false}, {"tcp+tls", true}, {"http", false}, {"http+tls", true}}));

        REQUIRE(provider.isSupported(protocolName));
        REQUIRE(provider.getProtocolDefinition(protocolName).getName() == protocolName);
        REQUIRE(provider.getProtocolDefinition(protocolName).isTlsSupported() == tlsSupported);
    }

    SECTION("create addresses") {
        auto [address, protocolName, host, port] =
            GENERATE(table<std::string, std::string, std::string, int>({{"quic://localhost", "quic", "localhost", 8080},
                                                                        {"tcp://localhost:1234", "tcp", "localhost", 1234},
                                                                        {"tcp+tls://localhost:1234", "tcp+tls", "localhost", 1234},
                                                                        {"http://localhost", "http", "localhost", 3000},
                                                                        {"http+tls://localhost:1234", "http+tls", "localhost", 1234}}));

        auto addr = provider.createAddress(address);
        REQUIRE(addr.getProtocol() == protocolName);
        REQUIRE(addr.getHost() == host);
        REQUIRE(addr.getPort() == port);
    }
}
