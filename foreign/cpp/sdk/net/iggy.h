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
#pragma once
#include <map>
#include <vector>
#include "address.h"

namespace icp {
namespace net {

const uint16_t DEFAULT_HTTP_PORT = 3000;
const uint16_t DEFAULT_TCP_PORT = 8090;
const uint16_t DEFAULT_QUIC_PORT = 8080;

const char QUIC_PROTOCOL[] = "quic";
const char TCP_PROTOCOL[] = "tcp";
const char TCP_TLS_PROTOCOL[] = "tcp+tls";
const char HTTP_PROTOCOL[] = "http";
const char HTTP_TLS_PROTOCOL[] = "http+tls";

using icp::net::protocol::MessageEncoding;
using icp::net::protocol::ProtocolDefinition;

/**
 * @brief Provider that declares support and offers defaults for all Iggy C++ supported protocols.
 *
 * At this time we support iggy:quic, iggy:tcp (binary messaging) and iggy:http (with JSON messaging).
 */
class IggyProtocolProvider : public icp::net::protocol::ProtocolProvider {
private:
    std::vector<ProtocolDefinition> supportedProtocols = {
        ProtocolDefinition(QUIC_PROTOCOL, DEFAULT_QUIC_PORT, icp::net::transport::QUIC, true, MessageEncoding::BINARY),
        ProtocolDefinition(TCP_PROTOCOL, DEFAULT_TCP_PORT, icp::net::transport::TCP, false, MessageEncoding::BINARY),
        ProtocolDefinition(TCP_TLS_PROTOCOL, DEFAULT_TCP_PORT, icp::net::transport::TCP, true, MessageEncoding::BINARY),
        ProtocolDefinition(HTTP_PROTOCOL, DEFAULT_HTTP_PORT, icp::net::transport::HTTP, false, MessageEncoding::TEXT),
        ProtocolDefinition(HTTP_TLS_PROTOCOL, DEFAULT_HTTP_PORT, icp::net::transport::HTTP, true, MessageEncoding::TEXT)};
    std::map<std::string, ProtocolDefinition> supportedProtocolLookup;

public:
    IggyProtocolProvider();
    const std::vector<ProtocolDefinition>& getSupportedProtocols() const override;
    const ProtocolDefinition& getProtocolDefinition(const std::string& protocol) const override;
    const bool isSupported(const std::string& protocol) const override;
};

};  // namespace net
};  // namespace icp
