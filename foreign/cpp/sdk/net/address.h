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

#include <ada.h>
#include <vector>
#include "protocol.h"
#include "transport.h"

namespace icp {
namespace net {
namespace address {

/***
 * @brief Logical address used in configuration and API to specify desired transport in a compact way, e.g. iggy:quic://localhost:8080.
 */
class LogicalAddress {
private:
    ada::url url;
    const icp::net::protocol::ProtocolProvider* protocolProvider;

    const icp::net::protocol::ProtocolDefinition& getProtocolDefinition() const;

public:
    /**
     * @brief Construct a logical address from a URL.
     * @param url URL to parse.
     * @param protocolProvider Context object providing supported protocols and default ports.
     * @throws std::invalid_argument if the URL is invalid or the protocol is unknown.
     */
    LogicalAddress(const std::string& url, const icp::net::protocol::ProtocolProvider* protocolProvider);

    /**
     * @brief Gets the protocol; you have a guarantee that it will be one of the supported protocols from ProtocolProvider.
     */
    const std::string getProtocol() const noexcept {
        auto protocol = url.get_protocol();
        return protocol.substr(0, protocol.length() - 1);
    }

    /**
     * @brief Gets the hostname to connect to or raw IP address.
     */
    const std::string getHost() const noexcept { return url.get_hostname(); }

    /**
     * @brief Gets the port to connect to; protocol default port will be substituted if not specified.
     */
    const uint16_t getPort() const;
};
};  // namespace address
};  // namespace net
};  // namespace icp
