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

#include <string>
#include <vector>
#include "transport.h"

namespace icp {
namespace net {
namespace address {
class LogicalAddress;
};

namespace protocol {

/**
 * @brief Enumerates the supported message encodings.
 */
enum MessageEncoding { BINARY = 0, TEXT = 1 };

/**
 * @brief Normalizes the protocol name to a canonical form.
 */
const std::string normalizeProtocolName(const std::string& protocol);

/**
 * @brief Metadata about a protocol including its default port, transport, TLS support and message encoding.
 */
class ProtocolDefinition {
private:
    std::string name;
    uint16_t defaultPort;
    icp::net::transport::Transport transport;
    bool tlsSupported;
    MessageEncoding messageEncoding;

public:
    ProtocolDefinition(const std::string& name,
                       uint16_t defaultPort,
                       icp::net::transport::Transport transport,
                       bool tlsSupported,
                       MessageEncoding messageEncoding)
        : name(icp::net::protocol::normalizeProtocolName(name))
        , defaultPort(defaultPort)
        , transport(transport)
        , tlsSupported(tlsSupported)
        , messageEncoding(messageEncoding) {}

    ProtocolDefinition() = default;
    ProtocolDefinition(const ProtocolDefinition& other) = default;

    /**
     * @brief Get the protocol name, e.g. iggy:tcp+tls.
     */
    const std::string& getName() const { return name; }

    /**
     * @brief Gets the default port for the protocol, e.g. 443 for https.
     */
    uint16_t getDefaultPort() const { return defaultPort; }

    /**
     * @brief Gets the transport for the protocol, e.g. icp::net::transport::Transport::TCP.
     */
    icp::net::transport::Transport getTransport() const { return transport; }

    /**
     * @brief Tests whether the protocol supports TLS; insecure and TLS protocols should be separate.
     */
    bool isTlsSupported() const { return tlsSupported; }

    /**
     * @brief Gets the default message encoding used by the protocol, e.g. MessageEncoding::TEXT for JSON.
     */
    MessageEncoding getMessageEncoding() const { return messageEncoding; }
};

/**
 * @brief Interface to plug in library-specific information on supported protocols.
 */
class ProtocolProvider {
public:
    ProtocolProvider() = default;
    virtual ~ProtocolProvider() = default;

    /**
     * @brief Factory method to create a logical address from a URL.
     * @param url The URL to parse in the context of this provider and its defaults.
     */
    icp::net::address::LogicalAddress createAddress(const std::string& url) const;

    /**
     * @brief Enumerates all the supported protocols in the provider.
     */
    virtual const std::vector<ProtocolDefinition>& getSupportedProtocols() const = 0;

    /**
     * @brief Given a normalized protocol name returns the definition with protocol metadata.
     */
    virtual const ProtocolDefinition& getProtocolDefinition(const std::string& protocol) const = 0;

    /**
     * @brief Tests whether the given protocol is supported by this provider.
     * @param protocol The protocol name to test.
     */
    virtual const bool isSupported(const std::string& protocol) const = 0;
};

};  // namespace protocol
};  // namespace net
};  // namespace icp
