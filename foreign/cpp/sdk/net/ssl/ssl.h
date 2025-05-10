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

#include <mutex>
#include <optional>
#include <string>
#include "crypto.h"
#include "ssl_engine.h"

namespace icp {
namespace ssl {

/**
 * @brief The type of peer endpoint represented by the local end of the socket.
 */
enum PeerType { CLIENT, SERVER };

/**
 * @brief The version of the SSL/TLS protocol to use.
 *
 * We do not support the older, less-secure variations since the expectation is the library will be used in a controlled client-server
 * environment where the developer can ensure the server endpoint is adequately hardened.
 */
enum ProtocolVersion { TLSV1_2 = 0, TLSV1_3 = 1 };

/**
 * @brief Helper function to get protocol version name given the enum.
 */
std::string getProtocolVersionName(icp::ssl::ProtocolVersion protocolVersion);

/**
 * @brief All options related to SSL/TLS are in -- what ciphers to use, client vs. server, etc..
 *
 * Mutable configuration object containing all options related to SSL/TLS. It offers reasonable defaults for a strict client. If you need to
 * talk to an SSL/TLS server that has not been hardened you may need to modify this.
 */
template <typename HandleType>
class SSLOptions : public icp::crypto::Configurable<HandleType> {
private:
    PeerType peerType;
    std::optional<std::string> peerCertPath = std::nullopt;
    ProtocolVersion minimumSupportedProtocolVersion = ProtocolVersion::TLSV1_3;
    std::vector<std::string> ciphers = getDefaultCipherList(ProtocolVersion::TLSV1_3);

public:
    /**
     * Creates a default set of options for a TLS 1.3-compatible client.
     */
    explicit SSLOptions(PeerType peerType = PeerType::CLIENT)
        : peerType(peerType) {}

    /**
     * @brief Gets the default cipher list for use in SSL/TLS contexts.
     * @return A vector of cipher strings, all uppercase.
     */
    static const std::vector<std::string> getDefaultCipherList(ProtocolVersion protocolVersion);

    /**
     * @brief Gets the type of peer endpoint represented by this end of the socket.
     */
    PeerType getPeerType() const { return this->peerType; }

    /**
     * @brief Sets the type of peer endpoint represented by the local end of the socket.
     */
    void setPeerType(PeerType peerType) { this->peerType = peerType; }

    /**
     * @brief Sets the type of peer endpoint represented by the local end of the socket.
     */
    std::optional<std::string> getPeerCertificatePath() const { return this->peerCertPath; }

    /**
     * @brief Sets the path to the peer's certificate, if any, to use for verifying the peer's identity.
     */
    void setPeerCertificatePath(const std::string& peerCertPath) { this->peerCertPath = peerCertPath; }

    /**
     * @brief Gets the minimum supported protocol version for the SSL/TLS context.
     */
    ProtocolVersion getMinimumSupportedProtocolVersion() const { return this->minimumSupportedProtocolVersion; }

    /**
     * @brief Sets the minimum supported protocol version for the SSL/TLS context.
     */
    void setMinimumSupportedProtocolVersion(ProtocolVersion minimumSupportedProtocolVersion) {
        this->minimumSupportedProtocolVersion = minimumSupportedProtocolVersion;
    }

    /**
     * @brief Gets the list of requested supported ciphers; will be validated by the context during init.
     */
    const std::vector<std::string>& getCiphers() const { return this->ciphers; }

    /**
     * @brief Sets the list of requested supported ciphers; will be validated by the context during init.
     */
    void setCiphers(const std::vector<std::string>& ciphers) { this->ciphers = ciphers; }

    /**
     * @brief Sanity checks the combination of options configured by the user.
     * @param strict if true, will apply additional validations that may be more restrictive.
     *
     * Applies basic validations to the SSL options, e.g. if PeerType::SERVER is set, then a peer certificate path must be provided.
     */
    void validate(bool strict = true) const;

    /**
     * @brief Configures the SSL/TLS context handle with these options.
     */
    void configure(HandleType handle, const icp::crypto::PKIEnvironment<HandleType>& pkiEnv) override;
};

/**
 * @brief An SSL/TLS context for use in secure communication.
 *
 * This class is a portable wrapper around the WolfSSL context object, providing a C++ interface to the underlying C library, encapsulating
 * all the defaults of how to initialize and clean up the SSL context. It does not expose every possible option, and the intention is to
 * support OpenSSL and BoringSSL in the future.
 */
template <typename HandleType>
class SSLContext {
private:
    static std::once_flag sslInitDone;

    const SSLOptions<HandleType>& options;
    const icp::crypto::PKIEnvironment<HandleType>& pkiEnv;

    WOLFSSL_CTX* ctx;
    WOLFSSL_CERT_MANAGER* cm;

    void raiseSSLError(const std::string& message) const;

public:
    explicit SSLContext(const SSLOptions<HandleType>& options = SSLOptions<HandleType>(),
                        const icp::crypto::PKIEnvironment<HandleType>& pkiEnv = icp::crypto::PKIEnvironment<HandleType>());
    SSLContext(const SSLContext& other);
    SSLContext(SSLContext&& other);
    ~SSLContext();

    SSLContext& operator=(const SSLContext& other);
    SSLContext& operator=(SSLContext&& other);

    /**
     * @brief Gets access to the underlying C SSL context handle. Subsequent internal C library calls should make a static cast to ensure
     * that the expected handle type is used.
     */
    HandleType getNativeHandle() const { return this->ctx; }
};
};  // namespace ssl
};  // namespace icp
