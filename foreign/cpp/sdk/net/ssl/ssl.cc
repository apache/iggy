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
#include "ssl.h"
#include <numeric>
#include "fmt/format.h"

std::string icp::ssl::getProtocolVersionName(icp::ssl::ProtocolVersion protocolVersion) {
    switch (protocolVersion) {
        case icp::ssl::ProtocolVersion::TLSV1_3:
            return "TLSV1_3";
        case icp::ssl::ProtocolVersion::TLSV1_2:
            return "TLSV1_2";
        default:
            int protocolVersionInt = static_cast<int>(protocolVersion);
            throw std::runtime_error(fmt::format("Unsupported protocol version code: {}", protocolVersionInt));
    }
}

template <>
const std::vector<std::string> icp::ssl::SSLOptions<WOLFSSL_CTX*>::getDefaultCipherList(icp::ssl::ProtocolVersion protocolVersion) {
    auto ciphers = std::vector<std::string>();

    // References:
    // - https://cheatsheetseries.owasp.org/cheatsheets/Transport_Layer_Security_Cheat_Sheet.html
    // - https://ssl-config.mozilla.org

    if (protocolVersion == icp::ssl::ProtocolVersion::TLSV1_3) {
// sanity check to make sure TLS 1.3 is compiled in
#ifdef WOLFSSL_NO_TLS13
        throw std::runtime_error("TLS 1.3 is not supported by this build of WolfSSL");
#else
        // recommended TLS 1.3 ciphers
#if defined(HAVE_AESGCM) && !defined(NO_AES)
        ciphers.push_back("TLS_AES_128_GCM_SHA256");
#if defined(WOLFSSL_SHA384)
        ciphers.push_back("TLS_AES_256_GCM_SHA384");
#endif  // WOLFSSL_SHA384
#endif  // HAVE_AESGCM && !NO_AES
#if defined(HAVE_CHACHA) && defined(HAVE_POLY1305)
        ciphers.push_back("TLS_CHACHA20_POLY1305_SHA256");
#endif  // HAVE_CHACHA && HAVE_POLY1305
#endif  // WOLFSSL_NO_TLS13
    } else if (protocolVersion == icp::ssl::ProtocolVersion::TLSV1_2) {
#ifdef WOLFSSL_NO_TLS12
        throw std::runtime_error("TLS 1.2 is not supported by this build of WolfSSL");
#else
#if !defined(HAVE_ECC) || !defined(HAVE_SUPPORTED_CURVES)
        throw std::runtime_error("ECC + Supported Curves must be enabled to support ECDHE");
#endif  // HAVE_ECC && HAVE_SUPPORTED_CURVES

        // recommended TLS 1.2 ciphers
#if defined(HAVE_AESGCM) && !defined(NO_AES)
        ciphers.push_back("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");
#ifndef NO_RSA
        ciphers.push_back("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
#endif  // NO_RSA
#if defined(WOLFSSL_SHA384)
        ciphers.push_back("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384");
#endif  // WOLFSSL_SHA384
#ifndef NO_RSA
        ciphers.push_back("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");
#endif  // NO_RSA
#endif  // HAVE_AESGCM && !NO_AES
#if defined(HAVE_CHACHA) && defined(HAVE_POLY1305)
        ciphers.push_back("TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305");
#ifndef NO_RSA
        ciphers.push_back("TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305");
#endif  // NO_RSA
#endif  // HAVE_CHACHA && HAVE_POLY1305
#endif  // WOLFSSL_NO_TLS12
    } else {
        auto protocolVersionName = icp::ssl::getProtocolVersionName(protocolVersion);
        throw std::runtime_error(fmt::format("Unsupported protocol version: {}", protocolVersionName));
    }

    if (ciphers.empty()) {
        auto protocolVersionName = icp::ssl::getProtocolVersionName(protocolVersion);
        throw std::runtime_error(fmt::format("No ciphers available for the specified protocol version: {}", protocolVersionName));
    }
    return ciphers;
}

template <>
void icp::ssl::SSLOptions<WOLFSSL_CTX*>::configure(WOLFSSL_CTX* handle, const icp::crypto::PKIEnvironment<WOLFSSL_CTX*>& pkiEnv) {
    // TODO
}

template <>
void icp::ssl::SSLContext<WOLFSSL_CTX*>::raiseSSLError(const std::string& message) const {
    char* errMsg = wolfSSL_ERR_error_string(wolfSSL_ERR_get_error(), nullptr);
    throw std::runtime_error(fmt::format("{}: {}", message, errMsg));
}

template <>
icp::ssl::SSLContext<WOLFSSL_CTX*>::SSLContext(const SSLOptions<WOLFSSL_CTX*>& options,
                                               const icp::crypto::PKIEnvironment<WOLFSSL_CTX*>& pkiEnv)
    : options(options)
    , pkiEnv(pkiEnv) {
    // before we make any other wolfSSL calls, make sure library is initialized once and only once
    std::call_once(sslInitDone, []() { wolfSSL_Init(); });

    auto protocolVersion = options.getMinimumSupportedProtocolVersion();
    if (protocolVersion == icp::ssl::ProtocolVersion::TLSV1_2) {
        this->ctx = wolfSSL_CTX_new(wolfTLSv1_2_client_method());
        if (!this->ctx) {
            raiseSSLError("Failed to allocate WolfSSL context");
        }
        wolfSSL_CTX_set_min_proto_version(this->ctx, TLS1_2_VERSION);
    } else if (protocolVersion == icp::ssl::ProtocolVersion::TLSV1_3) {
        this->ctx = wolfSSL_CTX_new(wolfTLSv1_3_client_method());
        if (!this->ctx) {
            raiseSSLError("Failed to allocate WolfSSL context");
        }
        wolfSSL_CTX_set_min_proto_version(this->ctx, TLS1_3_VERSION);
    } else {
        auto protocolVersionName = icp::ssl::getProtocolVersionName(protocolVersion);
        throw std::runtime_error(fmt::format("Unsupported protocol version: {}", protocolVersionName));
    }
    this->cm = wolfSSL_CTX_GetCertManager(ctx);

    // set up the supported ciphers
    std::string delimiter = ":";
    std::string joinedCiphers;

    auto supportedCiphers = options.getCiphers();
    if (!supportedCiphers.empty()) {
        joinedCiphers = std::accumulate(std::next(supportedCiphers.begin()), supportedCiphers.end(), supportedCiphers[0],
                                        [delimiter](const std::string& a, const std::string& b) { return a + delimiter + b; });
    }
    int ret = wolfSSL_CTX_set_cipher_list(this->ctx, joinedCiphers.c_str());
    if (ret != SSL_SUCCESS) {
        char* errMsg = wolfSSL_ERR_error_string(wolfSSL_ERR_get_error(), nullptr);
        throw std::runtime_error(fmt::format("Failed to set cipher list: {}", errMsg));
    }
}

template <>
void icp::ssl::SSLOptions<WOLFSSL_CTX*>::validate(bool strict) const {
    if (strict) {
        if (this->minimumSupportedProtocolVersion != icp::ssl::ProtocolVersion::TLSV1_3) {
            throw std::runtime_error("Only TLS 1.3 is supported in strict mode");
        }
    }
    if (this->peerType == icp::ssl::PeerType::SERVER && !this->peerCertPath.has_value()) {
        throw std::runtime_error("Server mode requires a peer certificate path");
    }
}

template <>
icp::ssl::SSLContext<WOLFSSL_CTX*>::SSLContext(const SSLContext<WOLFSSL_CTX*>& other)
    : options(other.options)
    , pkiEnv(other.pkiEnv) {
    this->ctx = wolfSSL_CTX_new(wolfTLSv1_3_client_method());
    this->cm = wolfSSL_CTX_GetCertManager(ctx);
}

template <>
icp::ssl::SSLContext<WOLFSSL_CTX*>::SSLContext(SSLContext<WOLFSSL_CTX*>&& other)
    : options(other.options)
    , pkiEnv(other.pkiEnv) {
    this->ctx = other.ctx;
    this->cm = other.cm;
    other.ctx = nullptr;
    other.cm = nullptr;
}

template <>
icp::ssl::SSLContext<WOLFSSL_CTX*>::~SSLContext() {
    if (this->ctx) {
        wolfSSL_CTX_free(this->ctx);
    }
}

template <>
icp::ssl::SSLContext<WOLFSSL_CTX*>& icp::ssl::SSLContext<WOLFSSL_CTX*>::operator=(const icp::ssl::SSLContext<WOLFSSL_CTX*>& other) {
    if (this != &other) {
        if (this->ctx) {
            wolfSSL_CTX_free(this->ctx);
        }
        this->ctx = wolfSSL_CTX_new(wolfTLSv1_3_client_method());
    }
    return *this;
}

template <>
icp::ssl::SSLContext<WOLFSSL_CTX*>& icp::ssl::SSLContext<WOLFSSL_CTX*>::operator=(SSLContext<WOLFSSL_CTX*>&& other) {
    if (this != &other) {
        if (this->ctx) {
            wolfSSL_CTX_free(this->ctx);
        }
        this->ctx = other.ctx;
        other.ctx = nullptr;
    }
    return *this;
}

template <>
std::once_flag icp::ssl::SSLContext<WOLFSSL_CTX*>::sslInitDone = std::once_flag();
