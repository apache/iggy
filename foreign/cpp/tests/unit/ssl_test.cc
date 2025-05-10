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
#include "../../sdk/net/ssl/ssl.h"
#include "unit_testutils.h"

TEST_CASE("SSL configuration", UT_TAG) {
    icp::ssl::SSLOptions<WOLFSSL_CTX*> options;

    SECTION("expected basic default settings") {
        // default options should always be strictly valid
        REQUIRE_NOTHROW(options.validate(true));

        REQUIRE(options.getPeerType() == icp::ssl::PeerType::CLIENT);
        REQUIRE(options.getPeerCertificatePath().has_value() == false);
        REQUIRE(options.getMinimumSupportedProtocolVersion() == icp::ssl::ProtocolVersion::TLSV1_3);
    }

    SECTION("default cipher list configured") {
        auto cipherListTLSV1_2 = options.getDefaultCipherList(icp::ssl::ProtocolVersion::TLSV1_2);
        auto cipherListTLSV1_3 = options.getDefaultCipherList(icp::ssl::ProtocolVersion::TLSV1_3);

        REQUIRE(cipherListTLSV1_2.size() == 6);
        REQUIRE(cipherListTLSV1_3.size() == 3);

        std::string tls12Cipher = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384";
        std::string tls13Cipher = "TLS_CHACHA20_POLY1305_SHA256";
        CHECK(std::find(cipherListTLSV1_2.begin(), cipherListTLSV1_2.end(), tls12Cipher) != cipherListTLSV1_2.end());
        CHECK(std::find(cipherListTLSV1_3.begin(), cipherListTLSV1_3.end(), tls13Cipher) != cipherListTLSV1_3.end());
    }

    SECTION("configure bespoke cipers") {
        auto testCipher = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384";
        options.setCiphers({testCipher});

        auto ciphers = options.getCiphers();
        REQUIRE(ciphers.size() == 1);
        CHECK(std::find(ciphers.begin(), ciphers.end(), testCipher) != ciphers.end());

        REQUIRE_NOTHROW(options.validate(true));
    }

    SECTION("configure server options") {
        options.setPeerType(icp::ssl::PeerType::SERVER);

        // missing certificate path
        REQUIRE_THROWS(options.validate());

        // fix the issue
        options.setPeerCertificatePath("test.pem");

        // first try strict validation, fail
        options.setMinimumSupportedProtocolVersion(icp::ssl::ProtocolVersion::TLSV1_2);
        REQUIRE_THROWS(options.validate(true));

        // loosen the validation
        REQUIRE_NOTHROW(options.validate(false));

        // finally harden the settings and tighten up validation
        options.setMinimumSupportedProtocolVersion(icp::ssl::ProtocolVersion::TLSV1_3);
        REQUIRE_NOTHROW(options.validate(true));
    }
}

TEST_CASE_METHOD(icp::testutil::SelfSignedCertificate, "SSL context init", UT_TAG) {
    auto certPath = getCertificatePath().filename();
    auto keyPath = getKeyPath().filename();

    auto ca = icp::crypto::CertificateAuthority<WOLFSSL_CTX*>();
    auto certStore = icp::crypto::LocalCertificateStore(std::filesystem::temp_directory_path());
    auto keyStore = icp::crypto::LocalKeyStore(std::filesystem::temp_directory_path());

    auto options = icp::ssl::SSLOptions<WOLFSSL_CTX*>();
    options.setPeerCertificatePath(certPath);

    icp::crypto::PKIEnvironment<WOLFSSL_CTX*> pkiEnv(ca, certStore, keyStore);

    SECTION("TLS version support") {
        auto [requestedVersion, minProtoVersion, maxProtoVersion] =
            GENERATE(std::make_tuple(icp::ssl::ProtocolVersion::TLSV1_2, TLS1_2_VERSION, TLS1_3_VERSION),
                     std::make_tuple(icp::ssl::ProtocolVersion::TLSV1_3, TLS1_3_VERSION, TLS1_3_VERSION));

        SECTION(icp::ssl::getProtocolVersionName(requestedVersion)) {
            options.setMinimumSupportedProtocolVersion(requestedVersion);
            auto sslCtx = icp::ssl::SSLContext<WOLFSSL_CTX*>(options, pkiEnv);

            WOLFSSL_CTX* handle = sslCtx.getNativeHandle();
            REQUIRE(handle != nullptr);

            WOLFSSL* ssl = wolfSSL_new(handle);
            REQUIRE(ssl != nullptr);

            WOLFSSL_CERT_MANAGER* cm = wolfSSL_CTX_GetCertManager(handle);
            REQUIRE(cm != nullptr);

            // sanity check configuration
            REQUIRE(wolfSSL_CTX_get_min_proto_version(handle) == minProtoVersion);
            REQUIRE(wolfSSL_CTX_get_max_proto_version(handle) == maxProtoVersion);

            // check cipher list

            // check certificate configuration

            // check CA configuration

            // test copy and move constructors
            auto sslCtxNew = sslCtx;

            // cppcheck-suppress knownConditionTrueFalse
            REQUIRE(sslCtx.getNativeHandle() != sslCtxNew.getNativeHandle());

            auto sslCtxMoved = std::move(sslCtxNew);
            REQUIRE(sslCtxNew.getNativeHandle() == nullptr);
            REQUIRE(sslCtxMoved.getNativeHandle() != nullptr);

            // test copy and move operators
            sslCtxNew = sslCtxMoved;
            REQUIRE(sslCtx.getNativeHandle() != sslCtxNew.getNativeHandle());

            icp::ssl::SSLContext<WOLFSSL_CTX*> sslCtxNew2;
            sslCtxNew2 = std::move(sslCtx);
        }
    }
}

TEST_CASE("error message conversion", UT_TAG) {
    SECTION("TLS 1.2") {
        auto displayName = icp::ssl::getProtocolVersionName(icp::ssl::ProtocolVersion::TLSV1_2);
        REQUIRE(displayName == "TLSV1_2");
    }
    SECTION("TLS 1.3") {
        auto displayName = icp::ssl::getProtocolVersionName(icp::ssl::ProtocolVersion::TLSV1_3);
        REQUIRE(displayName == "TLSV1_3");
    }
}
