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
#include "../../sdk/net/ssl/crypto.h"
#include "unit_testutils.h"

TEST_CASE_METHOD(icp::testutil::SelfSignedCertificate, "certificate loading", UT_TAG) {
    auto tmpDir = std::filesystem::temp_directory_path();
    icp::crypto::CertificateStore* cs = new icp::crypto::LocalCertificateStore(tmpDir);
    auto cert = cs->getCertificate(getCertificatePath().filename());
    REQUIRE(cert.size() > 0);
    delete cs;
}

TEST_CASE_METHOD(icp::testutil::SelfSignedCertificate, "private key loading", UT_TAG) {
    auto tmpDir = std::filesystem::temp_directory_path();
    icp::crypto::KeyStore* ks = new icp::crypto::LocalKeyStore(tmpDir);
    auto pk = ks->getPrivateKey(getKeyPath().filename());
    REQUIRE(pk.size() > 0);
    delete ks;
}

TEST_CASE("revocation methods", UT_TAG) {
    SECTION("default CRL configuration") {
        icp::crypto::CRL<WOLFSSL_CTX*> crl;

        REQUIRE(!crl.getCrlPath().has_value());
        REQUIRE(!crl.getCrlUrl().has_value());
    }

    SECTION("default OCSP configuration") {
        icp::crypto::OCSP<WOLFSSL_CTX*> ocsp;

        REQUIRE(!ocsp.getOcspOverrideUrl().has_value());
        REQUIRE(ocsp.isStaplingEnabled());
    }
}
