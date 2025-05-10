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
