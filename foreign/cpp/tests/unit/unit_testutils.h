#pragma once

#include <catch.hpp>
#include <filesystem>
#include <string>

const char UT_TAG[] = "[Unit Tests]";

namespace icp {
namespace testutil {
/**
 * @brief A helper class for generating self-signed certificates for testing purposes using OpenSSL CLI.
 */
class SelfSignedCertificate {
private:
    const std::filesystem::path certificatePath;
    const std::filesystem::path keyPath;

    static std::filesystem::path generateRandomTempPath(const std::string& baseName);

public:
    SelfSignedCertificate();
    ~SelfSignedCertificate();
    std::filesystem::path getCertificatePath() { return this->certificatePath; }
    std::filesystem::path getKeyPath() { return this->keyPath; }
};
}  // namespace testutil
}  // namespace icp
