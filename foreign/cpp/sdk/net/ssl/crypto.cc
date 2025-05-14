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
#include "crypto.h"
#include <fstream>
#include "fmt/format.h"
#include "spdlog/spdlog.h"
#include "ssl_engine.h"

icp::crypto::LocalCertificateStore::LocalCertificateStore(const std::optional<std::filesystem::path> certDir) {
    auto certDirAbs = std::filesystem::absolute(certDir.value_or(std::filesystem::current_path())).make_preferred();
    spdlog::debug("Loading certificates from {}", certDirAbs.string());
    if (!std::filesystem::exists(certDirAbs)) {
        throw std::invalid_argument(fmt::format("certificate directory does not exist: {}", certDirAbs.string()));
    } else if (!std::filesystem::is_directory(certDirAbs)) {
        throw std::invalid_argument(fmt::format("certificate directory is not a valid directory: {}", certDirAbs.string()));
    }
    this->certDir = certDirAbs;
}

const std::vector<uint8_t> icp::crypto::LocalCertificateStore::getCertificate(const std::string& certPath) const {
    std::filesystem::path certFile = (this->certDir.value() / certPath).make_preferred();
    spdlog::debug("Loading certificate from {}", certFile.string());
    if (!std::filesystem::exists(certFile)) {
        throw std::invalid_argument(fmt::format("certificate file does not exist: {}", certFile.string()));
    }
    std::ifstream certStream(certFile, std::ios::binary);
    if (!certStream.is_open()) {
        throw std::runtime_error(fmt::format("Failed to open certificate file: {}", certFile.string()));
    }
    std::vector<uint8_t> certData((std::istreambuf_iterator<char>(certStream)), std::istreambuf_iterator<char>());
    if (certData.empty()) {
        throw std::runtime_error(fmt::format("Invalid certificate file (empty): {}", certFile.string()));
    }
    return certData;
}

icp::crypto::LocalKeyStore::LocalKeyStore(const std::optional<std::filesystem::path> keyDir) {
    auto keyDirAbs = std::filesystem::absolute(keyDir.value_or(std::filesystem::current_path())).make_preferred();
    spdlog::debug("Loading private keys from {}", keyDirAbs.string());
    if (!std::filesystem::exists(keyDirAbs)) {
        throw std::invalid_argument(fmt::format("key directory does not exist: {}", keyDirAbs.string()));
    }
    if (!std::filesystem::is_directory(keyDirAbs)) {
        throw std::invalid_argument(fmt::format("key directory is not a valid directory: {}", keyDirAbs.string()));
    }
    this->privateKeyDir = keyDirAbs;
}

const std::vector<uint8_t> icp::crypto::LocalKeyStore::getPrivateKey(const std::string& keyPath) const {
    std::filesystem::path keyFile = (this->privateKeyDir.value() / keyPath).make_preferred();
    spdlog::debug("Loading private key from {}", keyFile.string());
    if (!std::filesystem::exists(keyFile)) {
        throw std::invalid_argument(fmt::format("private key file does not exist: {}", keyFile.string()));
    }
    std::ifstream keyStream(keyFile, std::ios::binary);
    if (!keyStream.is_open()) {
        throw std::runtime_error(fmt::format("Failed to open private key file: {}", keyFile.string()));
    }
    std::vector<uint8_t> keyData((std::istreambuf_iterator<char>(keyStream)), std::istreambuf_iterator<char>());
    if (keyData.empty()) {
        throw std::runtime_error(fmt::format("Invalid private key file (empty): {}", keyFile.string()));
    }
    return keyData;
}

template <>
void icp::crypto::CRL<WOLFSSL_CTX*>::configure(WOLFSSL_CTX* handle, const icp::crypto::PKIEnvironment<WOLFSSL_CTX*>& pkiEnv) {}

template <>
void icp::crypto::OCSP<WOLFSSL_CTX*>::configure(WOLFSSL_CTX* handle, const icp::crypto::PKIEnvironment<WOLFSSL_CTX*>& pkiEnv) {}

template <>
void icp::crypto::CertificateAuthority<WOLFSSL_CTX*>::configure(WOLFSSL_CTX* handle,
                                                                const icp::crypto::PKIEnvironment<WOLFSSL_CTX*>& pkiEnv) {}
