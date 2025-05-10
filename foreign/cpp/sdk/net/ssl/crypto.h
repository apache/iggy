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

#include <wolfssl/options.h>
#include <wolfssl/ssl.h>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>
#include "ada.h"

namespace icp {
namespace crypto {

/**
 * @brief Encoding format used for certificates and keys.
 */
enum CertificateFormat { PEM = 1, ASN1 = 2 };

/**
 * @brief A mechanism for loading public key certificates: from file; database;
 * or a secret vault.
 */
class CertificateStore {
public:
    CertificateStore() = default;
    virtual ~CertificateStore() = default;

    /**
     * @brief Retrieves a certificate from the store.
     * @param certPath A slash-delimited abstract path to the certificate file;
     * this may be translated according to the backend storage to a
     * filesystem-specific path or some other convention.
     * @return A vector of bytes representing the certificate in PEM or ASN.1.
     */
    virtual const std::vector<uint8_t> getCertificate(const std::string& certPath) const = 0;
};

/**
 * @brief A certificate store that loads certificates from the local filesystem.
 */
class LocalCertificateStore : public CertificateStore {
private:
    std::optional<std::filesystem::path> certDir;

public:
    explicit LocalCertificateStore(const std::optional<std::filesystem::path> certDir = std::nullopt);
    ~LocalCertificateStore() = default;

    /**
     * @brief Gets the default instance, which loads from the current working directory.
     */
    static LocalCertificateStore& getDefault() {
        static LocalCertificateStore instance;
        return instance;
    }

    /**
     * Retrieves the certificate from the filesystem, translating the abstract
     * path to a filesystem-appropriate absolute path.
     */
    const std::vector<uint8_t> getCertificate(const std::string& certPath) const override;
};

/**
 * @brief A mechanism for loading private keys: from file; database; or a secret
 * vault.
 */
class KeyStore {
public:
    KeyStore() = default;
    virtual ~KeyStore() = default;

    /**
     * @brief Retrieves the private key data from the store.
     * @param keyPath A slash-delimited abstract path to the key file;
     * this may be translated according to the backend storage to a
     * filesystem-specific path or some other convention.
     * @return A vector of bytes representing the key in PEM or ASN.1.
     */
    virtual const std::vector<uint8_t> getPrivateKey(const std::string& keyPath) const = 0;
};

/**
 * @brief A key store that loads private key materials from the local
 * filesystem.
 */
class LocalKeyStore : public KeyStore {
private:
    std::optional<std::filesystem::path> privateKeyDir;

public:
    explicit LocalKeyStore(const std::optional<std::filesystem::path> privateKeyDir = std::nullopt);
    ~LocalKeyStore() = default;

    /**
     * @brief Gets the default instance, which loads from the current working directory.
     */
    static LocalKeyStore& getDefault() {
        static LocalKeyStore instance;
        return instance;
    }

    /**
     * Retrieves the private key materials from the filesystem, translating the abstract
     * path to a filesystem-appropriate absolute path.
     */
    const std::vector<uint8_t> getPrivateKey(const std::string& keyPath) const override;
};

// forward declaration
template <typename HandleType>
class PKIEnvironment;

/**
 * Base type for objects that can be configured with library-specific calls. You will need
 * a specialization for each library supported, e.g. wolfSSL, OpenSSL/BoringSSL, mbedTLS, etc.,
 * which makes that library C API calls for handling the particular object's settings.
 */
template <typename HandleType>
class Configurable {
    /**
     * Crypto library-specific implementation to configure the revocation method, usually with C API.
     */
    virtual void configure(HandleType handle, const PKIEnvironment<HandleType>& pkiEnv) = 0;
};

/**
 * @brief A mechanism for revoking certificates: through CRL or OCSP.
 */
template <typename HandleType>
class RevocationMethod : Configurable<HandleType> {
public:
    RevocationMethod() = default;
    virtual ~RevocationMethod() = default;
};

/**
 * @brief Certificate revocation list (CRL)-based revocation method. If there are no CRL paths or URLs
 * specified, the CRL is assumed to be embedded in the CA certificate.
 */
template <typename HandleType>
class CRL : public RevocationMethod<HandleType> {
private:
    std::optional<std::filesystem::path> crlPath;
    std::optional<ada::url> crlUrl;

public:
    explicit CRL(std::optional<std::filesystem::path> crlPath = std::nullopt, std::optional<ada::url> crlUrl = std::nullopt)
        : crlPath(crlPath)
        , crlUrl(crlUrl) {}

    /**
     * @brief If specified (default: not), the filesystem path to the CRL file.
     */
    const std::optional<std::filesystem::path> getCrlPath() const { return this->crlPath; }

    /**
     * @brief If specified (default: not), an HTTP URL from which to load the CRL.
     */
    const std::optional<ada::url> getCrlUrl() const { return this->crlUrl; }

    /**
     * Installs all CRL settings supported in the library into the given handle.
     */
    void configure(HandleType handle, const PKIEnvironment<HandleType>& pkiEnv) override;
};

/**
 * @brief Online Certificate Status Protocol (OCSP)-based revocation method. If there is no override
 * OCSP URL specified, the OCSP URL is assumed to be embedded in the CA certificate.
 */
template <typename HandleType>
class OCSP : public RevocationMethod<HandleType> {
private:
    const std::optional<ada::url> ocspOverrideUrl;
    const bool staplingEnabled;

public:
    explicit OCSP(const std::optional<ada::url> ocspOverrideUrl = std::nullopt, const bool staplingEnabled = true)
        : ocspOverrideUrl(ocspOverrideUrl)
        , staplingEnabled(staplingEnabled) {}

    /**
     * @brief If specified (default: not), an HTTP URL used to check the status of certificates.
     */
    const std::optional<ada::url> getOcspOverrideUrl() const { return this->ocspOverrideUrl; }

    /**
     * @brief If enabled, servers will cache OCSP verification checks to improve performance.
     */
    const bool isStaplingEnabled() const { return this->staplingEnabled; }

    /**
     * Installs all OCSP settings supported in the library into the given handle.
     */
    void configure(HandleType handle, const PKIEnvironment<HandleType>& pkiEnv) override;
};

/**
 * @brief Authority for verifying certificates -- either through checking against a centralized CA, or via a trusted peer relationship. If
 * all defaults are taken, the system CA paths will be used, with revocation checking enabled via OCSP.
 */
template <typename HandleType>
class CertificateAuthority : public Configurable<HandleType> {
private:
    const std::optional<std::string> overrideCaCertificatePath;
    std::vector<std::string> trustedPeerCertificatePaths = std::vector<std::string>();
    const RevocationMethod<HandleType>* revocationMethod;

public:
    explicit CertificateAuthority(std::optional<std::string> overrideCaCertificatePath = std::nullopt,
                                  RevocationMethod<HandleType>* revocationMethod = new OCSP<HandleType>())
        : overrideCaCertificatePath(overrideCaCertificatePath)
        , revocationMethod(revocationMethod) {}

    /**
     * @brief Gets the default instance, which uses the system CA store and OCSP.
     */
    static CertificateAuthority& getDefault() {
        static CertificateAuthority instance;
        return instance;
    }

    /**
     * @brief If specified (default: not), the filesystem path to the CA certificate path file.
     */
    const std::optional<std::string> getOverrideCaCertificatePath() const { return this->overrideCaCertificatePath; }

    /**
     * @brief Gets all specified trusted peer certificate paths, if any.
     */
    const std::vector<std::string> getTrustedPeerCertificatePaths() const { return this->trustedPeerCertificatePaths; }

    /**
     * @brief Adds a trusted peer certificate path; optional -- if none, only CA-verified certificates will be trusted.
     */
    void addTrustedPeerCertificate(const std::string& certPath) { this->trustedPeerCertificatePaths.push_back(certPath); }

    /**
     * @brief Gets the revocation method to use for verifying certificates: CRL or OCSP.
     */
    RevocationMethod<HandleType>* getRevocationMethod() const { return this->revocationMethod; }

    /**
     * Installs all CA settings supported in the library into the given handle.
     */
    void configure(HandleType handle, const PKIEnvironment<HandleType>& pkiEnv) override;
};

/**
 * @brief All options related to the environment library is in -- where to load CA, certificates and keys.
 *
 * Mutable configuration object containing our hooks to load CA certificates, peer & trusted certificates, and keys. It offers reasonable
 * defaults if you are loading from PEM files on the filesystem and are OK using the operating system default CA store with OCSP.
 */
template <typename HandleType>
class PKIEnvironment : Configurable<HandleType> {
private:
    CertificateAuthority<HandleType>& certAuth;
    CertificateStore& certStore;
    KeyStore& keyStore;

public:
    PKIEnvironment(CertificateAuthority<HandleType>& certAuth = CertificateAuthority<HandleType>::getDefault(),
                   CertificateStore& certStore = LocalCertificateStore::getDefault(),
                   KeyStore& keyStore = LocalKeyStore::getDefault())
        : certAuth(certAuth)
        , certStore(certStore)
        , keyStore(keyStore) {}

    /**
     * @brief Gets the certificate authority to use for verifying peer certificates; defaults to local system CA store.
     */
    const CertificateAuthority<HandleType>& getCertificateAuthority() const { return this->certAuth; }

    /**
     * @brief Sets an alternative certificate authority to use for verifying peer certificates, e.g. if you use a custom CA service,
     * API-based secret store like Vault or 1Password, or a custom database.
     */
    void setCertificateAuthority(const CertificateAuthority<HandleType>& certAuth) { this->certAuth = certAuth; }

    /**
     * @brief Gets the certificate store to use for loading this peer's own certificate and any trusted peer certificates; defaults to a
     * local filesystem store.
     */
    const CertificateStore& getCertificateStore() const { return this->certStore; }

    /**
     * @brief Sets an alternative certificate store to use for loading this peer's own certificate and any trusted peer certificates, e.g.
     * if you use a database.
     */
    void setCertificateStore(const CertificateStore& certStore) { this->certStore = certStore; }

    /**
     * @brief Gets the certificate store to use for loading private key materials; defaults to a local filesystem store.
     */
    const KeyStore& getKeyStore() const { return this->keyStore; }

    /**
     * @brief Sets an alternative key store to use for loading private key materials, e.g. if you use an API-based secret store like Vault
     * or 1Password, cloud HSM-based vault, or a custom database.
     */
    void setKeyStore(const KeyStore& keyStore) { this->keyStore = keyStore; }

    /**
     * @brief Installs all PKI settings supported in the library into the given handle, walking the tree of objects.
     */
    void configure(HandleType handle, const PKIEnvironment<HandleType>& pkiEnv) override { this->certAuth.configure(handle, pkiEnv); }
};

};  // namespace crypto
};  // namespace icp
