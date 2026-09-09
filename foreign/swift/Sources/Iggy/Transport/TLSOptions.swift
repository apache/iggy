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

import NIOSSL

/// TLS settings of a TCP connection.
public struct TLSOptions: Sendable, Hashable {
    /// Server name presented for SNI and checked against the certificate.
    /// Defaults to the host part of the server address.
    public var domain: String?
    /// PEM file with the certificate authority to trust instead of the
    /// system roots, for servers with a private CA such as the test server.
    public var caFile: String?
    /// Whether to verify the server certificate at all. Turning this off
    /// makes the connection vulnerable to interception; only for development.
    public var validateCertificate: Bool

    public init(domain: String? = nil, caFile: String? = nil, validateCertificate: Bool = true) {
        self.domain = domain
        self.caFile = caFile
        self.validateCertificate = validateCertificate
    }

    /// The host name to present during the handshake. An IP literal cannot
    /// be used for SNI, so it is left out and only the chain is verified.
    func serverName(for endpoint: Endpoint) -> String? {
        let name = domain?.isEmpty == false ? domain! : endpoint.host
        let literal = (try? Endpoint(parsing: "\(name.contains(":") ? "[\(name)]" : name):1"))?.isIPLiteral ?? false
        return literal ? nil : name
    }

    func makeContext() throws -> NIOSSLContext {
        var configuration = TLSConfiguration.makeClientConfiguration()
        if validateCertificate {
            if let caFile {
                do {
                    let certificates = try NIOSSLCertificate.fromPEMFile(caFile)
                    configuration.trustRoots = .certificates(certificates)
                } catch {
                    throw IggyError(.invalidTlsCertificatePath, context: "\(caFile): \(error)")
                }
            }
            let name = domain ?? ""
            let literal = (try? Endpoint(parsing: "\(name.contains(":") ? "[\(name)]" : name):1"))?.isIPLiteral ?? false
            configuration.certificateVerification = name.isEmpty || !literal ? .fullVerification : .noHostnameVerification
        } else {
            configuration.certificateVerification = .none
        }
        do {
            return try NIOSSLContext(configuration: configuration)
        } catch {
            throw IggyError(.invalidTlsCertificate, context: "\(error)")
        }
    }
}
