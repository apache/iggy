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

import Testing

@testable import Iggy

@Suite("Endpoints")
struct EndpointTests {
    @Test func parsesHostsAndPorts() throws {
        let plain = try Endpoint(parsing: "localhost:8090")
        #expect(plain.host == "localhost")
        #expect(plain.port == 8090)
        #expect(!plain.isIPLiteral)
        let v4 = try Endpoint(parsing: "127.0.0.1:8090")
        #expect(v4.isIPLiteral)
        #expect(v4.description == "127.0.0.1:8090")
        let v6 = try Endpoint(parsing: "[fd00::1]:8091")
        #expect(v6.host == "fd00::1")
        #expect(v6.port == 8091)
        #expect(v6.isIPLiteral)
        #expect(v6.description == "[fd00::1]:8091")
    }

    @Test(arguments: [
        "", "localhost", ":8090", "localhost:0", "localhost:70000", "[::1]", "[::1]8090", "::1:8090", "host:port", "localhost:+8090", "[]:8090", "[::1:8090",
    ])
    func rejectsMalformedAddresses(_ address: String) {
        #expect(throws: IggyError.self) {
            try Endpoint(parsing: address)
        }
    }

    @Test func spellingComparison() {
        #expect(isSameSpelling("localhost:8090", "127.0.0.1:8090"))
        #expect(isSameSpelling("[::1]:8090", "localhost:8090"))
        #expect(isSameSpelling("Iggy-1:8090", "iggy-1:8090"))
        #expect(!isSameSpelling("localhost:8090", "localhost:8091"))
        #expect(!isSameSpelling("10.0.0.1:8090", "10.0.0.2:8090"))
    }

    @Test func namesAndTheAddressesTheyResolveToAreOneEndpoint() async {
        let resolver: @Sendable (String) async -> Set<String>? = { host in
            ["iggy-server": ["10.0.0.5", "fd00::5"], "10.0.0.5": ["10.0.0.5"], "other": ["10.0.0.6"]][host]
        }
        #expect(await isSameAddress("iggy-server:8090", "10.0.0.5:8090", resolve: resolver))
        #expect(await !isSameAddress("iggy-server:8090", "10.0.0.5:8091", resolve: resolver))
        #expect(await !isSameAddress("iggy-server:8090", "other:8090", resolve: resolver))
        // A name the resolver cannot answer for is a different endpoint.
        #expect(await !isSameAddress("unknown:8090", "10.0.0.5:8090", resolve: resolver))
        // Two literals are compared as written and never resolved.
        #expect(await !isSameAddress("10.0.0.5:8090", "10.0.0.6:8090") { _ in ["10.0.0.5"] })
        #expect(await isSameAddress("LOCALHOST:8090", "127.0.0.1:8090") { _ in nil })
    }

    @Test func literalAddressesResolveToThemselves() async {
        #expect(await resolveAddresses(of: "127.0.0.1") == ["127.0.0.1"])
        #expect(await resolveAddresses(of: "::1") == ["::1"])
    }

    @Test func durationsSaturateAsNanoseconds() {
        #expect(Duration.milliseconds(1500).nanosecondsClamped == 1_500_000_000)
        #expect(Duration.seconds(Int64.max).nanosecondsClamped == Int64.max)
        #expect(Duration.seconds(Int64.min).nanosecondsClamped == Int64.min)
        #expect(Duration.zero.nanosecondsClamped == 0)
    }
}

@Suite("Client configuration")
struct ClientConfigurationTests {
    @Test func tlsServerNameSkipsIPLiterals() throws {
        let literal = TLSOptions()
        #expect(literal.serverName(for: try Endpoint(parsing: "127.0.0.1:8090")) == nil)
        #expect(literal.serverName(for: try Endpoint(parsing: "[::1]:8090")) == nil)
        #expect(literal.serverName(for: try Endpoint(parsing: "iggy.example:8090")) == "iggy.example")
        let named = TLSOptions(domain: "localhost")
        #expect(named.serverName(for: try Endpoint(parsing: "127.0.0.1:8090")) == "localhost")
        #expect(TLSOptions(domain: "").serverName(for: try Endpoint(parsing: "iggy.example:8090")) == "iggy.example")
        #expect(TLSOptions(domain: "10.0.0.5").serverName(for: try Endpoint(parsing: "iggy.example:8090")) == nil)
    }

    @Test func missingCertificateAuthorityFileIsReported() {
        let options = TLSOptions(caFile: "/nonexistent/iggy-ca.pem")
        #expect(throws: IggyError(.invalidTlsCertificatePath)) { try options.makeContext() }
        // Without validation the file is never read.
        #expect(throws: Never.self) { try TLSOptions(caFile: "/nonexistent/iggy-ca.pem", validateCertificate: false).makeContext() }
    }

    /// A configuration ends up in logs and crash reports, so no description
    /// of it may carry the password or the token.
    @Test func descriptionsNeverCarryTheSecret() {
        let password = ClientConfiguration(autoLogin: .usernamePassword(username: "iggy", password: "hunter2-secret"))
        let token = ClientConfiguration(autoLogin: .personalAccessToken("iggypat-hunter2-secret"))
        for configuration in [password, token] {
            #expect(!"\(configuration)".contains("hunter2"))
            #expect(!String(reflecting: configuration).contains("hunter2"))
            #expect(!"\(String(describing: configuration.autoLogin))".contains("hunter2"))
        }
        #expect("\(Credentials.usernamePassword(username: "iggy", password: "hunter2"))" == "username iggy")
    }

    @Test func statesAndEventsDescribeThemselves() {
        #expect(ClientState.authenticated.description == "authenticated")
        #expect(DiagnosticEvent.signedIn.description == "signed_in")
    }
}
