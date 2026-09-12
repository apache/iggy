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

import Foundation
import Iggy

/// Credentials of the root user every example signs in with.
public enum Defaults {
    public static let rootUsername = "iggy"
    public static let rootPassword = "iggy"
    public static let streamName = "sample-stream"
    public static let topicName = "sample-topic"
    public static let partitionID: UInt32 = 0
    public static let messagesPerBatch = 10
    public static let batchesLimit = 5
    public static let interval: Duration = .milliseconds(500)
}

/// The command-line flags shared by the examples:
///
/// ```text
/// --tcp-server-address host:port   server to connect to (127.0.0.1:8090)
/// --tls                            enable TLS
/// --tls-ca-file path               CA certificate that signed the server's
/// --tls-domain name                server name to verify (defaults to the host)
/// ```
public struct ExampleOptions: Sendable {
    public var serverAddress = "127.0.0.1:8090"
    public var tls = false
    public var tlsCAFile: String?
    public var tlsDomain: String?

    public init() {}

    /// Parses the process arguments, exiting with usage on a mistake.
    public static func parse(_ arguments: [String] = Array(CommandLine.arguments.dropFirst())) -> ExampleOptions {
        var options = ExampleOptions()
        var index = arguments.startIndex
        func value(for flag: String) -> String {
            index += 1
            guard index < arguments.endIndex else {
                usage("\(flag) needs a value")
            }
            return arguments[index]
        }
        while index < arguments.endIndex {
            switch arguments[index] {
            case "--tcp-server-address": options.serverAddress = value(for: "--tcp-server-address")
            case "--tls": options.tls = true
            case "--tls-ca-file": options.tlsCAFile = value(for: "--tls-ca-file")
            case "--tls-domain": options.tlsDomain = value(for: "--tls-domain")
            case "--help", "-h": usage(nil)
            default: usage("unknown argument \(arguments[index])")
            }
            index += 1
        }
        return options
    }

    /// A client configured from the flags, signing in as root on connect.
    public func makeClient() -> IggyClient {
        var configuration = ClientConfiguration(
            address: serverAddress, autoLogin: .usernamePassword(username: Defaults.rootUsername, password: Defaults.rootPassword))
        if tls {
            configuration.tls = TLSOptions(domain: tlsDomain, caFile: tlsCAFile)
            print("TLS enabled with CA file: \(tlsCAFile ?? "system roots"), domain: \(tlsDomain ?? "from the address")")
        }
        return IggyClient(configuration: configuration)
    }

    private static func usage(_ problem: String?) -> Never {
        if let problem {
            print("error: \(problem)")
        }
        print(
            """
            usage: <example> [--tcp-server-address host:port] [--tls] [--tls-ca-file path] [--tls-domain name]
            """)
        exit(problem == nil ? 0 : 2)
    }
}
