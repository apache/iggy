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
import NIOCore
import NIOPosix

/// A `host:port` pair, with IPv6 hosts in brackets.
struct Endpoint: Sendable, Hashable, CustomStringConvertible {
    let host: String
    let port: Int

    /// Parses `host:port` or `[v6]:port`.
    init(parsing address: String) throws {
        let trimmed = address.trimmingCharacters(in: .whitespaces)
        guard let colon = trimmed.lastIndex(of: ":") else {
            throw IggyError(.invalidServerAddress, context: address)
        }
        var hostPart = trimmed[..<colon]
        let portPart = trimmed[trimmed.index(after: colon)...]
        if hostPart.hasPrefix("["), hostPart.hasSuffix("]") {
            hostPart = hostPart.dropFirst().dropLast()
        } else if hostPart.contains(":") {
            // An IPv6 host without brackets cannot be told from its port.
            throw IggyError(.invalidServerAddress, context: address)
        }
        guard !hostPart.isEmpty, portPart.allSatisfy({ $0.isASCII && $0.isNumber }), let port = UInt16(portPart), port > 0 else {
            throw IggyError(.invalidServerAddress, context: address)
        }
        host = String(hostPart)
        self.port = Int(port)
    }

    var description: String {
        host.contains(":") ? "[\(host)]:\(port)" : "\(host):\(port)"
    }

    /// Whether the host is a literal IP address rather than a name.
    var isIPLiteral: Bool { Self.isIPLiteral(host) }

    static func isIPLiteral(_ host: String) -> Bool {
        (try? SocketAddress(ipAddress: host, port: 0)) != nil
    }
}

/// Whether two addresses are written the same way, up to canonicalization
/// of the host and port. Cheap and non-blocking, so every comparison starts
/// here.
func isSameSpelling(_ lhs: String, _ rhs: String) -> Bool {
    guard let left = try? Endpoint(parsing: lhs), let right = try? Endpoint(parsing: rhs) else {
        return lhs == rhs
    }
    return left.port == right.port && canonicalHost(left.host) == canonicalHost(right.host)
}

private func canonicalHost(_ host: String) -> String {
    switch host.lowercased() {
    case "localhost", "127.0.0.1", "::1", "0.0.0.0", "::": "localhost"
    default: host.lowercased()
    }
}

/// How long a name lookup may take before the comparison gives up on it.
private let resolveTimeout: Duration = .seconds(2)

/// Whether two addresses name the same endpoint once host names resolve.
///
/// A host name and the address it resolves to are one endpoint: a client
/// configured as `iggy-server:8090` whose roster advertises `10.0.0.5:8090`
/// would otherwise redirect to the node it is already on. Tests pass their own
/// `resolve` so the outcome does not depend on the machine's resolver.
func isSameAddress(_ lhs: String, _ rhs: String, resolve: @Sendable (String) async -> Set<String>? = resolveAddresses) async -> Bool {
    if isSameSpelling(lhs, rhs) {
        return true
    }
    guard let left = try? Endpoint(parsing: lhs), let right = try? Endpoint(parsing: rhs), left.port == right.port else {
        return false
    }
    // Two literals that did not compare equal are different endpoints.
    if left.isIPLiteral && right.isIPLiteral {
        return false
    }
    guard let leftAddresses = await resolve(left.host), let rightAddresses = await resolve(right.host) else {
        return false
    }
    return !leftAddresses.isDisjoint(with: rightAddresses)
}

/// The numeric addresses of a host, or nil when it has none or the resolver
/// does not answer within ``resolveTimeout``.
///
/// `getaddrinfo` blocks and has no deadline of its own. It runs on the
/// blocking pool so a slow resolver cannot stall the tasks that share a
/// cooperative thread, and the caller stops waiting when the deadline fires.
func resolveAddresses(of host: String) async -> Set<String>? {
    let loop = MultiThreadedEventLoopGroup.singleton.any()
    let lookup = NIOThreadPool.singleton.runIfActive(eventLoop: loop) { numericAddresses(of: host) }
    let deadline = loop.scheduleTask(in: .nanoseconds(resolveTimeout.nanosecondsClamped)) { Set<String>() }
    // Cancelling the deadline fails its future, which hands the result over
    // to the lookup. A deadline that fires first answers with no addresses.
    lookup.whenComplete { _ in deadline.cancel() }
    let addresses = try? await deadline.futureResult.flatMapError { _ in lookup }.get()
    return addresses.flatMap { $0.isEmpty ? nil : $0 }
}

private func numericAddresses(of host: String) -> Set<String> {
    var hints = addrinfo()
    #if canImport(Glibc) || canImport(Musl)
        hints.ai_socktype = Int32(SOCK_STREAM.rawValue)
    #else
        hints.ai_socktype = SOCK_STREAM
    #endif
    var result: UnsafeMutablePointer<addrinfo>?
    guard getaddrinfo(host, nil, &hints, &result) == 0, let list = result else {
        return []
    }
    defer { freeaddrinfo(list) }
    var addresses = Set<String>()
    var current: UnsafeMutablePointer<addrinfo>? = list
    while let info = current {
        var buffer = [CChar](repeating: 0, count: Int(NI_MAXHOST))
        if getnameinfo(info.pointee.ai_addr, info.pointee.ai_addrlen, &buffer, socklen_t(buffer.count), nil, 0, NI_NUMERICHOST) == 0 {
            addresses.insert(String(decoding: buffer.prefix { $0 != 0 }.map { UInt8(bitPattern: $0) }, as: UTF8.self))
        }
        current = info.pointee.ai_next
    }
    return addresses
}

extension Duration {
    /// The duration in whole nanoseconds, saturating instead of overflowing.
    var nanosecondsClamped: Int64 {
        let (seconds, attoseconds) = components
        let whole = seconds.multipliedReportingOverflow(by: 1_000_000_000)
        let sum = whole.partialValue.addingReportingOverflow(attoseconds / 1_000_000_000)
        if whole.overflow || sum.overflow {
            return seconds < 0 ? Int64.min : Int64.max
        }
        return sum.partialValue
    }
}
