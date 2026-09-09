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
import NIOSSL
import NIOTLS

/// One complete consensus frame as read off the wire.
struct RawFrame: Sendable {
    let header: [UInt8]
    let body: [UInt8]
}

/// A transport that carries consensus frames to one endpoint.
///
/// The protocol is lockstep: one request is written, exactly one reply is
/// read. The client serializes callers; the connection only moves bytes.
protocol IggyConnection: AnyObject, Sendable {
    var localAddress: String? { get }
    var remoteAddress: String? { get }
    /// Writes a frame and reads the reply, bounded by `timeout`. A timeout
    /// leaves the stream at an unknown boundary, so the connection closes
    /// itself and every later call fails.
    func exchange(_ frame: [UInt8], timeout: Duration) async throws -> RawFrame
    func close() async
}

/// A dialer that produces connections; swapped out by tests.
protocol IggyConnector: Sendable {
    func connect(to endpoint: Endpoint, tls: TLSOptions?, timeout: Duration?, noDelay: Bool) async throws -> any IggyConnection
}

/// A `host:port` pair, with IPv6 hosts in brackets.
struct Endpoint: Sendable, Hashable, CustomStringConvertible {
    let host: String
    let port: Int

    /// Parses `host:port`, `[v6]:port`, or a bare port-less host (rejected).
    init(parsing address: String) throws {
        let trimmed = address.trimmingCharacters(in: .whitespaces)
        if trimmed.hasPrefix("[") {
            guard let close = trimmed.firstIndex(of: "]"), trimmed.index(after: close) < trimmed.endIndex,
                trimmed[trimmed.index(after: close)] == ":", let port = Int(trimmed[trimmed.index(close, offsetBy: 2)...])
            else {
                throw IggyError(.invalidServerAddress, context: address)
            }
            host = String(trimmed[trimmed.index(after: trimmed.startIndex)..<close])
            self.port = port
        } else {
            guard let colon = trimmed.lastIndex(of: ":"), let port = Int(trimmed[trimmed.index(after: colon)...]),
                trimmed[..<colon].firstIndex(of: ":") == nil
            else {
                throw IggyError(.invalidServerAddress, context: address)
            }
            host = String(trimmed[..<colon])
            self.port = port
        }
        guard !host.isEmpty, port > 0, port <= 65_535 else {
            throw IggyError(.invalidServerAddress, context: address)
        }
    }

    var description: String {
        host.contains(":") ? "[\(host)]:\(port)" : "\(host):\(port)"
    }

    /// Whether the host is a literal IP address rather than a name.
    var isIPLiteral: Bool {
        host.contains(":") || host.split(separator: ".").count == 4 && host.allSatisfy { $0.isNumber || $0 == "." }
    }
}

/// The NIO-backed dialer used in production.
struct NIOConnector: IggyConnector {
    func connect(to endpoint: Endpoint, tls: TLSOptions?, timeout: Duration?, noDelay: Bool) async throws -> any IggyConnection {
        try await TCPConnection.connect(to: endpoint, tls: tls, timeout: timeout, noDelay: noDelay)
    }
}

/// A TCP connection, optionally wrapped in TLS, speaking the consensus
/// framing. Backed by SwiftNIO so the same code runs on Apple platforms and
/// Linux.
final class TCPConnection: IggyConnection, @unchecked Sendable {
    private let channel: Channel
    private let handler: ExchangeHandler
    let localAddress: String?
    let remoteAddress: String?

    private init(channel: Channel, handler: ExchangeHandler) {
        self.channel = channel
        self.handler = handler
        localAddress = channel.localAddress.map(Self.describe)
        remoteAddress = channel.remoteAddress.map(Self.describe)
    }

    private static func describe(_ address: SocketAddress) -> String {
        switch address {
        case .v4(let v4): "\(v4.host):\(address.port ?? 0)"
        case .v6(let v6): "[\(v6.host)]:\(address.port ?? 0)"
        case .unixDomainSocket: address.description
        }
    }

    static func connect(to endpoint: Endpoint, tls: TLSOptions?, timeout: Duration?, noDelay: Bool = true) async throws -> TCPConnection {
        let sslContext: NIOSSLContext?
        if let tls {
            sslContext = try tls.makeContext()
        } else {
            sslContext = nil
        }
        let handler = ExchangeHandler()
        // TCP_NODELAY lives at the TCP level; the socket-level shorthand would
        // set SO_DEBUG instead, which Linux refuses without CAP_NET_ADMIN.
        let noDelayValue: ChannelOptions.Types.SocketOption.Value = noDelay ? 1 : 0
        let bootstrap = ClientBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .channelOption(.tcpOption(.tcp_nodelay), value: noDelayValue)
            .connectTimeout(timeout.map { TimeAmount.nanoseconds(Int64(clamping: $0.nanosecondsClamped)) } ?? .seconds(30))
            .channelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    if let sslContext {
                        let serverHostname = tls?.serverName(for: endpoint)
                        let sslHandler = try NIOSSLClientHandler(context: sslContext, serverHostname: serverHostname)
                        try channel.pipeline.syncOperations.addHandler(sslHandler)
                    }
                    try channel.pipeline.syncOperations.addHandler(ByteToMessageHandler(FrameDecoder()))
                    try channel.pipeline.syncOperations.addHandler(handler)
                }
            }
        let channel: Channel
        do {
            channel = try await bootstrap.connect(host: endpoint.host, port: endpoint.port).get()
        } catch {
            throw IggyError(.cannotEstablishConnection, context: "\(endpoint): \(error)")
        }
        if sslContext != nil {
            // The handshake completes asynchronously after connect; waiting for
            // it here means a failed handshake is reported by the dial rather
            // than by the first request.
            do {
                try await handler.handshakeDone(on: channel)
            } catch {
                try? await channel.close().get()
                throw IggyError(.cannotEstablishConnection, context: "TLS handshake with \(endpoint) failed: \(error)")
            }
        }
        return TCPConnection(channel: channel, handler: handler)
    }

    func exchange(_ frame: [UInt8], timeout: Duration) async throws -> RawFrame {
        let channel = self.channel
        let handler = self.handler
        return try await withThrowingTaskGroup(of: RawFrame.self) { group in
            group.addTask {
                try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<RawFrame, Error>) in
                    channel.eventLoop.execute {
                        handler.expectReply(continuation)
                        var buffer = channel.allocator.buffer(capacity: frame.count)
                        buffer.writeBytes(frame)
                        channel.writeAndFlush(buffer).whenFailure { error in
                            handler.fail(with: IggyError(.disconnected, context: "write failed: \(error)"))
                            channel.close(promise: nil)
                        }
                    }
                }
            }
            group.addTask {
                try await Task.sleep(for: timeout)
                throw IggyError(.disconnected, context: "timed out after \(timeout) waiting for the reply")
            }
            do {
                let frame = try await group.next()!
                group.cancelAll()
                return frame
            } catch {
                group.cancelAll()
                // Whether the reply timed out or the socket died, the stream is
                // at an unknown boundary: a late reply would desync framing.
                channel.close(promise: nil)
                throw error
            }
        }
    }

    func close() async {
        try? await channel.close().get()
    }
}

extension Duration {
    var nanosecondsClamped: Int64 {
        let (seconds, attoseconds) = components
        let nanos = seconds.multipliedReportingOverflow(by: 1_000_000_000)
        if nanos.overflow {
            return Int64.max
        }
        return nanos.partialValue.addingReportingOverflow(attoseconds / 1_000_000_000).partialValue
    }
}

/// Splits the byte stream into frames: a fixed 256-byte header whose size
/// field says how many body bytes follow.
final class FrameDecoder: ByteToMessageDecoder {
    typealias InboundOut = RawFrame

    func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        guard buffer.readableBytes >= VSRFrame.headerSize else {
            return .needMoreData
        }
        let header = buffer.getBytes(at: buffer.readerIndex, length: VSRFrame.headerSize)!
        guard let size = VSRFrame.readSize(header[...]), size >= VSRFrame.headerSize, Int(size) <= VSRFrame.maxFrameSize else {
            throw IggyError(.invalidCommand, context: "reply declares an invalid frame size")
        }
        let total = Int(size)
        guard buffer.readableBytes >= total else {
            return .needMoreData
        }
        buffer.moveReaderIndex(forwardBy: VSRFrame.headerSize)
        let body = buffer.readBytes(length: total - VSRFrame.headerSize)!
        context.fireChannelRead(wrapInboundOut(RawFrame(header: header, body: body)))
        return .continue
    }

    func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        try decode(context: context, buffer: &buffer)
    }
}

/// Hands each frame to the one caller waiting for it. All access happens on
/// the channel's event loop.
final class ExchangeHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = RawFrame

    private var pending: CheckedContinuation<RawFrame, Error>?
    private var handshake: CheckedContinuation<Void, Error>?
    private var handshakeComplete = false
    private var closed = false
    private var closeError: IggyError?

    func expectReply(_ continuation: CheckedContinuation<RawFrame, Error>) {
        if closed {
            continuation.resume(throwing: closeError ?? IggyError(.disconnected, context: "connection closed"))
            return
        }
        if let previous = pending {
            previous.resume(throwing: IggyError(.invalidCommand, context: "a reply was already awaited on this connection"))
        }
        pending = continuation
    }

    func fail(with error: IggyError) {
        pending?.resume(throwing: error)
        pending = nil
    }

    func handshakeDone(on channel: Channel) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            channel.eventLoop.execute {
                if self.handshakeComplete {
                    continuation.resume()
                } else if self.closed {
                    continuation.resume(throwing: self.closeError ?? IggyError(.disconnected))
                } else {
                    self.handshake = continuation
                }
            }
        }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let frame = unwrapInboundIn(data)
        if let pending {
            self.pending = nil
            pending.resume(returning: frame)
        }
        // A frame nobody waits for is either a stray reply or a session-level
        // eviction; the next exchange discovers the closed channel either way.
    }

    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if case .handshakeCompleted = event as? TLSUserEvent {
            handshakeComplete = true
            handshake?.resume()
            handshake = nil
        }
        context.fireUserInboundEventTriggered(event)
    }

    func errorCaught(context: ChannelHandlerContext, error: Error) {
        let mapped = (error as? IggyError) ?? IggyError(.disconnected, context: "\(error)")
        closeError = mapped
        handshake?.resume(throwing: mapped)
        handshake = nil
        fail(with: mapped)
        context.close(promise: nil)
    }

    func channelInactive(context: ChannelHandlerContext) {
        closed = true
        let error = closeError ?? IggyError(.disconnected, context: "connection closed by the peer")
        handshake?.resume(throwing: error)
        handshake = nil
        fail(with: error)
        context.fireChannelInactive()
    }
}
