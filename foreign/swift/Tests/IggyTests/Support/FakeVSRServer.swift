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

import NIOCore
import NIOPosix

@testable import Iggy

/// A scripted server speaking the consensus framing on loopback, so the
/// client's reconnection, replay, eviction, and timeout paths run over real
/// sockets without a server binary.
final class FakeVSRServer: @unchecked Sendable {
    /// One decoded request frame.
    struct Request: Sendable {
        let connection: Int
        let operation: ConsensusOperation
        /// The command code, read from the header for non-replicated
        /// requests and mapped from the operation otherwise.
        let code: UInt32?
        let requestID: UInt64
        let session: UInt64
        let clientID: UInt128Value
        let checksum: UInt64
        let context: PartitionContext
        let payload: [UInt8]
    }

    /// What the script answers with.
    enum Reply: Sendable {
        /// Status zero with this body; result-framed operations get the
        /// empty result section prepended.
        case ok([UInt8])
        /// `ok` from a metadata operation that committed at `commit`.
        case committed([UInt8], commit: UInt64)
        /// A pre-commit denial: the status in the header, no body.
        case status(UInt32)
        /// A committed rejection carried in the result section.
        case committedRejection(UInt32)
        case eviction(VSRFrame.EvictionReason)
        /// A reply that answers some other request id.
        case wrongRequestID(UInt64)
        /// Closes the connection instead of answering.
        case close
        /// Never answers.
        case stall
        /// A status-zero reply written in two TCP chunks with a pause between.
        case okInChunks([UInt8])
    }

    typealias Responder = @Sendable (Request) async -> Reply

    let state = FakeServerState()
    private let group = MultiThreadedEventLoopGroup.singleton
    private var channel: Channel?
    private(set) var port = 0

    var address: String { "127.0.0.1:\(port)" }

    var requests: [Request] {
        get async { await state.requests }
    }

    var connectionCount: Int {
        get async { await state.opened }
    }

    /// Connections that are still open.
    var openConnectionCount: Int {
        get async { await state.openCount }
    }

    /// Replaces the script; the default answers everything successfully.
    func respond(_ responder: @escaping Responder) async {
        await state.setResponder(responder)
    }

    func start() async throws {
        try await startOn(port: 0)
    }

    /// Binds a specific port, for a server that comes back after a stop.
    func startOn(port: Int) async throws {
        let state = state
        let bootstrap = ServerBootstrap(group: group)
            .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
            .childChannelInitializer { channel in
                channel.eventLoop.makeCompletedFuture {
                    try channel.pipeline.syncOperations.addHandler(ByteToMessageHandler(FrameDecoder()))
                    try channel.pipeline.syncOperations.addHandler(FakeServerHandler(state: state))
                }
            }
        let channel = try await bootstrap.bind(host: "127.0.0.1", port: port).get()
        self.channel = channel
        self.port = channel.localAddress!.port!
        await state.setAddress(address)
    }

    func stop() async {
        await state.closeAll()
        try? await channel?.close().get()
        channel = nil
    }

    func closeAllConnections() async {
        await state.closeAll()
    }

    // MARK: Reply helpers

    /// The body of a successful register reply.
    static func registerBody(userID: UInt32 = 0, session: UInt64 = 1, serverVersion: String = "fake") -> [UInt8] {
        var writer = ByteWriter()
        writer.write(userID)
        writer.write(session)
        writer.write(ProtocolVersion.current.packed)
        try! writer.writeName(serverVersion)
        return writer.bytes
    }

    /// A roster with one node per address; the first is the leader.
    static func rosterBody(_ addresses: [String], leaderIndex: Int = 0) -> [UInt8] {
        var writer = ByteWriter()
        try! writer.writeLongString("fake-cluster")
        writer.write(UInt32(addresses.count))
        for (index, address) in addresses.enumerated() {
            writeNode(address, replicaID: UInt8(index), leader: index == leaderIndex, into: &writer)
        }
        return writer.bytes
    }

    static func writeNode(_ address: String, replicaID: UInt8, leader: Bool, into writer: inout ByteWriter) {
        let endpoint = try! Endpoint(parsing: address)
        writer.write(replicaID)
        try! writer.writeLongString("node-\(replicaID)")
        try! writer.writeLongString(endpoint.host)
        writer.write(UInt16(endpoint.port))
        writer.write(UInt16(0))
        writer.write(UInt16(0))
        writer.write(UInt16(0))
        writer.write(leader ? ClusterNodeRole.leader.rawValue : ClusterNodeRole.follower.rawValue)
        writer.write(ClusterNodeStatus.healthy.rawValue)
    }

    /// The reply to `GetPartitionRoute`.
    static func partitionRouteBody(_ context: PartitionContext, partitionsCount: UInt32 = 3, primaryReplica: UInt8 = 0) -> [UInt8] {
        var writer = ByteWriter()
        writer.write(context.incarnation)
        writer.write(context.metadataOp)
        writer.write(UInt32(1))
        writer.write(partitionsCount)
        writer.write(primaryReplica)
        return writer.bytes
    }

    /// The reply to `GetPollRouting` and `GetConsumerOffsetRouting`: the
    /// session of `request` and a primary at `primary`.
    static func pollRouteBody(for request: Request, context: PartitionContext, primary: String, watermark: UInt64 = 0) -> [UInt8] {
        var writer = ByteWriter()
        writer.write(request.clientID)
        writer.write(request.session)
        writer.write(watermark)
        context.encode(into: &writer)
        writeNode(primary, replicaID: 0, leader: true, into: &writer)
        return writer.bytes
    }

    /// A poll reply without messages.
    static func polledBody(partitionID: UInt32, context: PartitionContext = .empty) -> [UInt8] {
        var writer = ByteWriter()
        writer.write(partitionID)
        writer.write(UInt64(0))
        writer.write(UInt32(0))
        context.encode(into: &writer)
        return writer.bytes
    }

    /// The reply to `SyncConsumerGroup` for a member.
    static func assignmentBody(generation: UInt64, partitions: [UInt32]) -> [UInt8] {
        var writer = ByteWriter()
        writer.write(generation)
        writer.write(UInt32(partitions.count))
        for partition in partitions {
            writer.write(partition)
        }
        return writer.bytes
    }

    /// A topic with three partitions, 0 to 2, under `contexts`.
    static func topicBody(contexts: [PartitionContext]) -> [UInt8] {
        precondition(contexts.count == 3, "the golden topic header names three partitions")
        var writer = ByteWriter()
        writer.write(GoldenFixture.shared.bytes("response.topic_header"))
        for (id, context) in contexts.enumerated() {
            writer.write(UInt32(id))
            writer.write(UInt64(0))
            writer.write(UInt32(1))
            writer.write(UInt64(0))
            writer.write(UInt64(0))
            writer.write(UInt64(0))
            context.encode(into: &writer)
        }
        return writer.bytes
    }

    static func encodeReply(request: Request, operation: ConsensusOperation, status: UInt32, body: [UInt8], commit: UInt64 = 0) -> [UInt8] {
        var frame = [UInt8](repeating: 0, count: VSRFrame.headerSize)
        frame.withUnsafeMutableBytes { buffer in
            buffer.storeBytes(of: UInt32(VSRFrame.headerSize + body.count).littleEndian, toByteOffset: VSRFrame.ReplyOffset.size, as: UInt32.self)
            buffer[VSRFrame.ReplyOffset.command] = VSRFrame.Command.reply.rawValue
            buffer.storeBytes(of: commit.littleEndian, toByteOffset: VSRFrame.ReplyOffset.commit, as: UInt64.self)
            buffer.storeBytes(of: request.requestID.littleEndian, toByteOffset: VSRFrame.ReplyOffset.request, as: UInt64.self)
            buffer[VSRFrame.ReplyOffset.operation] = operation.rawValue
            buffer.storeBytes(of: status.littleEndian, toByteOffset: VSRFrame.ReplyOffset.status, as: UInt32.self)
        }
        return frame + body
    }

    static func encodeEviction(request: Request, reason: VSRFrame.EvictionReason) -> [UInt8] {
        var frame = [UInt8](repeating: 0, count: VSRFrame.headerSize)
        frame.withUnsafeMutableBytes { buffer in
            buffer.storeBytes(of: UInt32(VSRFrame.headerSize).littleEndian, toByteOffset: VSRFrame.ReplyOffset.size, as: UInt32.self)
            buffer[VSRFrame.ReplyOffset.command] = VSRFrame.Command.eviction.rawValue
            buffer.storeBytes(of: request.clientID.low.littleEndian, toByteOffset: VSRFrame.RequestOffset.client, as: UInt64.self)
            buffer.storeBytes(of: request.clientID.high.littleEndian, toByteOffset: VSRFrame.RequestOffset.client + 8, as: UInt64.self)
            buffer.storeBytes(of: ProtocolVersion.current.packed.littleEndian, toByteOffset: VSRFrame.EvictionOffset.serverProtocolVersion, as: UInt32.self)
            buffer.storeBytes(of: ProtocolVersion.current.packed.littleEndian, toByteOffset: VSRFrame.EvictionOffset.serverProtocolVersionMin, as: UInt32.self)
            buffer[VSRFrame.EvictionOffset.reason] = reason.rawValue
        }
        return frame
    }

    static func decodeRequest(connection: Int, frame: RawFrame) -> Request {
        let header = frame.header
        func u32(_ offset: Int) -> UInt32 { UInt32(littleEndianBytes: header[offset..<offset + 4]) }
        func u64(_ offset: Int) -> UInt64 { UInt64(littleEndianBytes: header[offset..<offset + 8]) }
        let operation = ConsensusOperation(rawValue: header[VSRFrame.RequestOffset.operation]) ?? .reserved
        let code: UInt32?
        switch operation {
        case .nonReplicated: code = u32(VSRFrame.RequestOffset.reserved)
        case .logout: code = CommandCode.logoutUser.rawValue
        case .register: code = nil
        default: code = CommandCode.allCases.first { ConsensusOperation.replicated(for: $0.rawValue) == operation }?.rawValue
        }
        return Request(
            connection: connection, operation: operation, code: code, requestID: u64(VSRFrame.RequestOffset.request),
            session: u64(VSRFrame.RequestOffset.session),
            clientID: UInt128Value(low: u64(VSRFrame.RequestOffset.client), high: u64(VSRFrame.RequestOffset.client + 8)),
            checksum: u64(VSRFrame.RequestOffset.requestChecksum),
            context: PartitionContext(
                incarnation: u64(VSRFrame.RequestOffset.partitionIncarnation), ownerGeneration: u64(VSRFrame.RequestOffset.ownerGeneration),
                metadataOp: u64(VSRFrame.RequestOffset.minimumMetadataOp)),
            payload: frame.body)
    }

    /// The partition context the default script routes every partition to.
    static let routeContext = PartitionContext(incarnation: 7, ownerGeneration: 0, metadataOp: 10)

    /// The answer to a register and to a resume: the session is bound, as on
    /// a node that holds it. Nil for any other request.
    static func sessionReply(_ request: Request) -> Reply? {
        if request.operation == .register {
            return .ok(registerBody())
        }
        if request.code == CommandCode.bindSession.rawValue {
            return .ok(registerBody(session: request.session))
        }
        return nil
    }

    /// Answers every request successfully with an empty body, a bound
    /// session for a register and a bind, a single-node roster for the
    /// metadata read, routes to this server under ``routeContext``, and an
    /// empty poll.
    static func defaultReply(_ request: Request, address: String) -> Reply {
        if request.operation == .register {
            return .ok(registerBody())
        }
        switch request.code.flatMap(CommandCode.init(rawValue:)) {
        case .getClusterMetadata:
            return .ok(rosterBody([address]))
        case .bindSession:
            return .ok(registerBody(session: request.session))
        case .getPartitionRoute:
            return .ok(partitionRouteBody(routeContext))
        case .getPollRouting, .getConsumerOffsetRouting:
            return .ok(pollRouteBody(for: request, context: routeContext, primary: address))
        case .pollMessagesOnPrimary:
            return .ok(polledBody(partitionID: 0, context: routeContext))
        default:
            return .ok([])
        }
    }
}

actor FakeServerState {
    private(set) var requests: [FakeVSRServer.Request] = []
    private(set) var opened = 0
    private var channels: [ObjectIdentifier: Channel] = [:]
    private var responder: FakeVSRServer.Responder?
    var address = ""

    func setResponder(_ responder: @escaping FakeVSRServer.Responder) {
        self.responder = responder
    }

    func setAddress(_ address: String) {
        self.address = address
    }

    func connectionOpened(_ channel: Channel) -> Int {
        opened += 1
        channels[ObjectIdentifier(channel)] = channel
        return opened
    }

    func connectionClosed(_ channel: Channel) {
        channels[ObjectIdentifier(channel)] = nil
    }

    var openCount: Int { channels.count }

    func closeAll() {
        for channel in channels.values {
            channel.close(promise: nil)
        }
        channels.removeAll()
    }

    func handle(_ request: FakeVSRServer.Request) async -> FakeVSRServer.Reply {
        requests.append(request)
        if let responder {
            return await responder(request)
        }
        return FakeVSRServer.defaultReply(request, address: address)
    }
}

final class FakeServerHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = RawFrame

    private let state: FakeServerState
    private var connection = 0

    init(state: FakeServerState) {
        self.state = state
    }

    func channelActive(context: ChannelHandlerContext) {
        let channel = context.channel
        Task { [self] in
            connection = await state.connectionOpened(channel)
        }
    }

    func channelInactive(context: ChannelHandlerContext) {
        let channel = context.channel
        Task { await state.connectionClosed(channel) }
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let frame = unwrapInboundIn(data)
        let channel = context.channel
        Task { [self] in
            let request = FakeVSRServer.decodeRequest(connection: connection, frame: frame)
            let reply = await state.handle(request)
            let operation = request.operation
            func write(_ bytes: [UInt8]) {
                var buffer = channel.allocator.buffer(capacity: bytes.count)
                buffer.writeBytes(bytes)
                channel.writeAndFlush(buffer, promise: nil)
            }
            func framedBody(_ body: [UInt8]) -> [UInt8] {
                (operation.isResultFramed || operation == .register) ? [0, 0, 0, 0] + body : body
            }
            switch reply {
            case .ok(let body):
                write(FakeVSRServer.encodeReply(request: request, operation: operation, status: 0, body: framedBody(body)))
            case .committed(let body, let commit):
                write(FakeVSRServer.encodeReply(request: request, operation: operation, status: 0, body: framedBody(body), commit: commit))
            case .status(let status):
                write(FakeVSRServer.encodeReply(request: request, operation: operation, status: status, body: []))
            case .committedRejection(let code):
                var writer = ByteWriter()
                writer.write(UInt32(1))
                writer.write(UInt32(0))
                writer.write(code)
                write(FakeVSRServer.encodeReply(request: request, operation: operation, status: 0, body: writer.bytes))
            case .eviction(let reason):
                write(FakeVSRServer.encodeEviction(request: request, reason: reason))
            case .wrongRequestID(let requestID):
                let wrong = FakeVSRServer.Request(
                    connection: request.connection, operation: operation, code: request.code, requestID: requestID, session: request.session,
                    clientID: request.clientID, checksum: request.checksum, context: request.context, payload: request.payload)
                write(FakeVSRServer.encodeReply(request: wrong, operation: operation, status: 0, body: framedBody([])))
            case .close:
                channel.close(promise: nil)
            case .stall:
                break
            case .okInChunks(let body):
                let frame = FakeVSRServer.encodeReply(request: request, operation: operation, status: 0, body: framedBody(body))
                let split = frame.count / 2
                write(Array(frame[..<split]))
                try? await Task.sleep(for: .milliseconds(20))
                write(Array(frame[split...]))
            }
        }
    }
}

extension FakeVSRServer {
    /// A started server whose default script knows its own address.
    static func started() async throws -> FakeVSRServer {
        let server = FakeVSRServer()
        try await server.start()
        await server.state.setAddress(server.address)
        return server
    }
}
