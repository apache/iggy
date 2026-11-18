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

/// The list page contract that every SDK shares, checked against
/// `Fixtures/list_pages.json` from the Rust codecs. This SDK has no HTTP
/// transport, so `http_query` is left out.
@Suite("List pages match the shared fixture")
struct ListPagesFixtureTests {
    let fixture = ListPagesFixture.shared

    @Test func givenFixtureWhenLoadingShouldHoldEveryVectorGroupForEveryCommand() {
        #expect(fixture.schemaVersion == 1)
        let groups = [
            fixture.cursors.count, fixture.opaqueCursors.count, fixture.requests.count, fixture.responses.count, fixture.opaqueResponses.count,
            fixture.refused.cursors.count, fixture.refused.queries.count, fixture.refused.responses.count,
        ]
        #expect(groups.allSatisfy { $0 > 0 }, "\(groups)")
        #expect(Set(fixture.requests.map(\.command)) == Set(listCommands.keys))
        #expect(Set(fixture.responses.map(\.command)) == Set(listCommands.keys))
    }

    @Test func givenFixtureConstantsWhenComparingPublicTypesShouldMatchCodesOrdersAndLimits() {
        let constants = fixture.constants
        #expect(constants.codes == listCommands.mapValues(\.rawValue))
        for (name, code) in constants.codes {
            #expect(ConsensusOperation.forCode(code) == .nonReplicated, "\(name)")
        }
        #expect(constants.orders == pageOrders.mapValues(\.rawValue))
        #expect(Set(pageOrders.values) == Set(PageOrder.allCases))
        #expect(PageCursor.textPrefix == constants.cursorPrefix)
        #expect(PageCursor.maxSize == constants.maxCursorBytes)
        #expect(PageQuery.defaultLimit == constants.defaultLimit)
        #expect(PageQuery().limit == constants.defaultLimit)
        #expect(PageQuery.maxLimit == constants.maxLimit)
        #expect(NamedPageQuery.maxNameSize == constants.maxNameBytes)
        let errors = ["invalid_command": IggyErrorCode.invalidCommand, "reply_too_large": .replyTooLarge]
        #expect(constants.errors == errors.mapValues(\.rawValue))
        #expect(errors.allSatisfy { $0.key == $0.value.name })
    }

    /// The client checks only the text of a cursor, so the opaque cursors that
    /// the server refuses parse too.
    @Test func givenCursorVectorsWhenParsingShouldKeepTheirBytesAndText() throws {
        for vector in fixture.cursors + fixture.opaqueCursors {
            let cursor = try PageCursor(text: vector.text)
            #expect(cursor.description == vector.text, "\(vector.name)")
            #expect(cursor.bytes == [UInt8](hex: vector.hex), "\(vector.name)")
            #expect(PageCursor(bytes: [UInt8](hex: vector.hex)) == cursor, "\(vector.name)")
        }
    }

    /// Each page method sends the code of its command and the request bytes,
    /// and reads the reply with the item decoder of its command.
    @Test func givenRequestVectorsWhenReadingPagesShouldSendTheRequestBytesAndDecodeTheReply() async throws {
        var replies: [UInt32: ListPagesFixture.Response] = [:]
        for vector in fixture.responses {
            let code = try #require(listCommands[vector.command], "\(vector.command)").rawValue
            replies[code] = replies[code] ?? vector
        }
        let scripted = replies
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            if let code = request.code, let reply = scripted[code] {
                return .ok([UInt8](hex: reply.responseHex))
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await connectedClient(server)

        for vector in fixture.requests {
            let code = try #require(listCommands[vector.command], "\(vector.command)")
            let ids = try await readPage(
                client, code, query: try namedPageQuery(vector.input), streamID: vector.input.streamID?.identifier, topicID: vector.input.topicID?.identifier)
            #expect(ids == scripted[code.rawValue]?.items.map(\.id), "\(vector.name)")
            let sent = await server.requests.last { $0.code == code.rawValue }
            #expect(sent?.payload == [UInt8](hex: vector.requestHex), "\(vector.name)")
            #expect(sent?.operation == .nonReplicated, "\(vector.name)")
        }
        await client.shutdown()
    }

    @Test func givenResponseVectorsWhenDecodingShouldReachTheNextTextAndTheItemsInReplyOrder() throws {
        for vector in fixture.responses + fixture.opaqueResponses {
            let page = try decodePage(vector.command, [UInt8](hex: vector.responseHex))
            #expect(page.next == vector.next, "\(vector.name)")
            #expect(page.items == vector.items, "\(vector.name)")
        }
    }

    @Test func givenOpaqueNextWhenSendingItBackShouldKeepItsBytes() throws {
        for vector in fixture.opaqueResponses {
            let reply = [UInt8](hex: vector.responseHex)
            let next = try #require(try Responses.page(reply[...], item: Responses.stream(from:)).next, "\(vector.name)")
            // The block of a first page, with the [next_len][next] of the
            // reply as its [cursor_len][cursor] and no name.
            let replyNext = reply.prefix(1 + Int(reply[0]))
            let expected = [PageOrder.id.rawValue, 0] + PageQuery.defaultLimit.littleEndianBytes + replyNext + [0]
            #expect(try Requests.listPage(query: PageQuery(cursor: next)) == expected, "\(vector.name)")
        }
    }

    @Test func givenRefusedCursorTextWhenParsingShouldRefuseWithoutRepeatingTheText() {
        for vector in fixture.refused.cursors {
            do {
                _ = try PageCursor(text: vector.text)
                Issue.record("\(vector.name) parsed")
            } catch let error as IggyError {
                #expect(error.code == .invalidCommand, "\(vector.name)")
                #expect(vector.text.isEmpty || !String(reflecting: error).contains(vector.text), "\(vector.name)")
            } catch {
                Issue.record("\(vector.name): \(error)")
            }
        }
    }

    @Test func givenRefusedQueriesWhenReadingAPageShouldRefuseBeforeSendingAnything() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await connectedClient(server)
        for vector in fixture.refused.queries {
            let query = try namedPageQuery(vector.input)
            #expect(throws: IggyError(.invalidCommand), "\(vector.name)") { try query.validate() }
            if query.name == nil {
                #expect(throws: IggyError(.invalidCommand), "\(vector.name)") { try query.page.validate() }
            }
            // The partitions page takes a PageQuery, which has no name.
            for code in listCommands.values where code != .getPartitions || query.name == nil {
                await expectCode(.invalidCommand) {
                    _ = try await readPage(client, code, query: query, streamID: ListPagesTests.streamID, topicID: ListPagesTests.topicID)
                }
            }
        }
        let codes = Set(listCommands.values.map(\.rawValue))
        #expect(await server.requests.allSatisfy { $0.code.map { !codes.contains($0) } ?? true })
        await client.shutdown()
    }

    @Test func givenRefusedResponsesWhenDecodingShouldRefuseAsMalformed() {
        for vector in fixture.refused.responses {
            #expect(throws: IggyError(.invalidCommand), "\(vector.name)") { try decodePage(vector.command, [UInt8](hex: vector.responseHex)) }
        }
    }
}

/// The Swift codecs and the client on their own: replies the fixture does not
/// hold, the cursor text form, the page rules, and the walk of the whole-list
/// methods through a scripted server.
@Suite("List page codecs and client", .serialized)
struct ListPagesTests {
    static let streamID: Identifier = 1
    static let topicID: Identifier = 2
    /// Each page of the timeout test answers after 0.6 of the request
    /// timeout, so one page fits a timeout and two pages do not.
    static let requestTimeout: Duration = .seconds(2)
    static let pageDelay: Duration = .milliseconds(1200)
    /// One item of each whole list, as the Rust encoders wrote it. Each item
    /// starts with its id.
    static let itemVectors: [CommandCode: String] = [
        .getStreams: "response.stream", .getTopics: "response.topic_header", .getConsumerGroups: "response.consumer_group", .getUsers: "response.user",
    ]

    let fixture = ListPagesFixture.shared

    @Test func givenResponseVectorsWhenCutShouldRefuseAsMalformed() {
        for vector in fixture.responses + fixture.opaqueResponses {
            let reply = [UInt8](hex: vector.responseHex)
            for length in 0..<reply.count {
                #expect(throws: IggyError(.invalidCommand), "\(vector.name) cut to \(length) bytes") {
                    try decodePage(vector.command, Array(reply.prefix(length)))
                }
            }
        }
    }

    @Test func givenAnyCursorBytesWhenPrintingAndParsingTheTextShouldRoundTrip() throws {
        var generator = SystemRandomNumberGenerator()
        for length in 1...PageCursor.maxSize {
            let bytes = (0..<length).map { _ in UInt8.random(in: .min ... .max, using: &generator) }
            let cursor = try #require(PageCursor(bytes: bytes), "\(length)")
            #expect(cursor.description.utf8.count <= PageCursor.maxTextLength, "\(length)")
            #expect(try PageCursor(text: cursor.description) == cursor, "\(length)")
        }
        #expect(PageCursor(bytes: []) == nil)
        #expect(PageCursor(bytes: Array(repeating: 0, count: PageCursor.maxSize + 1)) == nil)
    }

    @Test func givenPageRulesWhenValidatingShouldRefuseOnlyBrokenQueries() throws {
        // A name counts bytes: 127 two-byte characters and one letter fill it.
        let longestName = String(repeating: "é", count: NamedPageQuery.maxNameSize / 2) + "a"
        #expect(longestName.utf8.count == NamedPageQuery.maxNameSize)
        for page in [PageQuery(), PageQuery(limit: 1), PageQuery(limit: PageQuery.maxLimit), PageQuery(order: .created, descending: true)] {
            try page.validate()
            try NamedPageQuery(page: page, name: longestName).validate()
        }
        for page in [PageQuery(limit: 0), PageQuery(limit: PageQuery.maxLimit + 1)] {
            #expect(throws: IggyError(.invalidCommand), "\(page)") { try page.validate() }
            #expect(throws: IggyError(.invalidCommand), "\(page)") { try NamedPageQuery(page: page).validate() }
        }
        for name in ["", longestName + "a"] {
            #expect(throws: IggyError(.invalidCommand), "\(name.utf8.count) bytes") { try NamedPageQuery(name: name).validate() }
        }
    }

    /// The middle page is empty and still has a next cursor, so a walk that
    /// stops on an empty page loses the last items.
    @Test(arguments: [CommandCode.getStreams, .getTopics, .getConsumerGroups, .getUsers])
    func givenThreePagesWhenReadingTheWholeListShouldFollowEveryNextAndKeepEveryItem(_ command: CommandCode) async throws {
        let item = GoldenFixture.shared.bytes(try #require(Self.itemVectors[command]))
        let cursors = [try #require(PageCursor.id(1)), try #require(PageCursor.id(2))]
        let replies = [
            FakeVSRServer.pageBody(next: cursors[0].bytes, items: [entry(item, id: 1)]),
            FakeVSRServer.pageBody(next: cursors[1].bytes),
            FakeVSRServer.pageBody(items: [entry(item, id: 2), entry(item, id: 3)]),
        ]
        let server = try await scriptedServer(command, replies)
        defer { Task { await server.stop() } }
        let client = try await connectedClient(server)

        #expect(try await wholeList(client, command) == [1, 2, 3])
        let sent = await server.requests.filter { $0.code == command.rawValue }.map(\.payload)
        let expected = try [nil, cursors[0], cursors[1]].map { try request(command, PageQuery(limit: PageQuery.maxLimit, cursor: $0)) }
        #expect(sent == expected)
        await client.shutdown()
    }

    /// The third reply is a last page, so a walk that misses the repeat ends
    /// and fails the test instead of hanging it.
    @Test(arguments: [CommandCode.getStreams, .getTopics, .getConsumerGroups, .getUsers])
    func givenANextThatRepeatsTheCursorSentWhenReadingTheWholeListShouldRefuseAsMalformed(_ command: CommandCode) async throws {
        let cursor = try #require(PageCursor.id(1))
        let repeated = FakeVSRServer.pageBody(next: cursor.bytes)
        let server = try await scriptedServer(command, [repeated, repeated, FakeVSRServer.pageBody()])
        defer { Task { await server.stop() } }
        let client = try await connectedClient(server)

        await expectCode(.invalidCommand) { _ = try await wholeList(client, command) }
        let sent = await server.requests.filter { $0.code == command.rawValue }.map(\.payload)
        let expected = try [nil, cursor].map { try request(command, PageQuery(limit: PageQuery.maxLimit, cursor: $0)) }
        #expect(sent == expected)
        await client.shutdown()
    }

    /// The caller signs in while the first page is in flight. The walk keeps
    /// the sign-in it started with, so the next page is not sent under the
    /// new one. The second reply is a last page, so a walk that sends it
    /// returns a list instead of failing.
    @Test(arguments: [CommandCode.getStreams, .getTopics, .getConsumerGroups, .getUsers])
    func givenACallerSignInDuringTheFirstPageWhenReadingTheWholeListShouldRefuseTheNextPageAsUnauthenticated(_ command: CommandCode) async throws {
        let cursor = try #require(PageCursor.id(1))
        let released = Locked(false)
        let server = try await scriptedServer(command, [FakeVSRServer.pageBody(next: cursor.bytes), FakeVSRServer.pageBody()]) {
            _ = await eventually { released.withLock { $0 } }
        }
        defer { Task { await server.stop() } }
        let client = try await connectedClient(server)
        let lineage = await client.operationLineage

        let walk = Task { try await wholeList(client, command) }
        #expect(await eventually { await pageRequests(server, command) == 1 })
        let signIn = Task { try await client.login(username: "iggy", password: "iggy") }
        #expect(await eventually { await client.operationLineage != lineage })
        released.withLock { $0 = true }

        await expectCode(.unauthenticated) { _ = try await walk.value }
        _ = try await signIn.value
        #expect(await pageRequests(server, command) == 1)
        await client.shutdown()
    }

    /// One request timeout covers the whole walk, so the second page fails
    /// although each page alone answers within a timeout. The second reply
    /// is a last page, so a walk with a timeout per page returns a list.
    /// Every whole list shares this walk, so one list keeps the suite short.
    @Test(arguments: [CommandCode.getStreams])
    func givenPagesThatTogetherOutlastTheRequestTimeoutWhenReadingTheWholeListShouldFailAsNotAccepted(_ command: CommandCode) async throws {
        let cursor = try #require(PageCursor.id(1))
        let server = try await scriptedServer(command, [FakeVSRServer.pageBody(next: cursor.bytes), FakeVSRServer.pageBody()]) {
            try? await Task.sleep(for: Self.pageDelay)
        }
        defer { Task { await server.stop() } }
        let client = try await connectedClient(server, requestTimeout: Self.requestTimeout)

        await expectCode(.transientNotAccepted) { _ = try await wholeList(client, command) }
        #expect(await pageRequests(server, command) == 2)
        await client.shutdown()
    }

    // MARK: Helpers

    /// A server that answers the requests of `command` with `replies` in
    /// turn, each once `beforeReply` returns, and with the last reply once
    /// they run out.
    func scriptedServer(
        _ command: CommandCode, _ replies: [[UInt8]], beforeReply: @escaping @Sendable () async -> Void = {}
    ) async throws -> FakeVSRServer {
        let served = Locked(0)
        let server = try await FakeVSRServer.started()
        await server.respond { request in
            guard request.code == command.rawValue else {
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
            let index = served.withLock { count in
                defer { count += 1 }
                return count
            }
            await beforeReply()
            return .ok(replies[min(index, replies.count - 1)])
        }
        return server
    }

    func pageRequests(_ server: FakeVSRServer, _ command: CommandCode) async -> Int {
        await server.requests.filter { $0.code == command.rawValue }.count
    }

    /// `item` with its leading id replaced by `id`.
    func entry(_ item: [UInt8], id: UInt32) -> [UInt8] {
        let idBytes = id.littleEndianBytes
        var entry = item
        entry.replaceSubrange(0..<idBytes.count, with: idBytes)
        return entry
    }

    func request(_ command: CommandCode, _ query: PageQuery) throws -> [UInt8] {
        switch command {
        case .getTopics:
            return try Requests.listPage(Self.streamID, query: query)
        case .getConsumerGroups:
            return try Requests.listPage(Self.streamID, Self.topicID, query: query)
        default:
            return try Requests.listPage(query: query)
        }
    }

    func wholeList(_ client: IggyClient, _ command: CommandCode) async throws -> [UInt32] {
        switch command {
        case .getStreams:
            return try await client.getStreams().map(\.id)
        case .getTopics:
            return try await client.getTopics(streamID: Self.streamID).map(\.id)
        case .getConsumerGroups:
            return try await client.getConsumerGroups(streamID: Self.streamID, topicID: Self.topicID).map(\.id)
        case .getUsers:
            return try await client.getUsers().map(\.id)
        default:
            Issue.record("\(command) has no whole-list method")
            return []
        }
    }
}

private let listCommands: [String: CommandCode] = [
    "get_users": .getUsers, "get_streams": .getStreams, "get_topics": .getTopics, "get_partitions": .getPartitions, "get_consumer_groups": .getConsumerGroups,
]

private let pageOrders: [String: PageOrder] = ["id": .id, "created": .created]

/// A client of `server`, connected and signed in.
private func connectedClient(_ server: FakeVSRServer, requestTimeout: Duration = .seconds(30)) async throws -> IggyClient {
    var configuration = ClientConfiguration(address: server.address, autoLogin: .usernamePassword(username: "iggy", password: "iggy"))
    configuration.connectTimeout = .seconds(2)
    configuration.requestTimeout = requestTimeout
    configuration.heartbeatInterval = .seconds(60)
    let client = IggyClient(configuration: configuration, connector: NIOConnector(), logger: testLogger)
    try await client.connect()
    return client
}

private func namedPageQuery(_ input: ListPagesFixture.Input) throws -> NamedPageQuery {
    let page = PageQuery(
        order: try #require(pageOrders[input.order], "\(input.order)"), descending: input.descending, limit: input.limit,
        cursor: try input.cursor.map { try PageCursor(text: $0) })
    return NamedPageQuery(page: page, name: input.name)
}

/// Reads one page with the page method of `code` and returns the ids of its
/// items. The partitions page takes only the ``PageQuery`` of `query`, so its
/// query must have no name. Only the lists of topics, partitions and consumer
/// groups need a parent.
private func readPage(
    _ client: IggyClient, _ code: CommandCode, query: NamedPageQuery, streamID: Identifier? = nil, topicID: Identifier? = nil
) async throws -> [UInt32] {
    switch code {
    case .getStreams:
        return try await client.getStreamsPage(query).items.map(\.id)
    case .getTopics:
        return try await client.getTopicsPage(streamID: try #require(streamID), query: query).items.map(\.id)
    case .getPartitions:
        try #require(query.name == nil, "a partitions query has no name")
        return try await client.getPartitionsPage(streamID: try #require(streamID), topicID: try #require(topicID), query: query.page).items.map(\.id)
    case .getConsumerGroups:
        return try await client.getConsumerGroupsPage(streamID: try #require(streamID), topicID: try #require(topicID), query: query).items.map(\.id)
    case .getUsers:
        return try await client.getUsersPage(query).items.map(\.id)
    default:
        Issue.record("\(code) is not a list command")
        return []
    }
}

/// The next text and the items of a reply, in the form of the fixture.
private func decodePage(_ command: String, _ body: [UInt8]) throws -> (next: String?, items: [ListPagesFixture.Item]) {
    func view<Item: Sendable & Hashable>(_ page: Page<Item>, _ item: (Item) -> ListPagesFixture.Item) -> (next: String?, items: [ListPagesFixture.Item]) {
        (page.next?.description, page.items.map(item))
    }
    switch command {
    case "get_streams":
        return view(try Responses.page(body[...], item: Responses.stream(from:))) { .init(id: $0.id, name: $0.name) }
    case "get_topics":
        return view(try Responses.page(body[...], item: Responses.topic(from:))) { .init(id: $0.id, name: $0.name) }
    case "get_partitions":
        return view(try Responses.page(body[...], item: Responses.partition(from:))) { .init(id: $0.id, name: nil) }
    case "get_consumer_groups":
        return view(try Responses.page(body[...], item: Responses.consumerGroup(from:))) { .init(id: $0.id, name: $0.name) }
    case "get_users":
        return view(try Responses.page(body[...], item: Responses.user(from:))) { .init(id: $0.id, name: $0.username) }
    default:
        Issue.record("unknown command \(command)")
        return (nil, [])
    }
}
