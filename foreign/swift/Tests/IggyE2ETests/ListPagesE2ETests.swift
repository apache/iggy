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

import Iggy
import Testing

/// List pages against a live server. Other suites run on the same server, so
/// every check filters by the names of this test and walks until the last
/// page instead of counting pages.
@Suite("List pages", requiresServer)
struct ListPagesE2ETests {
    /// Cursor texts of `core/common/fixtures/list_pages.json`. Both texts are
    /// valid, so only the server can refuse them.
    static let idSevenCursor = "pg1_AQcAAAAAAAAA"
    static let unknownKindCursor = "pg1_AwcAAAAAAAAA"

    @Test func givenThreeStreamsWhenReadingPagesOfTwoShouldReturnEachOnceInTheOrderAsked() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let prefix = E2EEnvironment.uniqueName("swift-pages")
            var created: [StreamDetails] = []
            for suffix in ["a", "b", "c"] {
                let name = "\(prefix)-\(suffix)"
                await session.track(stream: try Identifier(named: name))
                created.append(try await client.createStream(name: name))
            }
            // A create takes the lowest free id, so a delete of another test
            // can give a later stream a lower id.
            let ascending = created.map(\.id).sorted()
            let byCreation = created.sorted { ($0.createdAt, $0.id) < ($1.createdAt, $1.id) }.map(\.id)
            let walks: [(order: PageOrder, descending: Bool, expected: [UInt32])] = [
                (.id, false, ascending), (.id, true, ascending.reversed()), (.created, false, byCreation),
            ]
            for walk in walks {
                let query = PageQuery(order: walk.order, descending: walk.descending, limit: 2)
                let streams = try await Self.walk(query) { try await client.getStreamsPage(NamedPageQuery(page: $0, name: prefix)) }
                #expect(streams.map(\.id) == walk.expected, "\(walk.order), descending \(walk.descending)")
            }
            let listed = Set(try await client.getStreams().map(\.id))
            #expect(Set(created.map(\.id)).isSubset(of: listed))
        }
    }

    @Test func givenTopicWithThreePartitionsWhenReadingPagesOfTwoShouldReturnEachPartitionOnce() async throws {
        try await E2EEnvironment.withSession { session in
            let scratch = try await session.scratchTopic(partitions: 3)
            let partitions = try await Self.walk(PageQuery(limit: 2)) {
                try await session.client.getPartitionsPage(streamID: scratch.stream, topicID: scratch.topic, query: $0)
            }
            #expect(partitions.map(\.id) == [0, 1, 2])
        }
    }

    @Test func givenTopicGroupAndUserWhenFilteringByNameShouldReturnOnlyTheMatch() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            let topic = try await client.createTopic(streamID: scratch.stream, name: "sensor-readings", options: TopicCreateOptions(partitionsCount: 1))
            let group = try await client.createConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, name: "billing")
            _ = try await client.createConsumerGroup(streamID: scratch.stream, topicID: scratch.topic, name: "audit")

            let topics = try await Self.walk(PageQuery()) {
                try await client.getTopicsPage(streamID: scratch.stream, query: NamedPageQuery(page: $0, name: "sensor"))
            }
            #expect(topics.map(\.id) == [topic.id])
            let groups = try await Self.walk(PageQuery()) {
                try await client.getConsumerGroupsPage(streamID: scratch.stream, topicID: scratch.topic, query: NamedPageQuery(page: $0, name: "bill"))
            }
            #expect(groups.map(\.id) == [group.id])

            let username = String(E2EEnvironment.uniqueName("swift-pages-user").prefix(50))
            let user = try await client.createUser(username: username, password: "pages-secret")
            do {
                let users = try await Self.walk(PageQuery()) { try await client.getUsersPage(NamedPageQuery(page: $0, name: username)) }
                #expect(users.map(\.id) == [user.id])
            } catch {
                try? await client.deleteUser(Identifier(numeric: user.id))
                throw error
            }
            try await client.deleteUser(Identifier(numeric: user.id))
        }
    }

    @Test func givenPagesTheServerCannotServeWhenReadingShouldRefuseThemAsInvalidCommand() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let scratch = try await session.scratchTopic(partitions: 1)
            let unknownKind = try PageCursor(text: Self.unknownKindCursor)
            await expectIggyError(.invalidCommand) { _ = try await client.getStreamsPage(NamedPageQuery(page: PageQuery(cursor: unknownKind))) }
            let idCursor = try PageCursor(text: Self.idSevenCursor)
            await expectIggyError(.invalidCommand) { _ = try await client.getStreamsPage(NamedPageQuery(page: PageQuery(order: .created, cursor: idCursor))) }
            // The refusals keep the session.
            #expect(try await client.getPartitionsPage(streamID: scratch.stream, topicID: scratch.topic).items.map(\.id) == [0])
            #expect(await client.state == .authenticated)
        }
    }

    /// Reads pages from `first` until a page has no next cursor. No page holds
    /// more items than the limit of the query.
    static func walk<Item: Sendable & Hashable>(_ first: PageQuery, _ readPage: (PageQuery) async throws -> Page<Item>) async throws -> [Item] {
        var query = first
        var items: [Item] = []
        while true {
            let page = try await readPage(query)
            #expect(page.items.count <= Int(query.limit))
            items += page.items
            guard let next = page.next else {
                return items
            }
            query.cursor = next
        }
    }
}
