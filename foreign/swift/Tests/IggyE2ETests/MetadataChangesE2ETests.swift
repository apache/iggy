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
import Testing

/// The shared metadata changes scenarios of `bdd/scenarios/metadata_changes.feature`.
/// Other suites run alongside, so every check names the streams this test
/// created rather than the whole log. The keyed scenario needs a server
/// encryption key and lives with the resource options suite below.
@Suite("Metadata changes", requiresServer)
struct MetadataChangesE2ETests {
    static let rootUserID: UInt32 = 0
    static let streamGroups: [MetadataChangeGroup] = [.streams]

    @Test func givenLatestCursorWhenCreatingRenamingAndDeletingStreamShouldFollowEachChangeWithRootActor() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let cursor = try await Self.latestCursor(client, groups: Self.streamGroups)
            let name = E2EEnvironment.uniqueName("swift-changes-follow")
            let renamed = name + "-renamed"
            await session.track(stream: try Identifier(named: renamed))
            let stream = try await client.createStream(name: name)
            try await client.updateStream(Identifier(numeric: stream.id), options: StreamUpdateOptions(name: renamed))
            try await client.deleteStream(Identifier(numeric: stream.id))

            let records = try await Self.follow(client, after: cursor, groups: Self.streamGroups, count: 3, first: Self.creates(stream: name))
            #expect(records.map(\.change.kind) == [.streamCreated, .streamUpdated, .streamDeleted])
            #expect(records.allSatisfy { $0.actorUserID == Self.rootUserID })
            #expect(records.map(\.op) == records.map(\.op).sorted())
            guard records.count == 3, case .streamCreated(let created) = records[0].change, case .streamUpdated(let updated) = records[1].change,
                case .streamDeleted(let deleted) = records[2].change
            else {
                Issue.record("unexpected records \(records)")
                return
            }
            #expect(created.name == name)
            #expect(updated.name == renamed)
            #expect(deleted.name == renamed)
            #expect(deleted.streamCreatedOp == records[0].op)
            #expect(deleted.requestedOp == nil)
        }
    }

    @Test func givenTopicsGroupWhenCreatingStreamAndTopicShouldFollowOnlyTheTopic() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let groups: [MetadataChangeGroup] = [.topics]
            let cursor = try await Self.latestCursor(client, groups: groups)
            let name = E2EEnvironment.uniqueName("swift-changes-filter")
            let stream = try await client.createStream(name: name)
            await session.track(stream: try Identifier(named: name))
            let topicName = E2EEnvironment.uniqueName("swift-changes-events")
            _ = try await client.createTopic(streamID: Identifier(numeric: stream.id), name: topicName, options: TopicCreateOptions(partitionsCount: 1))

            let records = try await Self.follow(client, after: cursor, groups: groups, count: 1) { record in
                if case .topicCreated(let topic) = record.change { topic.name == topicName } else { false }
            }
            #expect(records.map(\.change.kind) == [.topicCreated])
            guard case .topicCreated(let topic) = records.first?.change else {
                Issue.record("expected a topic record, got \(records)")
                return
            }
            #expect(topic.streamID == stream.id)
            #expect(topic.partitionsCount == 1)
            #expect(topic.topicCreatedOp > topic.streamCreatedOp)
        }
    }

    @Test func givenStreamWithTopicWhenDeletingShouldRecordTheTwoPhaseDeleteAtItsCompletion() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let cursor = try await Self.latestCursor(client, groups: Self.streamGroups)
            let name = E2EEnvironment.uniqueName("swift-changes-two-phase")
            await session.track(stream: try Identifier(named: name))
            let stream = try await client.createStream(name: name)
            _ = try await client.createTopic(streamID: Identifier(numeric: stream.id), name: "events", options: TopicCreateOptions(partitionsCount: 1))
            try await client.deleteStream(Identifier(numeric: stream.id))

            let records = try await Self.follow(client, after: cursor, groups: Self.streamGroups, count: 2, first: Self.creates(stream: name))
            #expect(records.map(\.change.kind) == [.streamCreated, .streamDeleted])
            guard let last = records.last, case .streamDeleted(let deleted) = last.change else {
                Issue.record("expected a stream deletion, got \(records)")
                return
            }
            let requestedOp = try #require(deleted.requestedOp)
            #expect(requestedOp < last.op)
        }
    }

    @Test func givenLimitOfOneWhenFollowingShouldPageOneRecordAtATimeInOrder() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let cursor = try await Self.latestCursor(client, groups: Self.streamGroups)
            var names = Set<String>()
            for index in 0..<3 {
                let name = E2EEnvironment.uniqueName("swift-changes-page-\(index)")
                _ = try await client.createStream(name: name)
                names.insert(name)
                await session.track(stream: try Identifier(named: name))
            }
            // Other tests create streams under reused ids at the same time, so
            // only these names count.
            func createdHere(_ record: MetadataChangeRecord) -> Bool {
                guard case .streamCreated(let stream) = record.change else {
                    return false
                }
                return names.contains(stream.name)
            }

            var paged: [UInt64] = []
            var position = cursor
            while true {
                let page = try await client.getMetadataChanges(MetadataChangesQuery(start: .after(position), groups: Self.streamGroups, limit: 1))
                #expect(page.status == .ok)
                #expect(page.records.count <= 1)
                paged += page.records.filter(createdHere).map(\.op)
                position = try #require(page.next)
                if !page.hasMore {
                    break
                }
            }
            let whole = try await client.getMetadataChanges(MetadataChangesQuery(start: .after(cursor), groups: Self.streamGroups))
            let expected = whole.records.filter(createdHere).map(\.op)
            #expect(expected.count == names.count)
            #expect(paged == expected)
        }
    }

    @Test func givenStreamsWhenPagingCurrentStateShouldListThemAndAnchorTheChangeLog() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            var created: [UInt32: String] = [:]
            for index in 0..<2 {
                let name = E2EEnvironment.uniqueName("swift-changes-current-\(index)")
                created[try await client.createStream(name: name).id] = name
                await session.track(stream: try Identifier(named: name))
            }
            let (items, anchor) = try await Self.currentState(client, groups: Self.streamGroups, limit: 1)
            var projected: [UInt32: String] = [:]
            for item in items {
                guard case .stream(let stream) = item else {
                    Issue.record("unexpected item \(item)")
                    continue
                }
                projected[stream.streamID] = stream.name
            }
            for (id, name) in created {
                #expect(projected[id] == name)
            }
            let listed = Set(try await client.getStreams().map(\.id))
            #expect(Set(created.keys).isSubset(of: listed))
            let follow = try await client.getMetadataChanges(MetadataChangesQuery(start: .after(anchor), groups: Self.streamGroups))
            #expect(follow.status == .ok)
        }
    }

    @Test func givenCursorOfAnotherHistoryWhenFollowingShouldAnswerResetThenRebuildFromCurrentState() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let cursor = try await Self.latestCursor(client, groups: Self.streamGroups)
            let name = E2EEnvironment.uniqueName("swift-changes-reset")
            let stream = try await client.createStream(name: name)
            await session.track(stream: try Identifier(named: name))

            var foreign = MetadataChangeCursor.zero(logID: cursor.logID)
            foreign.logID.low &+= 1
            let page = try await client.getMetadataChanges(MetadataChangesQuery(start: .after(foreign), groups: Self.streamGroups))
            #expect(page.status == .reset)
            #expect(page.records.isEmpty)
            #expect(page.next == nil)

            let (items, anchor) = try await Self.currentState(client, groups: Self.streamGroups, limit: MetadataChangesQuery.maxLimit)
            #expect(
                items.contains { item in
                    if case .stream(let projected) = item { projected.streamID == stream.id && projected.name == name } else { false }
                })
            let follow = try await client.getMetadataChanges(MetadataChangesQuery(start: .after(anchor), groups: Self.streamGroups))
            #expect(follow.status == .ok)
        }
    }

    /// Runs where the server's resource ring is small enough to churn past,
    /// named by the same variable the server reads.
    @Test(.enabled(if: MetadataChangesE2ETests.resourceCapacity != nil, "set IGGY_METADATA_CHANGE_LOG_RESOURCE_CAPACITY to the server's value to run"))
    func givenChurnBeyondCapacityWhenFollowingShouldAnswerExpiredThenRebuildFromCurrentState() async throws {
        let capacity = try #require(Self.resourceCapacity)
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let cursor = try await Self.latestCursor(client, groups: Self.streamGroups)
            let prefix = E2EEnvironment.uniqueName("swift-changes-churn")
            for index in 0..<(capacity / 2 + 8) {
                let stream = try await client.createStream(name: "\(prefix)-\(index)")
                try await client.deleteStream(Identifier(numeric: stream.id))
            }
            let expired = try await client.getMetadataChanges(MetadataChangesQuery(start: .after(cursor), groups: Self.streamGroups))
            #expect(expired.status == .expired)
            #expect(expired.records.isEmpty)
            #expect(expired.next == nil)

            let (_, anchor) = try await Self.currentState(client, groups: Self.streamGroups, limit: MetadataChangesQuery.maxLimit)
            let follow = try await client.getMetadataChanges(MetadataChangesQuery(start: .after(anchor), groups: Self.streamGroups))
            #expect(follow.status == .ok)
        }
    }

    @Test func givenInvalidQueriesWhenReadingShouldRefuseEachWithInvalidCommand() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let queries = [
                MetadataChangesQuery(limit: 0),
                MetadataChangesQuery(limit: MetadataChangesQuery.maxLimit + 1),
                MetadataChangesQuery(since: IggyTimestamp(microseconds: 2), until: IggyTimestamp(microseconds: 1)),
                MetadataChangesQuery(start: .latest, since: IggyTimestamp(microseconds: 1)),
                MetadataChangesQuery(start: .current(nil), until: IggyTimestamp(microseconds: 1)),
                MetadataChangesQuery(start: .current(nil), groups: [.topics]),
            ]
            for query in queries {
                await expectIggyError(.invalidCommand) { _ = try await client.getMetadataChanges(query) }
            }
        }
    }

    @Test func givenUserWithoutManageServersWhenReadingShouldBeRefusedAsUnauthorized() async throws {
        try await E2EEnvironment.withSession { session in
            let root = session.client
            let username = String(E2EEnvironment.uniqueName("swift-changes-reader").prefix(50))
            let user = try await root.createUser(
                username: username, password: "changes-secret", permissions: Permissions(global: GlobalPermissions(readServers: true)))
            let reader = IggyClient(
                configuration: E2EEnvironment.configuration(autoLogin: .usernamePassword(username: username, password: "changes-secret")))
            do {
                try await reader.connect()
                await expectIggyError(.unauthorized) { _ = try await reader.getMetadataChanges() }
            } catch {
                await reader.shutdown()
                try? await root.deleteUser(Identifier(numeric: user.id))
                throw error
            }
            await reader.shutdown()
            try await root.deleteUser(Identifier(numeric: user.id))
        }
    }

    static var resourceCapacity: Int? {
        ProcessInfo.processInfo.environment["IGGY_METADATA_CHANGE_LOG_RESOURCE_CAPACITY"].flatMap(Int.init)
    }

    static func latestCursor(_ client: IggyClient, groups: [MetadataChangeGroup]) async throws -> MetadataChangeCursor {
        let page = try await client.getMetadataChanges(MetadataChangesQuery(start: .latest, groups: groups))
        #expect(page.status == .ok)
        #expect(page.records.isEmpty)
        #expect(page.logID != nil)
        return try #require(page.next)
    }

    /// Follows the change log after `cursor` until `count` records name the
    /// stream whose first record matches `first`. Other tests create and
    /// delete streams under reused ids at the same time, so the stream is told
    /// apart by its creating op. A two phase delete commits its record after
    /// the call returns, so an incomplete read polls again.
    static func follow(
        _ client: IggyClient, after cursor: MetadataChangeCursor, groups: [MetadataChangeGroup], count: Int,
        first: (MetadataChangeRecord) -> Bool
    ) async throws -> [MetadataChangeRecord] {
        var position = cursor
        var mine: [MetadataChangeRecord] = []
        var incarnation: (id: UInt32, createdOp: UInt64)?
        let found = try await E2EEnvironment.eventually {
            while true {
                let page = try await client.getMetadataChanges(
                    MetadataChangesQuery(start: .after(position), groups: groups, limit: MetadataChangesQuery.maxLimit))
                #expect(page.status == .ok)
                position = try #require(page.next)
                for record in page.records {
                    #expect(record.change.group.map(groups.contains) == true, "record of op \(record.op) is outside the requested groups")
                    guard let stream = Self.stream(of: record.change) else {
                        continue
                    }
                    if incarnation == nil, first(record) {
                        incarnation = stream
                    }
                    if let incarnation, stream == incarnation {
                        mine.append(record)
                    }
                }
                if !page.hasMore {
                    return mine.count >= count
                }
            }
        }
        #expect(found, "\(mine.count) of \(count) records")
        return mine
    }

    /// Whether `record` created the stream named `name`.
    static func creates(stream name: String) -> (MetadataChangeRecord) -> Bool {
        { record in
            if case .streamCreated(let stream) = record.change { stream.name == name } else { false }
        }
    }

    /// Pages the current state of `groups` and returns its items with the
    /// anchor to follow the change log from.
    static func currentState(_ client: IggyClient, groups: [MetadataChangeGroup], limit: UInt32) async throws -> ([MetadataCurrentItem], MetadataChangeCursor) {
        var query = MetadataChangesQuery(start: .current(nil), groups: groups, limit: limit)
        var items: [MetadataCurrentItem] = []
        while true {
            let page = try await client.getMetadataChanges(query)
            #expect(page.status == .ok)
            #expect(page.phase == .current)
            #expect(page.records.isEmpty)
            #expect(page.items.count <= Int(limit))
            items += page.items
            guard let token = page.currentPageToken else {
                return (items, try #require(page.next))
            }
            query.start = .current(token)
        }
    }

    static func stream(of change: MetadataChange) -> (id: UInt32, createdOp: UInt64)? {
        switch change {
        case .streamCreated(let body), .streamUpdated(let body): (body.streamID, body.streamCreatedOp)
        case .streamDeleted(let body): (body.streamID, body.streamCreatedOp)
        case .topicCreated(let body), .topicUpdated(let body): (body.streamID, body.streamCreatedOp)
        case .topicDeleted(let body): (body.streamID, body.streamCreatedOp)
        case .partitionsChanged(let body): (body.streamID, body.streamCreatedOp)
        default: nil
        }
    }
}

/// The keyed scenario needs a server encryption key, so it belongs to the
/// resource options suite the shipped default lane skips.
extension ResourceOptionsE2ETests {
    @Test func givenKeyedStreamWhenFollowingMetadataChangesShouldCarryFingerprintsAndNoKeyMaterial() async throws {
        try await E2EEnvironment.withSession { session in
            let client = session.client
            let groups = MetadataChangesE2ETests.streamGroups
            let cursor = try await MetadataChangesE2ETests.latestCursor(client, groups: groups)
            let key = Self.randomKey()
            let name = E2EEnvironment.uniqueName("swift-changes-keyed")
            _ = try await client.createStream(name: name, options: StreamCreateOptions(encryptionKey: key))
            await session.track(stream: try Identifier(named: name))

            let records = try await MetadataChangesE2ETests.follow(
                client, after: cursor, groups: groups, count: 1, first: MetadataChangesE2ETests.creates(stream: name))
            #expect(records.map(\.change.kind) == [.streamCreated])
            guard case .streamCreated(let created) = records.first?.change, case .readable(let options) = created.options else {
                Issue.record("expected a readable stream record, got \(records)")
                return
            }
            #expect(options[StreamOptionKey.encryptionKeySource.rawValue]?.value == "stream")
            #expect(options[StreamOptionKey.encryptionKeyFingerprint.rawValue]?.value == (try HeaderValue.string(try EncryptionKey.fingerprint(key))))
            #expect(options[StreamOptionKey.encryptionKekFingerprint.rawValue].map { Self.isFingerprint($0.value.description) } == true)
            Self.expectNoKeyMaterial(options, key: key)
        }
    }
}
