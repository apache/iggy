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

/// `metadata_changes.feature`, on top of the stream and topic steps of
/// ``ResourceOptionsSteps``.
enum MetadataChangesSteps {
    /// A two phase delete commits its record after the call returns.
    static let followAttempts = 100
    static let followInterval: Duration = .milliseconds(100)
    static let rootUserID: UInt32 = 0

    static func register(into registry: inout StepRegistry) {
        registry.step("I hold the latest metadata change cursor for the groups \"([^\"]+)\"") { world, groups in
            world.changeGroups = try parseGroups(groups[0])
            let changes = try await read(world, from: .latest)
            guard changes.status == .ok, let next = changes.next else {
                throw StepFailure("latest answered \(changes.status) with cursor \(changes.next.map { "\($0)" } ?? "none")")
            }
            world.changeCursor = next
        }
        registry.step("I delete the stream") { world, _ in
            let streamID = try world.requireStream()
            try await world.requireClient().deleteStream(streamID)
            world.createdStreams.removeAll { $0 == world.lastStreamID }
        }
        registry.step("I create and delete (\\d+) streams named \"([^\"]+)\"") { world, groups in
            let client = try world.requireClient()
            for index in 0..<(try number(groups[0], as: Int.self)) {
                let stream = try await client.createStream(name: "\(groups[1])-\(index)")
                try await client.deleteStream(Identifier(numeric: stream.id))
            }
        }
        registry.step("following the metadata changes should return the kinds \"([^\"]+)\" for the stream") { world, groups in
            try await followKinds(world, groups[0])
        }
        registry.step("each followed change should name the root user as its actor") { world, _ in
            for record in world.followedChanges {
                try check(record.actorUserID == rootUserID, "op \(record.op) names actor \(record.actorUserID.map { "\($0)" } ?? "none"), want the root user")
            }
        }
        registry.step("the followed \"([^\"]+)\" change should carry a requested op before its own op") { world, groups in
            let record = try followedRecord(world, groups[0])
            guard case .streamDeleted(let deleted) = record.change else {
                throw StepFailure("expected a stream deletion, got \(record.change)")
            }
            guard let requestedOp = deleted.requestedOp, requestedOp < record.op else {
                throw StepFailure("requested op \(deleted.requestedOp.map { "\($0)" } ?? "none") is not before op \(record.op)")
            }
        }
        registry.step("the followed \"([^\"]+)\" change should report encryption key source \"([^\"]+)\"") { world, groups in
            let source = try followedStreamOptions(world, groups[0])[StreamOptionKey.encryptionKeySource.rawValue]?.value.stringValue
            try check(source == groups[1], "encryption key source is \(source ?? "unreported"), want \(groups[1])")
        }
        registry.step("the followed \"([^\"]+)\" change should carry the stream key fingerprint") { world, groups in
            let fingerprint = try followedStreamOptions(world, groups[0])[StreamOptionKey.encryptionKeyFingerprint.rawValue]?.value.stringValue
            let expected = try EncryptionKey.fingerprint(try ResourceOptionsSteps.streamKey(world))
            try check(fingerprint == expected, "encryption key fingerprint is \(fingerprint ?? "unreported"), want \(expected)")
        }
        registry.step("the followed changes should expose no encryption key material") { world, _ in
            for record in world.followedChanges {
                guard let stream = streamChange(record.change) else {
                    continue
                }
                guard case .readable(let options) = stream.options else {
                    throw StepFailure("op \(record.op) options must stay readable")
                }
                try ResourceOptionsSteps.checkNoKeyMaterial(options, key: world.streamKey, label: "op \(record.op)")
            }
        }
        registry.step("following the metadata changes should answer \"([^\"]+)\"") { world, groups in
            let changes = try await read(world, from: .after(try cursor(world)))
            try check(changes.status.description == groups[0], "follow answered \(changes.status), want \(groups[0])")
            try check(
                changes.status == .ok || (changes.records.isEmpty && changes.next == nil),
                "a \(changes.status) answer carries \(changes.records.count) records and a cursor")
        }
        registry.step("I read the current state for the groups \"([^\"]+)\"") { world, groups in
            world.changeGroups = try parseGroups(groups[0])
            world.currentItems = []
            var token: MetadataCurrentPageToken?
            while true {
                let changes = try await read(world, from: .current(token))
                try check(changes.status == .ok, "current state answered \(changes.status)")
                world.currentItems += changes.items
                guard let next = changes.currentPageToken else {
                    guard let anchor = changes.next else {
                        throw StepFailure("the last current state page returned no anchor")
                    }
                    world.changeCursor = anchor
                    return
                }
                token = next
            }
        }
        registry.step("the current state should list the same streams as the stream list") { world, _ in
            let listed = Dictionary(uniqueKeysWithValues: try await world.requireClient().getStreams().map { ($0.id, $0.name) })
            var current: [UInt32: String] = [:]
            for case .stream(let stream) in world.currentItems {
                current[stream.streamID] = stream.name
            }
            try check(current == listed, "current state lists \(current), the stream list \(listed)")
        }
        registry.step("a user named \"([^\"]+)\" with password \"([^\"]+)\" and no server permissions exists") { world, groups in
            let permissions = Permissions(global: GlobalPermissions(readServers: true))
            try await ResourceOptionsSteps.createUser(world, username: groups[0], password: groups[1], permissions: permissions)
        }
        registry.step("I authenticate as \"([^\"]+)\" with password \"([^\"]+)\"") { world, groups in
            let client = try world.requireClient()
            try await client.logout()
            world.signedInAsAnotherUser = true
            _ = try await client.login(username: groups[0], password: groups[1])
        }
        registry.step("reading the metadata changes should be refused as unauthorized") { world, _ in
            world.changeGroups = []
            await world.capture { _ = try await read(world, from: .earliest) }
            try world.expectFailure(.unauthorized)
        }
    }

    /// Follows the log after the held cursor until the scenario's stream has
    /// as many records as `kinds` names, then compares their kinds in order.
    private static func followKinds(_ world: World, _ kinds: String) async throws {
        let want = try kinds.split(separator: ",").map { name in
            let trimmed = name.trimmingCharacters(in: .whitespaces)
            guard let kind = MetadataChangeKind.allCases.first(where: { $0.description == trimmed }) else {
                throw StepFailure("unknown change kind \(trimmed)")
            }
            return kind
        }
        let key = try await createdStreamKey(world)
        var position = try cursor(world)
        var records: [MetadataChangeRecord] = []
        var mine: [MetadataChangeRecord] = []
        for _ in 0..<followAttempts {
            let changes = try await read(world, from: .after(position))
            guard changes.status == .ok, let next = changes.next else {
                throw StepFailure("follow answered \(changes.status) with cursor \(changes.next.map { "\($0)" } ?? "none")")
            }
            position = next
            records += changes.records
            if changes.hasMore {
                continue
            }
            mine = records.filter { record in streamKey(record.change).map { $0 == key } ?? false }
            if mine.count >= want.count {
                break
            }
            try await Task.sleep(for: followInterval)
        }
        for record in records where !world.changeGroups.isEmpty {
            try check(
                record.change.group.map(world.changeGroups.contains) == true,
                "op \(record.op) of group \(record.change.group.map { "\($0)" } ?? "none") is outside the requested \(world.changeGroups)")
        }
        let got = mine.map(\.change.kind)
        try check(got == want, "followed kinds \(got), want \(want)")
        world.followedChanges = mine
    }

    /// The stream id and creating op of the scenario's stream, read from its
    /// creation record. Ids are reused after a delete, so an id alone can
    /// match a stream another scenario created and deleted meanwhile.
    private static func createdStreamKey(_ world: World) async throws -> StreamKey {
        guard let streamID = world.lastStreamID, let name = world.lastStreamName else {
            throw StepFailure("no stream was created in this scenario")
        }
        var query = MetadataChangesQuery(start: .after(try cursor(world)), groups: [.streams], limit: MetadataChangesQuery.maxLimit)
        while true {
            let changes = try await world.requireClient().getMetadataChanges(query)
            guard changes.status == .ok, let next = changes.next else {
                throw StepFailure("stream changes answered \(changes.status) with cursor \(changes.next.map { "\($0)" } ?? "none")")
            }
            for record in changes.records {
                if case .streamCreated(let created) = record.change, created.streamID == streamID, created.name == name {
                    return StreamKey(streamID: created.streamID, createdOp: created.streamCreatedOp)
                }
            }
            guard changes.hasMore else {
                throw StepFailure("no stream_created change for stream \(streamID) \(name)")
            }
            query.start = .after(next)
        }
    }

    struct StreamKey: Equatable {
        let streamID: UInt32
        let createdOp: UInt64
    }

    /// The stream a stream, topic or partitions record belongs to.
    private static func streamKey(_ change: MetadataChange) -> StreamKey? {
        switch change {
        case .streamCreated(let body), .streamUpdated(let body): StreamKey(streamID: body.streamID, createdOp: body.streamCreatedOp)
        case .streamDeleted(let body): StreamKey(streamID: body.streamID, createdOp: body.streamCreatedOp)
        case .topicCreated(let body), .topicUpdated(let body): StreamKey(streamID: body.streamID, createdOp: body.streamCreatedOp)
        case .topicDeleted(let body): StreamKey(streamID: body.streamID, createdOp: body.streamCreatedOp)
        case .partitionsChanged(let body): StreamKey(streamID: body.streamID, createdOp: body.streamCreatedOp)
        default: nil
        }
    }

    private static func streamChange(_ change: MetadataChange) -> MetadataStreamChange? {
        switch change {
        case .streamCreated(let body), .streamUpdated(let body): body
        default: nil
        }
    }

    private static func followedRecord(_ world: World, _ kind: String) throws -> MetadataChangeRecord {
        guard let record = world.followedChanges.first(where: { $0.change.kind?.description == kind }) else {
            throw StepFailure("no followed \(kind) change")
        }
        return record
    }

    private static func followedStreamOptions(_ world: World, _ kind: String) throws -> ResourceOptions {
        let record = try followedRecord(world, kind)
        guard let stream = streamChange(record.change) else {
            throw StepFailure("expected a stream change, got \(record.change)")
        }
        guard case .readable(let options) = stream.options else {
            throw StepFailure("stream options must stay readable")
        }
        return options
    }

    private static func read(_ world: World, from start: MetadataChangeStart) async throws -> MetadataChanges {
        try await world.requireClient().getMetadataChanges(
            MetadataChangesQuery(start: start, groups: world.changeGroups, limit: MetadataChangesQuery.maxLimit))
    }

    private static func cursor(_ world: World) throws -> MetadataChangeCursor {
        guard let cursor = world.changeCursor else {
            throw StepFailure("no metadata change cursor is held")
        }
        return cursor
    }

    private static func parseGroups(_ names: String) throws -> [MetadataChangeGroup] {
        try names.split(separator: ",").map { name in
            let trimmed = name.trimmingCharacters(in: .whitespaces)
            guard let group = MetadataChangeGroup.allCases.first(where: { $0.description == trimmed }) else {
                throw StepFailure("unknown change group \(trimmed)")
            }
            return group
        }
    }
}
