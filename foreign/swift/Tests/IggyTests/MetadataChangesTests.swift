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
import Testing

@testable import Iggy

/// The metadata changes contract every SDK shares, checked against
/// `Fixtures/metadata_changes.json` from the Rust codecs. The Swift encoder
/// and decoders are independent, so these bytes are the contract. Refused
/// request bytes and HTTP queries are left out: this SDK never decodes a
/// request and has no HTTP transport.
@Suite("Metadata changes match the shared fixture")
struct MetadataChangesFixtureTests {
    let fixture = MetadataChangesFixture.shared

    @Test func givenManyDuplicateGroupsWhenEncodingShouldPreserveEveryRequestedGroup() {
        let groups = Array(repeating: MetadataChangeGroup.streams, count: 255) + [.users]
        let query = MetadataChangesQuery(groups: groups)
        let expected = MetadataChangesQuery(groups: [.streams, .users])
        #expect(Requests.getMetadataChanges(query) == Requests.getMetadataChanges(expected))
    }

    @Test func givenFixtureWhenLoadingShouldHoldEveryVectorSection() {
        for section in ["records", "items", "cursors", "page_tokens", "requests", "responses"] {
            #expect(!fixture[section].array.isEmpty, "\(section)")
        }
        for section in ["cursors", "page_tokens", "record_bodies", "responses", "queries"] {
            #expect(!fixture["refused"][section].array.isEmpty, "refused \(section)")
        }
    }

    @Test func givenFixtureConstantsWhenComparingPublicTypesShouldMatchCodesAndNames() throws {
        let constants = fixture["constants"]
        #expect(fixture["schema_version"].number == 1)
        let command = try #require(CommandCode(rawValue: UInt32(constants["command_code"].number)))
        #expect(command == .getMetadataChanges)
        #expect(command.name == constants["command_name"].string)
        #expect(ConsensusOperation.forCode(command.rawValue) == .nonReplicated)

        #expect(Set(constants["groups"].members.keys) == Set(MetadataChangeGroup.allCases.map(\.description)))
        for (name, entry) in constants["groups"].members {
            let group = try #require(MetadataChangeGroup(name: name), "\(name)")
            #expect(UInt64(group.rawValue) == entry["code"].number, "\(name)")
            #expect(group.parents.map(\.description) == entry["parents"].array.map(\.string), "\(name)")
        }
        #expect(Set(constants["kinds"].members.keys) == Set(MetadataChangeKind.allCases.map(\.description)))
        for (name, entry) in constants["kinds"].members {
            let kind = try #require(MetadataChangeKind(name: name), "\(name)")
            #expect(UInt64(kind.rawValue) == entry["code"].number, "\(name)")
            #expect(kind.group.description == entry["group"].string, "\(name)")
        }
        expectCodes(MetadataChangeStatus.self, constants["statuses"])
        expectCodes(MetadataChangePhase.self, constants["phases"])
        expectCodes(MetadataMemberLeftReason.self, constants["member_left_reasons"])
        expectCodes(MetadataTokenDeletedReason.self, constants["token_deleted_reasons"])
        expectCodes(MetadataSessionClosedReason.self, constants["session_closed_reasons"])
        expectCodes(MetadataPermissionsUpdatedReason.self, constants["permissions_updated_reasons"])
        let startModes: [String: MetadataChangeStart] = [
            "earliest": .earliest, "latest": .latest, "after": .after(.zero(logID: 1)), "current": .current(nil),
        ]
        #expect(Set(constants["start_modes"].members.keys) == Set(startModes.keys))
        for (name, start) in startModes {
            #expect(UInt64(start.code) == constants["start_modes"][name].number, "\(name)")
        }

        #expect(MetadataChangeCursor.textPrefix == constants["cursor_prefix"].string)
        #expect(MetadataCurrentPageToken.textPrefix == constants["page_token_prefix"].string)
        #expect(UInt64(MetadataChangeCursor.encodedSize) == constants["cursor_size"].number)
        #expect(UInt64(MetadataChangeCursor.textLength) == constants["cursor_text_length"].number)
        #expect(UInt64(MetadataCurrentPageToken.maxEncodedSize) == constants["max_page_token_size"].number)
        #expect(UInt64(MetadataCurrentPageToken.maxKeySize) == constants["max_page_token_key_size"].number)
        #expect(UInt64(MetadataChangesQuery.defaultLimit) == constants["default_limit"].number)
        #expect(UInt64(MetadataChangesQuery.maxLimit) == constants["max_limit"].number)
        #expect(MetadataChangesQuery().limit == MetadataChangesQuery.defaultLimit)
    }

    @Test func givenRecordVectorsWhenDecodingShouldReachTheExpectedJsonForEveryKind() throws {
        var kinds = Set<MetadataChangeKind>()
        for vector in fixture["records"].array {
            let name = vector["case"].string
            var reader = ByteReader([UInt8](hex: vector["record_hex"].string))
            let record = try Responses.metadataChangeRecord(from: &reader)
            #expect(reader.isAtEnd, "\(name)")
            #expect(json(record) == vector["expected"], "\(name)")
            let kind = try #require(record.change.kind, "\(name)")
            #expect(UInt64(kind.rawValue) == vector["kind"].number, "\(name)")
            #expect(UInt64(kind.group.rawValue) == vector["group"].number, "\(name)")
            let body = try MetadataChange.decode(group: kind.group.rawValue, kind: kind.rawValue, body: [UInt8](hex: vector["body_hex"].string)[...])
            #expect(body == record.change, "\(name)")
            kinds.insert(kind)
        }
        #expect(kinds == Set(MetadataChangeKind.allCases))
    }

    @Test func givenItemVectorsWhenDecodingShouldReachTheExpectedJsonForEveryGroup() throws {
        var groups = Set<MetadataChangeGroup>()
        for vector in fixture["items"].array {
            let name = vector["case"].string
            var reader = ByteReader([UInt8](hex: vector["item_hex"].string))
            let item = try Responses.metadataItem(from: &reader)
            #expect(reader.isAtEnd, "\(name)")
            #expect(json(item) == vector["expected"], "\(name)")
            let group = try #require(item.group, "\(name)")
            #expect(UInt64(group.rawValue) == vector["group"].number, "\(name)")
            #expect(try MetadataCurrentItem.decode(group: group.rawValue, body: [UInt8](hex: vector["body_hex"].string)[...]) == item, "\(name)")
            groups.insert(group)
        }
        #expect(groups == Set(MetadataChangeGroup.allCases))
    }

    @Test func givenResponseVectorsWhenDecodingShouldReachTheExpectedAnswers() throws {
        for vector in fixture["responses"].array {
            let answer = try Responses.metadataChanges([UInt8](hex: vector["response_hex"].string)[...])
            #expect(json(answer) == vector["expected"], "\(vector["case"].string)")
        }
    }

    @Test func givenUnknownKindAndGroupWhenDecodingShouldKeepCodesAndRawBody() throws {
        let vector = try #require(fixture["responses"].array.first { $0["case"].string == "unknown_kind_and_group" })
        let answer = try Responses.metadataChanges([UInt8](hex: vector["response_hex"].string)[...])
        #expect(
            answer.records.map(\.change) == [
                .unknown(group: 1, kind: 9, body: [0xde, 0xad, 0xbe, 0xef]), .unknown(group: 200, kind: 1, body: [0xde, 0xad, 0xbe, 0xef]),
            ])
        #expect(answer.records.allSatisfy { $0.change.kind == nil && $0.change.group == nil })
        #expect(try MetadataCurrentItem.decode(group: 9, body: [1, 2][...]) == .unknown(group: 9, body: [1, 2]))
    }

    @Test func givenLargeValuesWhenDecodingShouldKeepEveryBit() throws {
        let vector = fixture["large_values"]
        var reader = ByteReader([UInt8](hex: vector["record_hex"].string))
        let record = try Responses.metadataChangeRecord(from: &reader)
        #expect(String(record.op) == vector["op"].string)
        #expect(UInt64(record.ordinal) == vector["ordinal"].number)
        #expect(String(record.timestamp.microseconds) == vector["timestamp"].string)
        #expect(hex(record.checksum) == vector["checksum"].string)
        guard case .sessionRegistered(let session) = record.change else {
            Issue.record("expected a session record, got \(record.change)")
            return
        }
        #expect(String(session.userCreatedAt.microseconds) == vector["user_created_at"].string)
        #expect(String(session.session) == vector["session"].string)
        #expect(hex(session.clientID) == vector["client_id"].string)
    }

    @Test func givenCursorVectorsWhenParsingShouldMatchTheirFieldsBytesAndText() throws {
        for vector in fixture["cursors"].array {
            let name = vector["case"].string
            let cursor = try MetadataChangeCursor(text: vector["text"].string)
            #expect(hex(cursor.logID) == vector["log_id"].string, "\(name)")
            #expect(cursor.op == vector["op"].number, "\(name)")
            #expect(hex(cursor.checksum) == vector["checksum"].string, "\(name)")
            #expect(UInt64(cursor.ordinal) == vector["ordinal"].number, "\(name)")
            #expect(cursor.isZero == (cursor.op == 0), "\(name)")
            #expect(cursor.description == vector["text"].string, "\(name)")
            var writer = ByteWriter()
            cursor.encode(into: &writer)
            #expect(writer.bytes == [UInt8](hex: vector["hex"].string), "\(name)")
        }
    }

    @Test func givenPageTokenVectorsWhenParsingShouldMatchTheirFieldsBytesAndText() throws {
        for vector in fixture["page_tokens"].array {
            let name = vector["case"].string
            let token = try MetadataCurrentPageToken(text: vector["text"].string)
            #expect(token.description == vector["text"].string, "\(name)")
            #expect(token.encoded() == [UInt8](hex: vector["hex"].string), "\(name)")
            #expect(token.anchor.description == vector["anchor"].string, "\(name)")
            #expect(token.requiredHead == vector["required_head"].number, "\(name)")
            #expect(UInt64(token.groupsMask) == vector["groups_mask"].number, "\(name)")
            #expect(token.groups == Set(MetadataChangeGroup.allCases.filter { UInt64($0.maskBit) & vector["groups_mask"].number != 0 }), "\(name)")
            #expect(token.lastGroup.description == vector["group"].string, "\(name)")
            #expect(token.lastKey == [UInt8](hex: vector["last_key_hex"].string), "\(name)")
        }
    }

    @Test func givenRequestVectorsWhenEncodingShouldValidateAndMatchTheRequestBytes() throws {
        for vector in fixture["requests"].array {
            let query = try query(vector["input"])
            try query.validate()
            #expect(Requests.getMetadataChanges(query) == [UInt8](hex: vector["request_hex"].string), "\(vector["case"].string)")
        }
    }

    @Test func givenRefusedCursorAndTokenTextWhenParsingShouldRefuseWithoutTheText() {
        for vector in fixture["refused"]["cursors"].array {
            expectRefusedText(vector["text"].string, vector["case"].string) { try MetadataChangeCursor(text: $0) }
        }
        for vector in fixture["refused"]["page_tokens"].array {
            expectRefusedText(vector["text"].string, vector["case"].string) { try MetadataCurrentPageToken(text: $0) }
        }
    }

    @Test func givenRefusedRecordBodiesWhenDecodingShouldRefuseEach() {
        for vector in fixture["refused"]["record_bodies"].array {
            let body = [UInt8](hex: vector["body_hex"].string)
            #expect(throws: (any Error).self, "\(vector["case"].string)") {
                try MetadataChange.decode(group: UInt8(vector["group"].number), kind: UInt8(vector["kind"].number), body: body[...])
            }
        }
    }

    @Test func givenRefusedResponsesWhenDecodingShouldRefuseEach() {
        for vector in fixture["refused"]["responses"].array {
            #expect(throws: IggyError(.invalidCommand), "\(vector["case"].string)") {
                try Responses.metadataChanges([UInt8](hex: vector["hex"].string)[...])
            }
        }
    }

    @Test func givenRefusedQueriesWhenValidatingShouldRefuseWithInvalidCommand() throws {
        for vector in fixture["refused"]["queries"].array {
            let query = try query(vector["input"])
            #expect(throws: IggyError(.invalidCommand), "\(vector["case"].string)") { try query.validate() }
        }
    }

    // MARK: Helpers

    func expectCodes<Code: RawRepresentable & CaseIterable & CustomStringConvertible>(
        _: Code.Type, _ table: JSONValue, sourceLocation: SourceLocation = #_sourceLocation
    ) where Code.RawValue == UInt8 {
        let expected = table.members.mapValues(\.number)
        let actual = Dictionary(uniqueKeysWithValues: Code.allCases.map { ($0.description, UInt64($0.rawValue)) })
        #expect(actual == expected, sourceLocation: sourceLocation)
    }

    func expectRefusedText(_ text: String, _ name: String, _ parse: (String) throws -> some Any) {
        do {
            _ = try parse(text)
            Issue.record("\(name) parsed")
        } catch let error as IggyError {
            #expect(error.code == .invalidCommand, "\(name)")
            #expect(!"\(error)".contains(text), "\(name)")
            #expect(!String(reflecting: error).contains(text), "\(name)")
        } catch {
            Issue.record("\(name): \(error)")
        }
    }

    func query(_ input: JSONValue) throws -> MetadataChangesQuery {
        let start: MetadataChangeStart
        switch input["start"].string {
        case "earliest": start = .earliest
        case "latest": start = .latest
        case "after": start = .after(try MetadataChangeCursor(text: input["cursor"].string))
        default: start = .current(input["page_token"].isNull ? nil : try MetadataCurrentPageToken(text: input["page_token"].string))
        }
        return MetadataChangesQuery(
            start: start, groups: try input["groups"].array.map { try #require(MetadataChangeGroup(name: $0.string)) },
            since: input["since"].isNull ? nil : IggyTimestamp(microseconds: input["since"].number),
            until: input["until"].isNull ? nil : IggyTimestamp(microseconds: input["until"].number), limit: UInt32(input["limit"].number))
    }
}

/// The Swift codecs and the client call on their own: query rules, the text
/// forms, and a round trip through a scripted server.
@Suite("Metadata changes codecs and client", .serialized)
struct MetadataChangesTests {
    let cursor = MetadataChangeCursor(logID: UInt128Value(low: 7, high: 9), op: 4187, checksum: UInt128Value(low: 11, high: 13), ordinal: 1)

    @Test func givenQueryRulesWhenValidatingShouldRefuseOnlyInvalidQueries() throws {
        let refused: [MetadataChangesQuery] = [
            MetadataChangesQuery(limit: 0),
            MetadataChangesQuery(limit: MetadataChangesQuery.maxLimit + 1),
            MetadataChangesQuery(since: IggyTimestamp(microseconds: 5), until: IggyTimestamp(microseconds: 4)),
            MetadataChangesQuery(start: .latest, until: IggyTimestamp(microseconds: 1)),
            MetadataChangesQuery(start: .current(nil), since: IggyTimestamp(microseconds: 1)),
            MetadataChangesQuery(start: .current(nil), groups: [.consumerGroupMembers, .consumerGroups, .topics, .streams]),
            MetadataChangesQuery(start: .after(MetadataChangeCursor(logID: 1, op: 0, checksum: 0, ordinal: 3))),
        ]
        for query in refused {
            #expect(throws: IggyError(.invalidCommand), "\(query)") { try query.validate() }
        }
        let accepted: [MetadataChangesQuery] = [
            MetadataChangesQuery(),
            MetadataChangesQuery(limit: 1),
            MetadataChangesQuery(limit: MetadataChangesQuery.maxLimit),
            MetadataChangesQuery(start: .after(cursor), until: IggyTimestamp(microseconds: 1)),
            MetadataChangesQuery(start: .after(.zero(logID: 1))),
            MetadataChangesQuery(start: .current(nil), groups: [.sessions, .users]),
            MetadataChangesQuery(start: .earliest, groups: [.topics]),
        ]
        for query in accepted {
            try query.validate()
        }
    }

    @Test func givenPageTokenWhenGroupsDifferShouldRefuseTheContinuation() throws {
        let token = MetadataCurrentPageToken(anchor: cursor, requiredHead: 4200, groupsMask: 0b11, lastGroup: .topics, lastKey: [3, 0, 0, 0, 1, 0, 0, 0])
        let parsed = try MetadataCurrentPageToken(text: token.description)
        #expect(parsed == token)
        #expect(parsed.groups == [.streams, .topics])
        try MetadataChangesQuery(start: .current(parsed), groups: [.topics, .streams]).validate()
        #expect(throws: IggyError(.invalidCommand)) { try MetadataChangesQuery(start: .current(parsed)).validate() }
        #expect(throws: IggyError(.invalidCommand)) { try MetadataChangesQuery(start: .current(parsed), groups: [.streams]).validate() }

        var request = ByteReader(Requests.getMetadataChanges(MetadataChangesQuery(start: .current(parsed), groups: [.streams, .topics])))
        #expect(try request.readUInt8() == 3)
        #expect(try request.readBytes(MetadataChangeCursor.encodedSize).allSatisfy { $0 == 0 })
        try request.skip(1 + 1 + 4)
        #expect(Array(try request.readBytes(Int(try request.readUInt8()))) == [1, 2])
        #expect(Array(try request.readBytes(Int(try request.readUInt16()))) == token.encoded())
        #expect(request.isAtEnd)
    }

    @Test func givenCursorTextWhenTamperingShouldRefuseEveryNonCanonicalForm() throws {
        let text = cursor.description
        #expect(text.count == MetadataChangeCursor.textLength)
        #expect(try MetadataChangeCursor(text: text) == cursor)
        let zero = MetadataChangeCursor.zero(logID: cursor.logID)
        #expect(try MetadataChangeCursor(text: zero.description).isZero)

        let alphabet = Array("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
        let last = try #require(alphabet.firstIndex(of: text.last!))
        let encoded = String(text.dropFirst(4))
        let tampered = [
            "mc2_" + encoded, encoded, text + "A", String(text.dropLast()), "mc1_" + encoded.dropLast() + "=",
            text.replacingOccurrences(of: "_", with: "/"), String(text.dropLast()) + String(alphabet[last ^ 1]),
            MetadataChangeCursor(logID: 1, op: 0, checksum: 2, ordinal: 0).description,
        ]
        for candidate in tampered {
            #expect(throws: IggyError(.invalidCommand), "\(candidate)") { try MetadataChangeCursor(text: candidate) }
        }
    }

    @Test func givenRandomBytesWhenEncodingBase64URLShouldMatchTheStandardAlphabetUnpadded() {
        var generator = SystemRandomNumberGenerator()
        for length in 0..<70 {
            let bytes = (0..<length).map { _ in UInt8.random(in: .min ... .max, using: &generator) }
            let expected = Data(bytes).base64EncodedString().replacingOccurrences(of: "+", with: "-").replacingOccurrences(of: "/", with: "_")
                .replacingOccurrences(of: "=", with: "")
            let encoded = Base64URL.encode(bytes)
            #expect(encoded == expected, "\(length)")
            #expect(Base64URL.decode("cs1_" + encoded, prefix: "cs1_") == bytes, "\(length)")
        }
        #expect(Base64URL.decode("cs1_A", prefix: "cs1_") == nil)
        #expect(Base64URL.decode("cs1_AA==", prefix: "cs1_") == nil)
        #expect(Base64URL.decode("cs1_+w", prefix: "cs1_") == nil)
    }

    @Test func givenScriptedServerWhenReadingChangesShouldSendCode17AndDecodeTheReply() async throws {
        let reply = GoldenFixture.shared.bytes("response.get_metadata_changes")
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            if request.code == CommandCode.getMetadataChanges.rawValue {
                return .ok(reply)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        var configuration = ClientConfiguration(address: server.address, autoLogin: .usernamePassword(username: "iggy", password: "iggy"))
        configuration.connectTimeout = .seconds(2)
        configuration.heartbeatInterval = .seconds(60)
        let client = IggyClient(configuration: configuration, connector: NIOConnector(), logger: testLogger)
        try await client.connect()

        let query = MetadataChangesQuery(start: .after(cursor), groups: [.streams], limit: 10)
        let page = try await client.getMetadataChanges(query)
        #expect(page == (try Responses.metadataChanges(reply[...])))
        let sent = await server.requests.filter { $0.code == CommandCode.getMetadataChanges.rawValue }
        #expect(sent.map(\.payload) == [Requests.getMetadataChanges(query)])
        #expect(sent.allSatisfy { $0.operation == .nonReplicated })

        // A refused query never reaches the connection.
        await expectCode(.invalidCommand) { _ = try await client.getMetadataChanges(MetadataChangesQuery(limit: 0)) }
        await expectCode(.invalidCommand) { _ = try await client.getMetadataChanges(MetadataChangesQuery(start: .latest, since: .zero)) }
        #expect(await server.requests.filter { $0.code == CommandCode.getMetadataChanges.rawValue }.count == 1)
        await client.shutdown()
    }
}

/// The 32 lowercase hex characters the fixture renders a 128-bit value as.
func hex(_ value: UInt128Value) -> String {
    String(format: "%016llx%016llx", value.high, value.low)
}

/// The JSON the HTTP route renders for a record, so a decoded record compares
/// whole with the fixture's `expected` object.
func json(_ record: MetadataChangeRecord) -> JSONValue {
    var members: [String: JSONValue] = [
        "op": .number(record.op), "ordinal": .number(UInt64(record.ordinal)), "checksum": .string(hex(record.checksum)),
        "timestamp": .number(record.timestamp.microseconds),
    ]
    if let actor = record.actorUserID {
        members["actor_user_id"] = .number(UInt64(actor))
    }
    if let kind = record.change.kind {
        members["group"] = .string(kind.group.description)
        members["kind"] = .string(kind.description)
    }
    members.merge(body(record.change)) { _, body in body }
    return .object(members)
}

func json(_ item: MetadataCurrentItem) -> JSONValue {
    var members = body(item)
    if let group = item.group {
        members["group"] = .string(group.description)
    }
    return .object(members)
}

func json(_ answer: MetadataChanges) -> JSONValue {
    .object([
        "status": .string(answer.status.description), "phase": .string(answer.phase.description), "log_id": answer.logID.map { .string(hex($0)) } ?? .null,
        "oldest_op": .number(answer.oldestOp), "retained_since": .number(answer.retainedSince.microseconds), "head_op": .number(answer.headOp),
        "next": answer.next.map { .string($0.description) } ?? .null, "has_more": .bool(answer.hasMore), "records": .array(answer.records.map(json)),
        "items": .array(answer.items.map(json)), "current_page_token": answer.currentPageToken.map { .string($0.description) } ?? .null,
    ])
}

private func body(_ change: MetadataChange) -> [String: JSONValue] {
    switch change {
    case .streamCreated(let body), .streamUpdated(let body): stream(body)
    case .streamDeleted(let body):
        optional(
            streamKey(body.streamID, body.streamCreatedOp).merging(["created_at": .number(body.createdAt.microseconds), "name": .string(body.name)]) { $1 },
            "requested_op", body.requestedOp)
    case .topicCreated(let body), .topicUpdated(let body):
        options(
            topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp).merging([
                "created_at": .number(body.createdAt.microseconds), "name": .string(body.name), "partitions_count": .number(UInt64(body.partitionsCount)),
            ]) { $1 }, body.options)
    case .topicDeleted(let body):
        optional(
            topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp).merging([
                "created_at": .number(body.createdAt.microseconds), "name": .string(body.name),
            ]) { $1 }, "requested_op", body.requestedOp)
    case .partitionsChanged(let body):
        optional(
            topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp).merging([
                "partitions_count": .number(UInt64(body.partitionsCount)), "removed_partition_ids": ids(body.removedPartitionIDs),
            ]) { $1 }, "requested_op", body.requestedOp)
    case .consumerGroupCreated(let body):
        topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp).merging([
            "group_id": .number(UInt64(body.groupID)), "name": .string(body.name), "consumer_groups_count": .number(UInt64(body.consumerGroupsCount)),
        ]) { $1 }
    case .consumerGroupDeleted(let body):
        optional(
            topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp).merging([
                "group_id": .number(UInt64(body.groupID)), "name": .string(body.name), "consumer_groups_count": .number(UInt64(body.consumerGroupsCount)),
            ]) { $1 }, "requested_op", body.requestedOp)
    case .consumerGroupMemberJoined(let body):
        topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp).merging([
            "group_id": .number(UInt64(body.groupID)), "member_id": .number(UInt64(body.memberID)), "client_id": .string(hex(body.clientID)),
            "members_count": .number(UInt64(body.membersCount)), "partition_ids": ids(body.partitionIDs),
        ]) { $1 }
    case .consumerGroupMemberLeft(let body):
        topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp).merging([
            "group_id": .number(UInt64(body.groupID)), "member_id": .number(UInt64(body.memberID)), "client_id": .string(hex(body.clientID)),
            "members_count": .number(UInt64(body.membersCount)), "reason": .string(body.reason.description),
        ]) { $1 }
    case .consumerGroupPartitionHandedOver(let body):
        optional(
            topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp).merging([
                "group_id": .number(UInt64(body.groupID)), "partition_id": .number(UInt64(body.partitionID)),
            ]) { $1 }, "member_id", body.memberID.map(UInt64.init))
    case .userCreated(let body), .userUpdated(let body): user(body)
    case .userDeleted(let body), .userPasswordChanged(let body):
        userKey(body.userID, body.userCreatedAt).merging(["username": .string(body.username)]) { $1 }
    case .userPermissionsUpdated(let body):
        userKey(body.userID, body.userCreatedAt).merging([
            "username": .string(body.username), "permissions": permissions(body.permissions), "reason": .string(body.reason.description),
        ]) { $1 }
    case .personalAccessTokenCreated(let body): token(body)
    case .personalAccessTokenDeleted(let body):
        userKey(body.userID, body.userCreatedAt).merging(["name": .string(body.name), "reason": .string(body.reason.description)]) { $1 }
    case .sessionRegistered(let body): session(body)
    case .sessionClosed(let body):
        session(MetadataSession(userID: body.userID, userCreatedAt: body.userCreatedAt, clientID: body.clientID, session: body.session)).merging([
            "reason": .string(body.reason.description)
        ]) { $1 }
    case .unknown(let group, let kind, let body):
        ["group": .string(String(group)), "kind": .string(String(kind)), "body": .string(Data(body).base64EncodedString())]
    }
}

private func body(_ item: MetadataCurrentItem) -> [String: JSONValue] {
    switch item {
    case .stream(let body): stream(body)
    case .topic(let body):
        options(
            topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp).merging([
                "created_at": .number(body.createdAt.microseconds), "name": .string(body.name),
            ]) { $1 }, body.options)
    case .partitions(let body):
        topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp).merging(["partitions_count": .number(UInt64(body.partitionsCount))]) {
            $1
        }
    case .consumerGroup(let body):
        topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp).merging([
            "group_id": .number(UInt64(body.groupID)), "name": .string(body.name),
        ]) { $1 }
    case .consumerGroupMember(let body):
        topicKey(body.streamID, body.streamCreatedOp, body.topicID, body.topicCreatedOp).merging([
            "group_id": .number(UInt64(body.groupID)), "member_id": .number(UInt64(body.memberID)), "client_id": .string(hex(body.clientID)),
            "partition_ids": ids(body.partitionIDs),
        ]) { $1 }
    case .user(let body): user(body)
    case .personalAccessToken(let body): token(body)
    case .session(let body): session(body)
    case .unknown(let group, let body): ["group": .string(String(group)), "body": .string(Data(body).base64EncodedString())]
    }
}

private func streamKey(_ streamID: UInt32, _ streamCreatedOp: UInt64) -> [String: JSONValue] {
    ["stream_id": .number(UInt64(streamID)), "stream_created_op": .number(streamCreatedOp)]
}

private func topicKey(_ streamID: UInt32, _ streamCreatedOp: UInt64, _ topicID: UInt32, _ topicCreatedOp: UInt64) -> [String: JSONValue] {
    streamKey(streamID, streamCreatedOp).merging(["topic_id": .number(UInt64(topicID)), "topic_created_op": .number(topicCreatedOp)]) { $1 }
}

private func userKey(_ userID: UInt32, _ userCreatedAt: IggyTimestamp) -> [String: JSONValue] {
    ["user_id": .number(UInt64(userID)), "user_created_at": .number(userCreatedAt.microseconds)]
}

private func stream(_ body: MetadataStreamChange) -> [String: JSONValue] {
    options(
        streamKey(body.streamID, body.streamCreatedOp).merging(["created_at": .number(body.createdAt.microseconds), "name": .string(body.name)]) { $1 },
        body.options)
}

private func user(_ body: MetadataUserChange) -> [String: JSONValue] {
    options(
        userKey(body.userID, body.userCreatedAt).merging([
            "username": .string(body.username), "status": .string(body.status.description), "permissions": permissions(body.permissions),
        ]) { $1 }, body.options)
}

private func token(_ body: MetadataTokenChange) -> [String: JSONValue] {
    optional(userKey(body.userID, body.userCreatedAt).merging(["name": .string(body.name)]) { $1 }, "expiry", body.expiry?.microseconds)
}

private func session(_ body: MetadataSession) -> [String: JSONValue] {
    userKey(body.userID, body.userCreatedAt).merging(["client_id": .string(hex(body.clientID)), "session": .number(body.session)]) { $1 }
}

private func optional(_ members: [String: JSONValue], _ key: String, _ value: UInt64?) -> [String: JSONValue] {
    var members = members
    if let value {
        members[key] = .number(value)
    }
    return members
}

private func ids(_ ids: [UInt32]) -> JSONValue {
    .array(ids.map { .number(UInt64($0)) })
}

private func options(_ members: [String: JSONValue], _ options: MetadataOptions) -> [String: JSONValue] {
    var members = members
    switch options {
    case .readable(let options):
        members["options"] = .object(
            options.mapValues { option in
                let text = option.value.kind == .raw ? Data(option.value.bytes).base64EncodedString() : option.value.description
                return .object(["value": .string(text), "kind": .string(option.value.kind.description), "explicit": .bool(option.explicit)])
            })
    case .unreadable:
        members["options"] = .null
        members["options_unreadable"] = .bool(true)
    }
    return members
}

private func permissions(_ permissions: Permissions?) -> JSONValue {
    guard let permissions else {
        return .null
    }
    let global = permissions.global
    var streams: JSONValue = .null
    if !permissions.streams.isEmpty {
        streams = .object(
            Dictionary(
                uniqueKeysWithValues: permissions.streams.map { streamID, stream in
                    let topics: JSONValue =
                        stream.topics.isEmpty
                        ? .null
                        : .object(
                            Dictionary(
                                uniqueKeysWithValues: stream.topics.map { topicID, topic in
                                    (
                                        String(topicID),
                                        JSONValue.object([
                                            "manage_topic": .bool(topic.manageTopic), "read_topic": .bool(topic.readTopic),
                                            "poll_messages": .bool(topic.pollMessages), "send_messages": .bool(topic.sendMessages),
                                        ])
                                    )
                                }))
                    return (
                        String(streamID),
                        JSONValue.object([
                            "manage_stream": .bool(stream.manageStream), "read_stream": .bool(stream.readStream), "manage_topics": .bool(stream.manageTopics),
                            "read_topics": .bool(stream.readTopics), "poll_messages": .bool(stream.pollMessages), "send_messages": .bool(stream.sendMessages),
                            "topics": topics,
                        ])
                    )
                }))
    }
    return .object([
        "global": .object([
            "manage_servers": .bool(global.manageServers), "read_servers": .bool(global.readServers), "manage_users": .bool(global.manageUsers),
            "read_users": .bool(global.readUsers), "manage_streams": .bool(global.manageStreams), "read_streams": .bool(global.readStreams),
            "manage_topics": .bool(global.manageTopics), "read_topics": .bool(global.readTopics), "poll_messages": .bool(global.pollMessages),
            "send_messages": .bool(global.sendMessages),
        ]),
        "streams": streams,
    ])
}

// The README sample, never called: it only has to compile, so it cannot drift from the API.
private func follow(_ client: IggyClient, groups: [MetadataChangeGroup]) async throws {
    var query = MetadataChangesQuery(start: .latest, groups: groups)
    while true {
        let page = try await client.getMetadataChanges(query)
        switch page.status {
        case .ok:
            for record in page.records {
                print("op \(record.op) by \(String(describing: record.actorUserID)): \(record.change)")
            }
            if let next = page.next {
                query.start = .after(next)
            }
            if !page.hasMore {
                try await Task.sleep(for: .seconds(1))
            }
        case .behind:
            try await Task.sleep(for: .milliseconds(100))
        case .expired, .reset:
            query.start = try await rebuild(client, groups: groups)
        }
    }
}

private func rebuild(_ client: IggyClient, groups: [MetadataChangeGroup]) async throws -> MetadataChangeStart {
    var query = MetadataChangesQuery(start: .current(nil), groups: groups)
    while true {
        let page = try await client.getMetadataChanges(query)
        switch page.status {
        case .ok:
            break
        case .behind:
            try await Task.sleep(for: .milliseconds(100))
            continue
        case .expired, .reset:
            query.start = .current(nil)
            continue
        }
        for item in page.items {
            print(item)
        }
        if let token = page.currentPageToken {
            query.start = .current(token)
        } else if let anchor = page.next {
            return .after(anchor)
        } else {
            query.start = .current(nil)
        }
    }
}
