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

// The methods of the agent: read the arguments, call the SDK, and return the
// canonical form of the result. A method name is the Rust SDK method name.

extension Agent {
    func call(_ entity: String, _ method: String, _ raw: JSON?, saveAs: String?) async -> Reply {
        do {
            return .result(try await invoke(entity, method, raw, saveAs: saveAs))
        } catch {
            remember(error)
            return Reply(error)
        }
    }

    private func invoke(_ entity: String, _ method: String, _ raw: JSON?, saveAs: String?) async throws -> JSON {
        var args = try Arguments(raw)
        if method == "next" {
            try args.finish()
            return try await next(entity)
        }
        guard let client = clients[entity] else {
            throw Fault("no client \(entity)")
        }
        switch method {
        case "connect":
            try args.finish()
            try await client.connect()
            return .null
        case "disconnect":
            try args.finish()
            await client.disconnect()
            return .null
        case "ping":
            try args.finish()
            try await client.ping()
            return .null
        case "login_user":
            let username: String = try args.required("username")
            let password: String = try args.required("password")
            try args.finish()
            let identity = try await client.login(username: username, password: password)
            return .object(["user_id": JSON(identity.userID)])
        case "logout_user":
            try args.finish()
            try await client.logout()
            return .null
        case "get_stream":
            let streamID: Identifier = try args.required("stream_id")
            try args.finish()
            return try await client.getStream(streamID)?.canonical() ?? .null
        case "get_streams":
            try args.finish()
            return .array(try await client.getStreams().map { try $0.canonical() })
        case "create_stream":
            let name: String = try args.required("name")
            let key: HexBytes? = try args.optional("encryption_key_hex")
            try args.finish()
            let options = StreamCreateOptions(encryptionKey: key.map { Data($0.bytes) })
            return try await client.createStream(name: name, options: options).canonical()
        case "update_stream":
            let streamID: Identifier = try args.required("stream_id")
            let name: String? = try args.optional("name")
            let reset: [String] = try args.optional("reset") ?? []
            try args.finish()
            try await client.updateStream(streamID, options: StreamUpdateOptions(name: name, rawReset: Set(reset)))
            return .null
        case "delete_stream":
            let streamID: Identifier = try args.required("stream_id")
            try args.finish()
            try await client.deleteStream(streamID)
            return .null
        case "get_topic":
            let streamID: Identifier = try args.required("stream_id")
            let topicID: Identifier = try args.required("topic_id")
            try args.finish()
            return try await client.getTopic(streamID: streamID, topicID: topicID)?.canonical() ?? .null
        case "get_topics":
            let streamID: Identifier = try args.required("stream_id")
            try args.finish()
            return .array(try await client.getTopics(streamID: streamID).map { try $0.canonical() })
        case "create_topic":
            let streamID: Identifier = try args.required("stream_id")
            let name: String = try args.required("name")
            var options = TopicCreateOptions()
            options.partitionsCount = try args.optional("partitions_count")
            options.messageExpiry = try args.optional("message_expiry")
            options.maxTopicSize = try args.optional("max_topic_size")
            try args.finish()
            return try await client.createTopic(streamID: streamID, name: name, options: options).canonical()
        case "update_topic":
            let streamID: Identifier = try args.required("stream_id")
            let topicID: Identifier = try args.required("topic_id")
            var options = TopicUpdateOptions()
            options.name = try args.optional("name")
            options.messageExpiry = try args.optional("message_expiry")
            options.maxTopicSize = try args.optional("max_topic_size")
            options.rawReset = Set(try args.optional("reset") ?? [String]())
            try args.finish()
            try await client.updateTopic(streamID: streamID, topicID: topicID, options: options)
            return .null
        case "delete_topic":
            let streamID: Identifier = try args.required("stream_id")
            let topicID: Identifier = try args.required("topic_id")
            try args.finish()
            try await client.deleteTopic(streamID: streamID, topicID: topicID)
            return .null
        case "send_messages":
            let streamID: Identifier = try args.required("stream_id")
            let topicID: Identifier = try args.required("topic_id")
            let partitioning: Partitioning = try args.required("partitioning")
            var messages: [IggyMessage] = try args.required("messages")
            try args.finish()
            try await client.sendMessages(streamID: streamID, topicID: topicID, partitioning: partitioning, messages: &messages)
            return .null
        case "poll_messages":
            let streamID: Identifier = try args.required("stream_id")
            let topicID: Identifier = try args.required("topic_id")
            let partitionID: UInt32 = try args.required("partition_id")
            let consumer: Consumer = try args.optional("consumer") ?? .default
            let strategy: PollingStrategy = try args.required("strategy")
            var options = PollOptions(count: try args.required("count"), commit: try args.optional("commit") ?? .manual)
            options.maxWait = try milliseconds(args.optional("max_wait_ms"))
            options.minCount = try args.optional("min_count") ?? 1
            try args.finish()
            return try await client.pollMessages(
                streamID: streamID, topicID: topicID, partitionID: partitionID, consumer: consumer, strategy: strategy, options: options
            ).canonical()
        case "create_consumer_group":
            let streamID: Identifier = try args.required("stream_id")
            let topicID: Identifier = try args.required("topic_id")
            let name: String = try args.required("name")
            try args.finish()
            return try await client.createConsumerGroup(streamID: streamID, topicID: topicID, name: name).canonical()
        case "get_consumer_group":
            let streamID: Identifier = try args.required("stream_id")
            let topicID: Identifier = try args.required("topic_id")
            let groupID: Identifier = try args.required("group_id")
            try args.finish()
            return try await client.getConsumerGroup(streamID: streamID, topicID: topicID, groupID: groupID)?.canonical() ?? .null
        case "delete_consumer_group":
            let streamID: Identifier = try args.required("stream_id")
            let topicID: Identifier = try args.required("topic_id")
            let groupID: Identifier = try args.required("group_id")
            try args.finish()
            try await client.deleteConsumerGroup(streamID: streamID, topicID: topicID, groupID: groupID)
            return .null
        case "build_reader":
            let streamID: Identifier = try args.required("stream_id")
            let topicID: Identifier = try args.required("topic_id")
            let consumer: Consumer = try args.optional("consumer") ?? .default
            var configuration = ReaderConfiguration(partitions: try args.optional("partitions"))
            configuration.start = try args.optional("start") ?? configuration.start
            configuration.options = PollOptions(
                maxWait: try milliseconds(args.optional("max_wait_ms")), commit: try args.optional("commit") ?? .manual)
            try args.finish()
            guard let saveAs else {
                throw Fault("build_reader needs save_as")
            }
            readers[saveAs] = try await client.reader(streamID: streamID, topicID: topicID, consumer: consumer, configuration: configuration)
            return .null
        case "build_consumer":
            let streamID: Identifier = try args.required("stream_id")
            let topicID: Identifier = try args.required("topic_id")
            let group: String = try args.required("group")
            var options = PollOptions(maxWait: try milliseconds(args.optional("max_wait_ms")), commit: try args.optional("commit") ?? .manual)
            options.count = try args.optional("count") ?? options.count
            try args.finish()
            guard let saveAs else {
                throw Fault("build_consumer needs save_as")
            }
            let consumer = try client.consumerGroup(
                name: group, streamID: streamID, topicID: topicID, configuration: ConsumerConfiguration(pollOptions: options))
            do {
                try await consumer.initialize()
            } catch {
                await consumer.shutdown()
                throw error
            }
            consumers[saveAs] = consumer
            return .null
        case "resolve_operation":
            throw Unsupported("the SDK has no call that resolves an operation")
        case "abandon_operation":
            let operationID: UInt64 = try args.required("operation_id")
            try args.finish()
            try client.abandonOperation(operation(operationID))
            return .null
        case "send_binary_request":
            let code: UInt32 = try args.required("code")
            let payload: HexBytes? = try args.optional("payload_hex")
            try args.finish()
            let reply = try await client.sendRawRequest(code: code, payload: payload?.bytes ?? [])
            return .object(["payload_hex": .string(hex(reply))])
        case "send_binary_request_to_replica":
            let code: UInt32 = try args.required("code")
            let payload: HexBytes? = try args.optional("payload_hex")
            let replica: UInt8 = try args.required("replica")
            try args.finish()
            let reply = try await client.sendRawRequest(code: code, payload: payload?.bytes ?? [], replica: replica)
            return .object(["payload_hex": .string(hex(reply))])
        case "describe_options":
            let scope: OptionsScope = try args.required("scope")
            try args.finish()
            return .array(try await client.describeOptions(scope: scope).map { $0.canonical() })
        case "create_user":
            let username: String = try args.required("username")
            let password: String = try args.required("password")
            let status: UserStatus = try args.optional("status") ?? .active
            let permissions: Permissions? = try args.optional("permissions")
            try args.finish()
            return try await client.createUser(username: username, password: password, status: status, permissions: permissions).canonical()
        case "get_user":
            let userID: Identifier = try args.required("user_id")
            try args.finish()
            return try await client.getUser(userID)?.canonical() ?? .null
        case "update_user":
            let userID: Identifier = try args.required("user_id")
            var options = UserUpdateOptions()
            options.username = try args.optional("username")
            options.status = try args.optional("status")
            try args.finish()
            try await client.updateUser(userID, options: options)
            return .null
        case "delete_user":
            let userID: Identifier = try args.required("user_id")
            try args.finish()
            try await client.deleteUser(userID)
            return .null
        case "get_metadata_changes":
            var query = MetadataChangesQuery(start: try args.required("start"), groups: try args.optional("groups") ?? [])
            query.limit = try args.optional("limit") ?? query.limit
            try args.finish()
            return try await client.getMetadataChanges(query).canonical()
        case "get_connection_info":
            try args.finish()
            return .object(["server_address": .string(await client.serverAddress)])
        default:
            throw Unsupported("the method \(method)")
        }
    }

    /// The next batch of a reader, or the next message of a consumer.
    private func next(_ entity: String) async throws -> JSON {
        if let reader = readers[entity] {
            guard let batch = try await reader.next() else {
                throw Fault("the reader \(entity) is closed")
            }
            return try batch.canonical()
        }
        if let consumer = consumers[entity] {
            guard let received = try await consumer.next() else {
                throw Fault("the consumer \(entity) is shut down")
            }
            return .object(["partition_id": JSON(received.partitionID), "message": try received.message.canonical()])
        }
        throw Fault("no reader or consumer \(entity)")
    }
}

/// A wait in milliseconds. Without one a read answers at once, as the
/// default poll options of the Rust SDK do.
private func milliseconds(_ value: UInt64?) -> Duration {
    .milliseconds(value ?? 0)
}
