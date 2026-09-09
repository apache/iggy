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

/// Server-wide permissions of a user.
public struct GlobalPermissions: Sendable, Hashable, Codable {
    public var manageServers = false
    public var readServers = false
    public var manageUsers = false
    public var readUsers = false
    public var manageStreams = false
    public var readStreams = false
    public var manageTopics = false
    public var readTopics = false
    public var pollMessages = false
    public var sendMessages = false

    public init(
        manageServers: Bool = false, readServers: Bool = false, manageUsers: Bool = false, readUsers: Bool = false,
        manageStreams: Bool = false, readStreams: Bool = false, manageTopics: Bool = false, readTopics: Bool = false,
        pollMessages: Bool = false, sendMessages: Bool = false
    ) {
        self.manageServers = manageServers
        self.readServers = readServers
        self.manageUsers = manageUsers
        self.readUsers = readUsers
        self.manageStreams = manageStreams
        self.readStreams = readStreams
        self.manageTopics = manageTopics
        self.readTopics = readTopics
        self.pollMessages = pollMessages
        self.sendMessages = sendMessages
    }

    /// Every permission granted.
    public static let all = GlobalPermissions(
        manageServers: true, readServers: true, manageUsers: true, readUsers: true, manageStreams: true,
        readStreams: true, manageTopics: true, readTopics: true, pollMessages: true, sendMessages: true)
}

/// Permissions of a user on one topic.
public struct TopicPermissions: Sendable, Hashable, Codable {
    public var manageTopic = false
    public var readTopic = false
    public var pollMessages = false
    public var sendMessages = false

    public init(manageTopic: Bool = false, readTopic: Bool = false, pollMessages: Bool = false, sendMessages: Bool = false) {
        self.manageTopic = manageTopic
        self.readTopic = readTopic
        self.pollMessages = pollMessages
        self.sendMessages = sendMessages
    }
}

/// Permissions of a user on one stream and, optionally, on its topics.
public struct StreamPermissions: Sendable, Hashable, Codable {
    public var manageStream = false
    public var readStream = false
    public var manageTopics = false
    public var readTopics = false
    public var pollMessages = false
    public var sendMessages = false
    public var topics: [UInt32: TopicPermissions] = [:]

    public init(
        manageStream: Bool = false, readStream: Bool = false, manageTopics: Bool = false, readTopics: Bool = false,
        pollMessages: Bool = false, sendMessages: Bool = false, topics: [UInt32: TopicPermissions] = [:]
    ) {
        self.manageStream = manageStream
        self.readStream = readStream
        self.manageTopics = manageTopics
        self.readTopics = readTopics
        self.pollMessages = pollMessages
        self.sendMessages = sendMessages
        self.topics = topics
    }
}

/// The full permission set of a user.
public struct Permissions: Sendable, Hashable, Codable {
    public var global: GlobalPermissions
    public var streams: [UInt32: StreamPermissions]

    public init(global: GlobalPermissions = GlobalPermissions(), streams: [UInt32: StreamPermissions] = [:]) {
        self.global = global
        self.streams = streams
    }
}

extension Permissions {
    private static let hasNext: UInt8 = 1
    private static let noNext: UInt8 = 0

    /// Streams and topics are written sorted by id, the deterministic layout
    /// the Rust SDK produces.
    func encode() -> [UInt8] {
        var writer = ByteWriter()
        writer.write(global.manageServers)
        writer.write(global.readServers)
        writer.write(global.manageUsers)
        writer.write(global.readUsers)
        writer.write(global.manageStreams)
        writer.write(global.readStreams)
        writer.write(global.manageTopics)
        writer.write(global.readTopics)
        writer.write(global.pollMessages)
        writer.write(global.sendMessages)
        let streams = self.streams.sorted { $0.key < $1.key }
        if streams.isEmpty {
            writer.write(Self.noNext)
            return writer.bytes
        }
        writer.write(Self.hasNext)
        for (index, (streamID, stream)) in streams.enumerated() {
            writer.write(streamID)
            writer.write(stream.manageStream)
            writer.write(stream.readStream)
            writer.write(stream.manageTopics)
            writer.write(stream.readTopics)
            writer.write(stream.pollMessages)
            writer.write(stream.sendMessages)
            let topics = stream.topics.sorted { $0.key < $1.key }
            if topics.isEmpty {
                writer.write(Self.noNext)
            } else {
                writer.write(Self.hasNext)
                for (topicIndex, (topicID, topic)) in topics.enumerated() {
                    writer.write(topicID)
                    writer.write(topic.manageTopic)
                    writer.write(topic.readTopic)
                    writer.write(topic.pollMessages)
                    writer.write(topic.sendMessages)
                    writer.write(topicIndex == topics.count - 1 ? Self.noNext : Self.hasNext)
                }
            }
            writer.write(index == streams.count - 1 ? Self.noNext : Self.hasNext)
        }
        return writer.bytes
    }

    static func decode(from reader: inout ByteReader) throws -> Permissions {
        var global = GlobalPermissions()
        global.manageServers = try reader.readBool()
        global.readServers = try reader.readBool()
        global.manageUsers = try reader.readBool()
        global.readUsers = try reader.readBool()
        global.manageStreams = try reader.readBool()
        global.readStreams = try reader.readBool()
        global.manageTopics = try reader.readBool()
        global.readTopics = try reader.readBool()
        global.pollMessages = try reader.readBool()
        global.sendMessages = try reader.readBool()
        var streams: [UInt32: StreamPermissions] = [:]
        if try reader.readBool() {
            while true {
                let streamID = try reader.readUInt32()
                var stream = StreamPermissions()
                stream.manageStream = try reader.readBool()
                stream.readStream = try reader.readBool()
                stream.manageTopics = try reader.readBool()
                stream.readTopics = try reader.readBool()
                stream.pollMessages = try reader.readBool()
                stream.sendMessages = try reader.readBool()
                if try reader.readBool() {
                    while true {
                        let topicID = try reader.readUInt32()
                        var topic = TopicPermissions()
                        topic.manageTopic = try reader.readBool()
                        topic.readTopic = try reader.readBool()
                        topic.pollMessages = try reader.readBool()
                        topic.sendMessages = try reader.readBool()
                        stream.topics[topicID] = topic
                        if try !reader.readBool() {
                            break
                        }
                    }
                }
                streams[streamID] = stream
                if try !reader.readBool() {
                    break
                }
            }
        }
        return Permissions(global: global, streams: streams)
    }
}
