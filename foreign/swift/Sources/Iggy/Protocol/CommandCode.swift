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

/// Command codes of the Apache Iggy binary protocol, mirroring
/// `core/binary_protocol/src/codes.rs`.
public enum CommandCode: UInt32, Sendable, Hashable, CaseIterable {
    case ping = 1
    case getStats = 10
    case getSnapshotFile = 11
    case getClusterMetadata = 12
    case describeOptions = 13
    case getMe = 20
    case getClient = 21
    case getClients = 22
    case getUser = 31
    case getUsers = 32
    case createUser = 33
    case deleteUser = 34
    case updateUser = 35
    case updatePermissions = 36
    case changePassword = 37
    case loginUser = 38
    case logoutUser = 39
    case loginRegister = 40
    case getPersonalAccessTokens = 41
    case createPersonalAccessToken = 42
    case deletePersonalAccessToken = 43
    case loginWithPersonalAccessToken = 44
    case loginRegisterWithPersonalAccessToken = 45
    case pollMessages = 100
    case sendMessages = 101
    case flushUnsavedBuffer = 102
    case getConsumerOffset = 120
    case storeConsumerOffset = 121
    case deleteConsumerOffset = 122
    case getStream = 200
    case getStreams = 201
    case createStream = 202
    case deleteStream = 203
    case updateStream = 204
    case purgeStream = 205
    case getTopic = 300
    case getTopics = 301
    case createTopic = 302
    case deleteTopic = 303
    case updateTopic = 304
    case purgeTopic = 305
    case createPartitions = 402
    case deletePartitions = 403
    case deleteSegments = 503
    case getConsumerGroup = 600
    case getConsumerGroups = 601
    case createConsumerGroup = 602
    case deleteConsumerGroup = 603
    case joinConsumerGroup = 604
    case leaveConsumerGroup = 605
    case syncConsumerGroup = 606

    /// Human-readable name, identical to the Rust dispatch table.
    public var name: String {
        switch self {
        case .ping: "ping"
        case .getStats: "stats"
        case .getSnapshotFile: "snapshot"
        case .getClusterMetadata: "cluster.metadata"
        case .describeOptions: "options.describe"
        case .getMe: "me"
        case .getClient: "client.get"
        case .getClients: "client.list"
        case .getUser: "user.get"
        case .getUsers: "user.list"
        case .createUser: "user.create"
        case .deleteUser: "user.delete"
        case .updateUser: "user.update"
        case .updatePermissions: "user.permissions"
        case .changePassword: "user.password"
        case .loginUser: "user.login"
        case .logoutUser: "user.logout"
        case .loginRegister: "user.login_register"
        case .getPersonalAccessTokens: "personal_access_token.list"
        case .createPersonalAccessToken: "personal_access_token.create"
        case .deletePersonalAccessToken: "personal_access_token.delete"
        case .loginWithPersonalAccessToken: "personal_access_token.login"
        case .loginRegisterWithPersonalAccessToken: "user.login_register_with_pat"
        case .pollMessages: "message.poll"
        case .sendMessages: "message.send"
        case .flushUnsavedBuffer: "message.flush_unsaved_buffer"
        case .getConsumerOffset: "consumer_offset.get"
        case .storeConsumerOffset: "consumer_offset.store"
        case .deleteConsumerOffset: "consumer_offset.delete"
        case .getStream: "stream.get"
        case .getStreams: "stream.list"
        case .createStream: "stream.create"
        case .deleteStream: "stream.delete"
        case .updateStream: "stream.update"
        case .purgeStream: "stream.purge"
        case .getTopic: "topic.get"
        case .getTopics: "topic.list"
        case .createTopic: "topic.create"
        case .deleteTopic: "topic.delete"
        case .updateTopic: "topic.update"
        case .purgeTopic: "topic.purge"
        case .createPartitions: "partition.create"
        case .deletePartitions: "partition.delete"
        case .deleteSegments: "segment.delete"
        case .getConsumerGroup: "consumer_group.get"
        case .getConsumerGroups: "consumer_group.list"
        case .createConsumerGroup: "consumer_group.create"
        case .deleteConsumerGroup: "consumer_group.delete"
        case .joinConsumerGroup: "consumer_group.join"
        case .leaveConsumerGroup: "consumer_group.leave"
        case .syncConsumerGroup: "consumer_group.sync"
        }
    }

    /// Codes that drive the session handshake and therefore cannot be sent
    /// through the raw request path.
    static func isSessionControl(_ code: UInt32) -> Bool {
        switch CommandCode(rawValue: code) {
        case .loginUser, .logoutUser, .loginRegister, .loginWithPersonalAccessToken,
            .loginRegisterWithPersonalAccessToken:
            true
        default:
            false
        }
    }

    /// Codes that carry the sign-in handshake.
    static func isRegister(_ code: UInt32) -> Bool {
        code == CommandCode.loginRegister.rawValue
            || code == CommandCode.loginRegisterWithPersonalAccessToken.rawValue
    }
}
