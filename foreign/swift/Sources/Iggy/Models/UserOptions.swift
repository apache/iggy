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

/// Keys of the user option catalog a client can name, in catalog order.
public enum UserOptionKey: String, Sendable, Hashable, CaseIterable {
    /// The user's name. Updatable, never resettable.
    case username
    /// Whether the user can sign in. Updatable, never resettable.
    case status
}

/// Options of a user being created. The catalog has no user option yet, so
/// `raw`, keys newer than this SDK as strings the server parses, is the only
/// field.
public struct UserCreateOptions: Sendable, Hashable {
    public var raw: [String: String]

    public init(raw: [String: String] = [:]) {
        self.raw = raw
    }

    func encode() throws -> [UInt8] {
        try OptionsBlock.encode(try rawOptions(raw))
    }
}

/// Options of a user being updated. Absent keys keep their stored values.
public struct UserUpdateOptions: Sendable, Hashable {
    /// Nil keeps the username.
    public var username: String?
    /// Nil keeps the status. Deactivating a user ends its sessions.
    public var status: UserStatus?
    /// Keys to return to their defaults. No user key is resettable yet, so a
    /// non-empty set is refused before anything is sent.
    public var reset: Set<UserOptionKey>
    /// Updatable keys newer than this SDK, as strings the server parses.
    public var raw: [String: String]
    /// Reset names newer than this SDK. The server alone decides whether a
    /// patch carrying raw keys or names is admitted.
    public var rawReset: Set<String>

    public init(
        username: String? = nil, status: UserStatus? = nil, reset: Set<UserOptionKey> = [], raw: [String: String] = [:], rawReset: Set<String> = []
    ) {
        self.username = username
        self.status = status
        self.reset = reset
        self.raw = raw
        self.rawReset = rawReset
    }

    func toResourceOptions() throws -> ResourceOptions {
        var options = try rawOptions(raw)
        if let username {
            try Requests.validateUsername(username)
            options[UserOptionKey.username.rawValue] = .explicit(try columnValue(username, refusal: .invalidUsername))
        }
        if let status {
            options[UserOptionKey.status.rawValue] = .explicit(try .string(status.description))
        }
        return options
    }

    func toPatch() throws -> OptionsPatch {
        let typed: [UserOptionKey] = [username.map { _ in .username }, status.map { _ in .status }].compactMap { $0 }
        return try OptionsPatch(typed: typed, raw: raw, reset: reset, rawReset: rawReset, values: toResourceOptions)
    }
}

extension UserOptionKey: CatalogOptionKey {
    var flags: OptionFlags {
        .updatable
    }
}
