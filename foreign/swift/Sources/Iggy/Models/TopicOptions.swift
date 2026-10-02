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

/// Keys of the topic option catalog.
public enum TopicOptionKey {
    public static let compressionAlgorithm = "compression_algorithm"
    public static let messageExpiry = "message_expiry"
    public static let maxTopicSize = "max_topic_size"
    public static let segmentSize = "segment_size"
    public static let durability = "durability"
    public static let consumerOffsetDurability = "consumer_offset_durability"
    public static let messagesRequiredToSave = "messages_required_to_save"
    public static let sizeOfMessagesRequiredToSave = "size_of_messages_required_to_save"
    public static let preallocateSegments = "preallocate_segments"
}

/// How far a write has to get before the server completes it.
public enum Durability: String, Sendable, Hashable, CaseIterable {
    /// Quorum commit without an additional stable-storage barrier.
    case replicated
    /// Quorum commit backed by recoverable stable-storage copies.
    case persisted

    /// Parses the wire spelling case-insensitively, as the server does.
    init?(parsing text: String) {
        self.init(rawValue: text.lowercased())
    }
}

/// Options of a topic being created. Every knob is optional; an absent key
/// resolves against the server's defaults at admission.
///
/// `raw` carries keys this SDK has no typed field for, as strings the server
/// parses, so a newer server's option costs no SDK change.
public struct TopicCreateOptions: Sendable, Hashable {
    /// Number of partitions the topic starts with. A field of the create
    /// command rather than an option key; nil means 1, the server's
    /// `DEFAULT_PARTITIONS_COUNT`.
    public var partitionsCount: UInt32?
    public var compressionAlgorithm: CompressionAlgorithm?
    public var messageExpiry: IggyExpiry?
    public var maxTopicSize: MaxTopicSize?
    public var segmentSizeBytes: UInt64?
    /// Completion policy for messages. Always sent, like the Rust client does,
    /// so the topic records the policy it was created with.
    public var durability: Durability
    /// Completion policy for stored consumer offsets, independent of `durability`.
    public var consumerOffsetDurability: Durability
    public var messagesRequiredToSave: UInt32?
    public var sizeOfMessagesRequiredToSaveBytes: UInt64?
    public var preallocateSegments: Bool?
    public var raw: [String: String]

    public init(
        partitionsCount: UInt32? = nil, compressionAlgorithm: CompressionAlgorithm? = nil, messageExpiry: IggyExpiry? = nil,
        maxTopicSize: MaxTopicSize? = nil, segmentSizeBytes: UInt64? = nil, durability: Durability = .replicated,
        consumerOffsetDurability: Durability = .replicated, messagesRequiredToSave: UInt32? = nil,
        sizeOfMessagesRequiredToSaveBytes: UInt64? = nil, preallocateSegments: Bool? = nil, raw: [String: String] = [:]
    ) {
        self.partitionsCount = partitionsCount
        self.compressionAlgorithm = compressionAlgorithm
        self.messageExpiry = messageExpiry
        self.maxTopicSize = maxTopicSize
        self.segmentSizeBytes = segmentSizeBytes
        self.durability = durability
        self.consumerOffsetDurability = consumerOffsetDurability
        self.messagesRequiredToSave = messagesRequiredToSave
        self.sizeOfMessagesRequiredToSaveBytes = sizeOfMessagesRequiredToSaveBytes
        self.preallocateSegments = preallocateSegments
        self.raw = raw
    }

    func toResourceOptions() throws -> ResourceOptions {
        var options = try TopicUpdateOptions(
            compressionAlgorithm: compressionAlgorithm, messageExpiry: messageExpiry, maxTopicSize: maxTopicSize, raw: raw
        ).toResourceOptions()
        if let segmentSizeBytes {
            options[TopicOptionKey.segmentSize] = .explicit(.uint64(segmentSizeBytes))
        }
        for (key, policy) in [(TopicOptionKey.durability, durability), (TopicOptionKey.consumerOffsetDurability, consumerOffsetDurability)] {
            // A raw entry may strengthen the default; it may not weaken a
            // typed `persisted`, which would make the two fields disagree.
            // Whatever spelling it used, the canonical one is sent.
            var effective = policy
            if let rawValue = raw[key] {
                guard let parsed = Durability(parsing: rawValue) else {
                    throw IggyError(.invalidOptionValue, context: key)
                }
                if policy == .persisted, parsed != policy {
                    throw IggyError(.invalidOptionValue, context: key)
                }
                effective = parsed
            }
            options[key] = .explicit(try .string(effective.rawValue))
        }
        if let messagesRequiredToSave {
            options[TopicOptionKey.messagesRequiredToSave] = .explicit(.uint32(messagesRequiredToSave))
        }
        if let sizeOfMessagesRequiredToSaveBytes {
            options[TopicOptionKey.sizeOfMessagesRequiredToSave] = .explicit(.uint64(sizeOfMessagesRequiredToSaveBytes))
        }
        if let preallocateSegments {
            options[TopicOptionKey.preallocateSegments] = .explicit(.bool(preallocateSegments))
        }
        return options
    }

    func encode() throws -> [UInt8] {
        try OptionsBlock.encode(try toResourceOptions())
    }
}

/// Options of a topic being updated.
public struct TopicUpdateOptions: Sendable, Hashable {
    public var compressionAlgorithm: CompressionAlgorithm?
    public var messageExpiry: IggyExpiry?
    public var maxTopicSize: MaxTopicSize?
    public var raw: [String: String]

    public init(
        compressionAlgorithm: CompressionAlgorithm? = nil, messageExpiry: IggyExpiry? = nil, maxTopicSize: MaxTopicSize? = nil, raw: [String: String] = [:]
    ) {
        self.compressionAlgorithm = compressionAlgorithm
        self.messageExpiry = messageExpiry
        self.maxTopicSize = maxTopicSize
        self.raw = raw
    }

    func toResourceOptions() throws -> ResourceOptions {
        var options = try rawOptions(raw)
        if let compressionAlgorithm {
            options[TopicOptionKey.compressionAlgorithm] = .explicit(try .string(compressionAlgorithm.description))
        }
        if let messageExpiry, let microseconds = try messageExpiry.wireMicroseconds() {
            options[TopicOptionKey.messageExpiry] = .explicit(.uint64(microseconds))
        }
        if let maxTopicSize {
            options[TopicOptionKey.maxTopicSize] = .explicit(.uint64(maxTopicSize.wireValue))
        }
        return options
    }

    func encode() throws -> [UInt8] {
        try OptionsBlock.encode(try toResourceOptions())
    }
}

/// Options of a stream being updated. The catalog has no stream keys today;
/// `raw` is forwarded as-is for forward compatibility.
public struct StreamUpdateOptions: Sendable, Hashable {
    public var raw: [String: String]

    public init(raw: [String: String] = [:]) {
        self.raw = raw
    }

    func encode() throws -> [UInt8] {
        try OptionsBlock.encode(try rawOptions(raw))
    }
}

/// Options of a user being updated. The catalog has no user keys today;
/// `raw` is forwarded as-is for forward compatibility.
public struct UserUpdateOptions: Sendable, Hashable {
    public var raw: [String: String]

    public init(raw: [String: String] = [:]) {
        self.raw = raw
    }

    func encode() throws -> [UInt8] {
        try OptionsBlock.encode(try rawOptions(raw))
    }
}

/// Characters of an offending key an error message shows,
/// `ERROR_KEY_PREVIEW_LEN` in the Rust crate.
private let errorKeyPreviewLength = 64

private func rawOptions(_ raw: [String: String]) throws -> ResourceOptions {
    var options: ResourceOptions = [:]
    for (key, value) in raw {
        guard !key.isEmpty, key.utf8.count <= HeaderTLV.maxFieldLength else {
            throw IggyError(.unsupportedOptionKey, context: String(key.prefix(errorKeyPreviewLength)))
        }
        guard let headerValue = try? HeaderValue.string(value) else {
            throw IggyError(.invalidOptionValue, context: String(key.prefix(errorKeyPreviewLength)))
        }
        options[key] = .explicit(headerValue)
    }
    return options
}
