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

/// Keys of the topic option catalog, in catalog order.
public enum TopicOptionKey: String, Sendable, Hashable, CaseIterable {
    /// The topic's name. Updatable, never resettable.
    case name
    /// Create-only: the server stores it and does not apply it.
    case compressionAlgorithm = "compression_algorithm"
    /// Updatable and resettable to never expire.
    case messageExpiry = "message_expiry"
    /// Updatable and resettable to unlimited.
    case maxTopicSize = "max_topic_size"
    case segmentSize = "segment_size"
    case durability
    case consumerOffsetDurability = "consumer_offset_durability"
    case partitionResizePolicy = "partition_resize_policy"
    case messagesRequiredToSave = "messages_required_to_save"
    case sizeOfMessagesRequiredToSave = "size_of_messages_required_to_save"
    case preallocateSegments = "preallocate_segments"
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

/// Whether a topic's partition count may change after create.
public enum PartitionResizePolicy: String, Sendable, Hashable, CaseIterable {
    /// Partitions may be added and deleted.
    case mutable
    /// The partition set is fixed at create: the server refuses later
    /// partition creation and deletion on the topic.
    case fixed
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
    /// Nil leaves the server's default, ``PartitionResizePolicy/mutable``.
    public var partitionResizePolicy: PartitionResizePolicy?
    public var messagesRequiredToSave: UInt32?
    public var sizeOfMessagesRequiredToSaveBytes: UInt64?
    public var preallocateSegments: Bool?
    public var raw: [String: String]

    public init(
        partitionsCount: UInt32? = nil, compressionAlgorithm: CompressionAlgorithm? = nil, messageExpiry: IggyExpiry? = nil,
        maxTopicSize: MaxTopicSize? = nil, segmentSizeBytes: UInt64? = nil, durability: Durability = .replicated,
        consumerOffsetDurability: Durability = .replicated, partitionResizePolicy: PartitionResizePolicy? = nil, messagesRequiredToSave: UInt32? = nil,
        sizeOfMessagesRequiredToSaveBytes: UInt64? = nil, preallocateSegments: Bool? = nil, raw: [String: String] = [:]
    ) {
        self.partitionsCount = partitionsCount
        self.compressionAlgorithm = compressionAlgorithm
        self.messageExpiry = messageExpiry
        self.maxTopicSize = maxTopicSize
        self.segmentSizeBytes = segmentSizeBytes
        self.durability = durability
        self.consumerOffsetDurability = consumerOffsetDurability
        self.partitionResizePolicy = partitionResizePolicy
        self.messagesRequiredToSave = messagesRequiredToSave
        self.sizeOfMessagesRequiredToSaveBytes = sizeOfMessagesRequiredToSaveBytes
        self.preallocateSegments = preallocateSegments
        self.raw = raw
    }

    /// The default partition count when none is given.
    public static let defaultPartitionsCount: UInt32 = 1

    func toResourceOptions() throws -> ResourceOptions {
        var options = try rawOptions(raw)
        if let compressionAlgorithm {
            options[TopicOptionKey.compressionAlgorithm.rawValue] = .explicit(try .string(compressionAlgorithm.description))
        }
        if let messageExpiry, let microseconds = try messageExpiry.wireMicroseconds() {
            options[TopicOptionKey.messageExpiry.rawValue] = .explicit(.uint64(microseconds))
        }
        if let maxTopicSize {
            options[TopicOptionKey.maxTopicSize.rawValue] = .explicit(.uint64(maxTopicSize.wireValue))
        }
        if let segmentSizeBytes {
            options[TopicOptionKey.segmentSize.rawValue] = .explicit(.uint64(segmentSizeBytes))
        }
        for (key, policy) in [(TopicOptionKey.durability.rawValue, durability), (TopicOptionKey.consumerOffsetDurability.rawValue, consumerOffsetDurability)] {
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
        if let partitionResizePolicy {
            options[TopicOptionKey.partitionResizePolicy.rawValue] = .explicit(try .string(partitionResizePolicy.rawValue))
        }
        if let messagesRequiredToSave {
            options[TopicOptionKey.messagesRequiredToSave.rawValue] = .explicit(.uint32(messagesRequiredToSave))
        }
        if let sizeOfMessagesRequiredToSaveBytes {
            options[TopicOptionKey.sizeOfMessagesRequiredToSave.rawValue] = .explicit(.uint64(sizeOfMessagesRequiredToSaveBytes))
        }
        if let preallocateSegments {
            options[TopicOptionKey.preallocateSegments.rawValue] = .explicit(.bool(preallocateSegments))
        }
        return options
    }

    func encode() throws -> [UInt8] {
        try OptionsBlock.encode(try toResourceOptions())
    }
}

/// Options of a topic being updated. Absent keys keep their stored values.
/// Create-only settings, compression among them, have no field here, so they
/// cannot be sent only to be refused by the server.
public struct TopicUpdateOptions: Sendable, Hashable {
    /// Nil keeps the topic's name.
    public var name: String?
    /// Nil keeps the current expiry. ``IggyExpiry/serverDefault`` changes
    /// nothing: reset ``TopicOptionKey/messageExpiry`` to return to the
    /// default.
    public var messageExpiry: IggyExpiry?
    /// Nil keeps the current cap. ``MaxTopicSize/serverDefault`` changes
    /// nothing: reset ``TopicOptionKey/maxTopicSize`` to return to the
    /// default.
    public var maxTopicSize: MaxTopicSize?
    /// Keys to return to their defaults: ``TopicOptionKey/messageExpiry`` or
    /// ``TopicOptionKey/maxTopicSize``. Any other key is refused before
    /// anything is sent.
    public var reset: Set<TopicOptionKey>
    /// Updatable keys newer than this SDK, as strings the server parses.
    public var raw: [String: String]
    /// Reset names newer than this SDK. The server alone decides whether a
    /// patch carrying raw keys or names is admitted.
    public var rawReset: Set<String>

    public init(
        name: String? = nil, messageExpiry: IggyExpiry? = nil, maxTopicSize: MaxTopicSize? = nil, reset: Set<TopicOptionKey> = [],
        raw: [String: String] = [:], rawReset: Set<String> = []
    ) {
        self.name = name
        self.messageExpiry = messageExpiry
        self.maxTopicSize = maxTopicSize
        self.reset = reset
        self.raw = raw
        self.rawReset = rawReset
    }

    func toResourceOptions() throws -> ResourceOptions {
        var options = try rawOptions(raw)
        if let name {
            options[TopicOptionKey.name.rawValue] = .explicit(try columnValue(name, refusal: .invalidTopicName))
        }
        if let messageExpiry, let microseconds = try messageExpiry.wireMicroseconds() {
            options[TopicOptionKey.messageExpiry.rawValue] = .explicit(.uint64(microseconds))
        }
        if let maxTopicSize, maxTopicSize != .serverDefault {
            options[TopicOptionKey.maxTopicSize.rawValue] = .explicit(.uint64(maxTopicSize.wireValue))
        }
        return options
    }

    func toPatch() throws -> OptionsPatch {
        // A server default sets nothing, so it is no set key either.
        let typed: [TopicOptionKey] = [
            name.map { _ in .name },
            messageExpiry.flatMap { $0 == .serverDefault ? nil : .messageExpiry },
            maxTopicSize.flatMap { $0 == .serverDefault ? nil : .maxTopicSize },
        ].compactMap { $0 }
        return try OptionsPatch(typed: typed, raw: raw, reset: reset, rawReset: rawReset, values: toResourceOptions)
    }
}

extension TopicOptionKey: CatalogOptionKey {
    var flags: OptionFlags {
        switch self {
        case .name: .updatable
        case .messageExpiry, .maxTopicSize: [.updatable, .resettable]
        default: []
        }
    }
}

extension Topic {
    /// How long messages are kept, from ``options``.
    public var messageExpiry: IggyExpiry {
        resolvedMessageExpiry(options)
    }

    /// The compression label the topic was created with, from ``options``.
    public var compressionAlgorithm: CompressionAlgorithm {
        resolvedCompressionAlgorithm(options)
    }

    /// The topic's size cap, from ``options``.
    public var maxTopicSize: MaxTopicSize {
        resolvedMaxTopicSize(options)
    }
}

extension TopicDetails {
    /// How long messages are kept, from ``options``.
    public var messageExpiry: IggyExpiry {
        resolvedMessageExpiry(options)
    }

    /// The compression label the topic was created with, from ``options``.
    public var compressionAlgorithm: CompressionAlgorithm {
        resolvedCompressionAlgorithm(options)
    }

    /// The topic's size cap, from ``options``.
    public var maxTopicSize: MaxTopicSize {
        resolvedMaxTopicSize(options)
    }
}

// An unreported or unreadable key resolves to its catalog default, as in the Rust crate.

private func resolvedMessageExpiry(_ options: ResourceOptions) -> IggyExpiry {
    guard let microseconds = options[TopicOptionKey.messageExpiry.rawValue]?.value.uint64Value, microseconds != 0 else {
        return .never
    }
    return IggyExpiry(wireMicroseconds: microseconds)
}

private func resolvedCompressionAlgorithm(_ options: ResourceOptions) -> CompressionAlgorithm {
    options[TopicOptionKey.compressionAlgorithm.rawValue]?.value.stringValue.flatMap(CompressionAlgorithm.init(name:)) ?? .none
}

private func resolvedMaxTopicSize(_ options: ResourceOptions) -> MaxTopicSize {
    options[TopicOptionKey.maxTopicSize.rawValue]?.value.uint64Value.map(MaxTopicSize.init(wireValue:)) ?? .unlimited
}
