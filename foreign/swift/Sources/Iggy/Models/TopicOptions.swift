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
    case replicationFactor = "replication_factor"
    case segmentMaxAge = "segment_max_age"
    /// An update turns it on, never off, and it is never resettable.
    case tieredStorage = "tiered_storage"
    /// Updatable and resettable to one day.
    case localRetentionAge = "local_retention_age"
    /// Updatable and resettable to unlimited.
    case localRetentionSize = "local_retention_size"
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
    /// Number of nodes that store each partition, 1 to 32 and at most the
    /// cluster's node count. Nil lets the server's
    /// `[cluster] default_replication_factor` apply: every node unless an
    /// operator set a number. Create-only.
    ///
    /// Two replicas keep two copies but stop accepting writes while either
    /// node is down, like Kafka with `min.insync.replicas=2`. The server
    /// refuses an out-of-range value with
    /// ``IggyErrorCode/invalidReplicationFactor``.
    public var replicationFactor: UInt32?
    /// How long after the active segment's first batch a batch starts the
    /// next segment. Nil or ``IggyExpiry/serverDefault`` leaves the server
    /// default, ``IggyExpiry/never``, so segments roll by size alone.
    /// Create-only.
    ///
    /// The server refuses less than ten minutes, and any age together with
    /// `preallocateSegments`.
    public var segmentMaxAge: IggyExpiry?
    /// Whether sealed segments move to the object store. Nil leaves the
    /// server default, off.
    ///
    /// The server refuses `true` with ``IggyErrorCode/featureUnavailable``
    /// until it tiers topics.
    public var tieredStorage: Bool?
    /// How long a tiered topic keeps an uploaded segment on local disk. Nil
    /// or ``IggyExpiry/serverDefault`` leaves the server default, one day.
    ///
    /// The server refuses an age above a finite `messageExpiry`.
    public var localRetentionAge: IggyExpiry?
    /// Topic-wide cap on the uploaded segments a tiered topic keeps on local
    /// disk. Nil leaves the server default, ``MaxTopicSize/unlimited``.
    ///
    /// The server refuses a size above a finite `maxTopicSize`.
    public var localRetentionSize: MaxTopicSize?
    public var raw: [String: String]

    public init(
        partitionsCount: UInt32? = nil, compressionAlgorithm: CompressionAlgorithm? = nil, messageExpiry: IggyExpiry? = nil,
        maxTopicSize: MaxTopicSize? = nil, segmentSizeBytes: UInt64? = nil, durability: Durability = .replicated,
        consumerOffsetDurability: Durability = .replicated, partitionResizePolicy: PartitionResizePolicy? = nil, messagesRequiredToSave: UInt32? = nil,
        sizeOfMessagesRequiredToSaveBytes: UInt64? = nil, preallocateSegments: Bool? = nil, replicationFactor: UInt32? = nil,
        segmentMaxAge: IggyExpiry? = nil, tieredStorage: Bool? = nil, localRetentionAge: IggyExpiry? = nil, localRetentionSize: MaxTopicSize? = nil,
        raw: [String: String] = [:]
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
        self.replicationFactor = replicationFactor
        self.segmentMaxAge = segmentMaxAge
        self.tieredStorage = tieredStorage
        self.localRetentionAge = localRetentionAge
        self.localRetentionSize = localRetentionSize
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
        if let replicationFactor {
            options[TopicOptionKey.replicationFactor.rawValue] = .explicit(.uint32(replicationFactor))
        }
        if let segmentMaxAge, let microseconds = try segmentMaxAge.wireMicroseconds() {
            options[TopicOptionKey.segmentMaxAge.rawValue] = .explicit(.uint64(microseconds))
        }
        if let tieredStorage {
            options[TopicOptionKey.tieredStorage.rawValue] = .explicit(.bool(tieredStorage))
        }
        if let localRetentionAge, let microseconds = try localRetentionAge.wireMicroseconds() {
            options[TopicOptionKey.localRetentionAge.rawValue] = .explicit(.uint64(microseconds))
        }
        if let localRetentionSize {
            options[TopicOptionKey.localRetentionSize.rawValue] = .explicit(.uint64(localRetentionSize.wireValue))
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
    /// `true` turns tiered storage on. Nil keeps it as it is, and `false` is
    /// refused: an update never turns it off. The refusal comes before
    /// anything is sent, or from the server when the patch carries ``raw``
    /// keys or ``rawReset`` names.
    public var tieredStorage: Bool?
    /// Nil keeps the current local retention age.
    /// ``IggyExpiry/serverDefault`` changes nothing: reset
    /// ``TopicOptionKey/localRetentionAge`` to return to the default.
    public var localRetentionAge: IggyExpiry?
    /// Nil keeps the current local retention size.
    /// ``MaxTopicSize/serverDefault`` changes nothing: reset
    /// ``TopicOptionKey/localRetentionSize`` to return to the default.
    public var localRetentionSize: MaxTopicSize?
    /// Keys to return to their defaults: ``TopicOptionKey/messageExpiry``,
    /// ``TopicOptionKey/maxTopicSize``, ``TopicOptionKey/localRetentionAge``
    /// or ``TopicOptionKey/localRetentionSize``. Any other key is refused
    /// before anything is sent.
    public var reset: Set<TopicOptionKey>
    /// Updatable keys newer than this SDK, as strings the server parses.
    public var raw: [String: String]
    /// Reset names newer than this SDK. The server alone decides whether a
    /// patch carrying raw keys or names is admitted.
    public var rawReset: Set<String>

    public init(
        name: String? = nil, messageExpiry: IggyExpiry? = nil, maxTopicSize: MaxTopicSize? = nil, tieredStorage: Bool? = nil,
        localRetentionAge: IggyExpiry? = nil, localRetentionSize: MaxTopicSize? = nil, reset: Set<TopicOptionKey> = [], raw: [String: String] = [:],
        rawReset: Set<String> = []
    ) {
        self.name = name
        self.messageExpiry = messageExpiry
        self.maxTopicSize = maxTopicSize
        self.tieredStorage = tieredStorage
        self.localRetentionAge = localRetentionAge
        self.localRetentionSize = localRetentionSize
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
        if let tieredStorage {
            // A patch with a raw key or raw reset name is the server's to
            // refuse, in its own order, so `false` is sent with it.
            guard tieredStorage || !raw.isEmpty || !rawReset.isEmpty else {
                throw IggyError(.optionNotUpdatable, context: TopicOptionKey.tieredStorage.rawValue)
            }
            options[TopicOptionKey.tieredStorage.rawValue] = .explicit(.bool(tieredStorage))
        }
        if let localRetentionAge, let microseconds = try localRetentionAge.wireMicroseconds() {
            options[TopicOptionKey.localRetentionAge.rawValue] = .explicit(.uint64(microseconds))
        }
        if let localRetentionSize, localRetentionSize != .serverDefault {
            options[TopicOptionKey.localRetentionSize.rawValue] = .explicit(.uint64(localRetentionSize.wireValue))
        }
        return options
    }

    func toPatch() throws -> OptionsPatch {
        // A server default sets nothing, so it is no set key either.
        let typed: [TopicOptionKey] = [
            name.map { _ in .name },
            messageExpiry.flatMap { $0 == .serverDefault ? nil : .messageExpiry },
            maxTopicSize.flatMap { $0 == .serverDefault ? nil : .maxTopicSize },
            tieredStorage.map { _ in .tieredStorage },
            localRetentionAge.flatMap { $0 == .serverDefault ? nil : .localRetentionAge },
            localRetentionSize.flatMap { $0 == .serverDefault ? nil : .localRetentionSize },
        ].compactMap { $0 }
        return try OptionsPatch(typed: typed, raw: raw, reset: reset, rawReset: rawReset, values: toResourceOptions)
    }
}

extension TopicOptionKey: CatalogOptionKey {
    var flags: OptionFlags {
        switch self {
        case .name: .updatable
        case .messageExpiry, .maxTopicSize, .localRetentionAge, .localRetentionSize: [.updatable, .resettable]
        case .segmentMaxAge: .critical
        case .tieredStorage: [.updatable, .critical]
        default: []
        }
    }

    var isEnableOnly: Bool {
        self == .tieredStorage
    }
}

extension Topic {
    /// How long messages are kept, from ``options``.
    public var messageExpiry: IggyExpiry {
        resolvedExpiry(options, .messageExpiry)
    }

    /// The compression label the topic was created with, from ``options``.
    public var compressionAlgorithm: CompressionAlgorithm {
        resolvedCompressionAlgorithm(options)
    }

    /// The topic's size cap, from ``options``.
    public var maxTopicSize: MaxTopicSize {
        resolvedMaxTopicSize(options)
    }

    /// How long after a segment's first batch a batch starts the next one,
    /// from ``options``.
    public var segmentMaxAge: IggyExpiry {
        resolvedExpiry(options, .segmentMaxAge)
    }

    /// Whether sealed segments move to the object store, from ``options``.
    public var tieredStorage: Bool {
        resolvedTieredStorage(options)
    }

    /// How long an uploaded segment stays on local disk, from ``options``.
    /// Never above ``messageExpiry``.
    public var localRetentionAge: IggyExpiry {
        resolvedLocalRetentionAge(options)
    }

    /// The cap on the uploaded segments kept on local disk, from
    /// ``options``. Never above ``maxTopicSize``.
    public var localRetentionSize: MaxTopicSize {
        resolvedLocalRetentionSize(options)
    }
}

extension TopicDetails {
    /// How long messages are kept, from ``options``.
    public var messageExpiry: IggyExpiry {
        resolvedExpiry(options, .messageExpiry)
    }

    /// The compression label the topic was created with, from ``options``.
    public var compressionAlgorithm: CompressionAlgorithm {
        resolvedCompressionAlgorithm(options)
    }

    /// The topic's size cap, from ``options``.
    public var maxTopicSize: MaxTopicSize {
        resolvedMaxTopicSize(options)
    }

    /// How long after a segment's first batch a batch starts the next one,
    /// from ``options``.
    public var segmentMaxAge: IggyExpiry {
        resolvedExpiry(options, .segmentMaxAge)
    }

    /// Whether sealed segments move to the object store, from ``options``.
    public var tieredStorage: Bool {
        resolvedTieredStorage(options)
    }

    /// How long an uploaded segment stays on local disk, from ``options``.
    /// Never above ``messageExpiry``.
    public var localRetentionAge: IggyExpiry {
        resolvedLocalRetentionAge(options)
    }

    /// The cap on the uploaded segments kept on local disk, from
    /// ``options``. Never above ``maxTopicSize``.
    public var localRetentionSize: MaxTopicSize {
        resolvedLocalRetentionSize(options)
    }
}

// An unreported or unreadable key resolves to its catalog default, as in the Rust crate.

private func resolvedExpiry(_ options: ResourceOptions, _ key: TopicOptionKey) -> IggyExpiry {
    guard let microseconds = options[key.rawValue]?.value.uint64Value, microseconds != 0 else {
        return .never
    }
    return IggyExpiry(wireMicroseconds: microseconds)
}

private func resolvedCompressionAlgorithm(_ options: ResourceOptions) -> CompressionAlgorithm {
    options[TopicOptionKey.compressionAlgorithm.rawValue]?.value.stringValue.flatMap(CompressionAlgorithm.init(name:)) ?? .none
}

private func resolvedTieredStorage(_ options: ResourceOptions) -> Bool {
    options[TopicOptionKey.tieredStorage.rawValue]?.value.boolValue ?? false
}

// A tiered topic keeps nothing on local disk longer, or in a larger amount,
// than it keeps at all, so both local bounds yield to the topic's own.

private func resolvedLocalRetentionAge(_ options: ResourceOptions) -> IggyExpiry {
    let oneDay: UInt64 = 86_400_000_000
    let local = storedLimit(options, .localRetentionAge) ?? oneDay
    return IggyExpiry(wireMicroseconds: min(local, storedLimit(options, .messageExpiry) ?? UInt64.max))
}

private func resolvedLocalRetentionSize(_ options: ResourceOptions) -> MaxTopicSize {
    let local = storedLimit(options, .localRetentionSize) ?? UInt64.max
    return MaxTopicSize(wireValue: min(local, storedLimit(options, .maxTopicSize) ?? UInt64.max))
}

/// The stored value of an expiry or size key, nil when unreported or 0.
private func storedLimit(_ options: ResourceOptions, _ key: TopicOptionKey) -> UInt64? {
    options[key.rawValue]?.value.uint64Value.flatMap { $0 == 0 ? nil : $0 }
}

private func resolvedMaxTopicSize(_ options: ResourceOptions) -> MaxTopicSize {
    options[TopicOptionKey.maxTopicSize.rawValue]?.value.uint64Value.map(MaxTopicSize.init(wireValue:)) ?? .unlimited
}
