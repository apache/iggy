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

package iggcon

import "encoding/binary"

// TopicOptionKey names a topic option of the server catalog.
type TopicOptionKey string

// Topic option keys. Only TopicOptionName, TopicOptionMessageExpiry and
// TopicOptionMaxTopicSize are updatable, and only the last two resettable.
// Every other key is create-only, so an update refuses it by name.
// DescribeOptions enumerates the catalog one server serves, with the bounds
// each value is checked against.
const (
	TopicOptionName                         TopicOptionKey = "name"
	TopicOptionCompressionAlgorithm         TopicOptionKey = "compression_algorithm"
	TopicOptionMessageExpiry                TopicOptionKey = "message_expiry"
	TopicOptionMaxTopicSize                 TopicOptionKey = "max_topic_size"
	TopicOptionSegmentSize                  TopicOptionKey = "segment_size"
	TopicOptionDurability                   TopicOptionKey = "durability"
	TopicOptionConsumerOffsetDurability     TopicOptionKey = "consumer_offset_durability"
	TopicOptionPartitionResizePolicy        TopicOptionKey = "partition_resize_policy"
	TopicOptionMessagesRequiredToSave       TopicOptionKey = "messages_required_to_save"
	TopicOptionSizeOfMessagesRequiredToSave TopicOptionKey = "size_of_messages_required_to_save"
	TopicOptionPreallocateSegments          TopicOptionKey = "preallocate_segments"
)

// TopicOptions are the options CreateTopic may carry. A nil *TopicOptions
// selects the defaults, and so does a nil field.
//
// The constructors below build the same entries for Raw, the escape hatch
// for keys newer than this build. A typed field wins on collision.
type TopicOptions struct {
	// PartitionsCount is how many partitions the topic starts with, 1 when
	// nil. It is an argument of the create, not a stored option.
	PartitionsCount *uint32
	// CompressionAlgorithm is a create-only label the server stores and
	// reports. CompressionAlgorithmNone and the default leave it derived.
	CompressionAlgorithm *CompressionAlgorithm
	// MessageExpiry in microseconds. IggyExpiryServerDefault leaves it derived.
	MessageExpiry *Duration
	// MaxTopicSize in bytes, math.MaxUint64 for unlimited. Zero leaves it
	// derived.
	MaxTopicSize *uint64
	// SegmentSize, see SegmentSizeOption.
	SegmentSize *uint64
	// Durability of messages, replicated when nil.
	Durability *Durability
	// ConsumerOffsetDurability of explicit offset writes, replicated when nil.
	ConsumerOffsetDurability *Durability
	// PartitionResizePolicy, mutable when nil.
	PartitionResizePolicy *PartitionResizePolicy
	// MessagesRequiredToSave, see MessagesRequiredToSaveOption.
	MessagesRequiredToSave *uint32
	// SizeOfMessagesRequiredToSave, see SizeOfMessagesRequiredToSaveOption.
	SizeOfMessagesRequiredToSave *uint64
	// PreallocateSegments, see PreallocateSegmentsOption.
	PreallocateSegments *bool
	// Raw carries keys newer than this build.
	Raw []HeaderEntry
}

// TopicUpdate is a patch of a topic's options. Absent keys keep their stored
// values. Create-only settings, compression included, have no field here.
type TopicUpdate struct {
	// Name renames the topic when set.
	Name *string
	// MessageExpiry sets the expiry in microseconds. IggyExpiryServerDefault
	// changes nothing, Reset returns the key to its default.
	MessageExpiry *Duration
	// MaxTopicSize sets the size cap in bytes. Zero changes nothing, Reset
	// returns the key to its default.
	MaxTopicSize *uint64
	// Reset names keys to return to their defaults: TopicOptionMessageExpiry
	// or TopicOptionMaxTopicSize.
	Reset []TopicOptionKey
	// RawReset names keys to reset that this build may not know. The server
	// decides whether each one may be reset, and a patch that carries any raw
	// name or Options entry is checked by the server alone.
	RawReset []string
	// Options carries updatable keys newer than this build. A typed field wins
	// on collision.
	Options []HeaderEntry
}

// SegmentSizeOption sets how large a partition segment grows before it rotates.
// The server takes a 512-byte multiple inside the bounds its catalog reports.
// Zero is not refused: it leaves the key to the server default.
func SegmentSizeOption(bytes uint64) HeaderEntry {
	return uint64Option(TopicOptionSegmentSize, bytes)
}

type Durability string

const (
	DurabilityReplicated Durability = "replicated"
	DurabilityPersisted  Durability = "persisted"
)

func DurabilityOption(value Durability) HeaderEntry {
	return stringOption(TopicOptionDurability, string(value))
}

func ConsumerOffsetDurabilityOption(value Durability) HeaderEntry {
	return stringOption(TopicOptionConsumerOffsetDurability, string(value))
}

// PartitionResizePolicy says whether a topic may add and delete partitions.
type PartitionResizePolicy string

const (
	PartitionResizePolicyMutable PartitionResizePolicy = "mutable"
	PartitionResizePolicyFixed   PartitionResizePolicy = "fixed"
)

// PartitionResizePolicyOption fixes the policy at create. A fixed topic keeps
// the partitions it was created with.
func PartitionResizePolicyOption(value PartitionResizePolicy) HeaderEntry {
	return stringOption(TopicOptionPartitionResizePolicy, string(value))
}

// MessagesRequiredToSaveOption flushes the journal once it holds this many
// messages. Zero is refused. Pairs with SizeOfMessagesRequiredToSaveOption:
// whichever threshold trips first flushes.
func MessagesRequiredToSaveOption(messages uint32) HeaderEntry {
	return uint32Option(TopicOptionMessagesRequiredToSave, messages)
}

// SizeOfMessagesRequiredToSaveOption flushes the journal once it holds this many
// bytes. A threshold above the largest a segment may be is refused, since it
// could never trip. Zero leaves the key to the server default.
func SizeOfMessagesRequiredToSaveOption(bytes uint64) HeaderEntry {
	return uint64Option(TopicOptionSizeOfMessagesRequiredToSave, bytes)
}

// PreallocateSegmentsOption reserves each segment's bytes on disk up front. A
// topic reserves segment_size for every partition it has, and the server refuses
// a create whose product crosses its preallocation cap.
func PreallocateSegmentsOption(enabled bool) HeaderEntry {
	return boolOption(TopicOptionPreallocateSegments, enabled)
}

func stringOption(key TopicOptionKey, value string) HeaderEntry {
	return HeaderEntry{
		Key:   HeaderKey{Kind: String, Value: []byte(key)},
		Value: HeaderValue{Kind: String, Value: []byte(value)},
	}
}

func uint64Option(key TopicOptionKey, value uint64) HeaderEntry {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, value)
	return HeaderEntry{
		Key:   HeaderKey{Kind: String, Value: []byte(key)},
		Value: HeaderValue{Kind: Uint64, Value: buf},
	}
}

func uint32Option(key TopicOptionKey, value uint32) HeaderEntry {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, value)
	return HeaderEntry{
		Key:   HeaderKey{Kind: String, Value: []byte(key)},
		Value: HeaderValue{Kind: Uint32, Value: buf},
	}
}

func boolOption(key TopicOptionKey, value bool) HeaderEntry {
	encoded := byte(0)
	if value {
		encoded = 1
	}
	return HeaderEntry{
		Key:   HeaderKey{Kind: String, Value: []byte(key)},
		Value: HeaderValue{Kind: Bool, Value: []byte{encoded}},
	}
}
