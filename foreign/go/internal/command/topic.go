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

package command

import (
	"encoding/binary"
	"slices"

	"github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

// defaultPartitionsCount is what a topic starts with when the options name
// no count, as in the other SDKs.
const defaultPartitionsCount = 1

type CreateTopic struct {
	StreamId iggcon.Identifier
	Name     string
	Options  iggcon.TopicOptions
}

func (t *CreateTopic) Code() Code {
	return CreateTopicCode
}

func (t *CreateTopic) MarshalBinary() ([]byte, error) {
	streamIdBytes, err := t.StreamId.MarshalBinary()
	if err != nil {
		return nil, err
	}
	nameBytes := []byte(t.Name)
	options, err := t.options()
	if err != nil {
		return nil, err
	}
	optionsBytes, err := encodeOptions(options)
	if err != nil {
		return nil, err
	}
	partitionsCount := uint32(defaultPartitionsCount)
	if t.Options.PartitionsCount != nil {
		partitionsCount = *t.Options.PartitionsCount
	}

	bytes := make([]byte, 0, len(streamIdBytes)+4+1+len(nameBytes)+len(optionsBytes))
	bytes = append(bytes, streamIdBytes...)
	bytes = binary.LittleEndian.AppendUint32(bytes, partitionsCount)
	bytes = append(bytes, byte(len(nameBytes)))
	bytes = append(bytes, nameBytes...)
	bytes = append(bytes, optionsBytes...)

	return bytes, nil
}

// options builds the trailing options block. partitions_count is not an
// option: it fills the command's own fixed field. Keys carrying the
// server-default sentinel (expiry 0, size 0, compression none) are omitted so
// the server derives them and returns them as derived entries. A geo topic's
// durations and the read cache sizes have no such sentinel, so a zero one is
// sent. Both durability keys are always sent, replicated unless named
// otherwise.
func (t *CreateTopic) options() ([]iggcon.HeaderEntry, error) {
	typed, err := t.typedOptions()
	if err != nil {
		return nil, err
	}
	merged, err := mergeOptions(typed, t.Options.Raw)
	if err != nil {
		return nil, err
	}
	for _, key := range []iggcon.TopicOptionKey{iggcon.TopicOptionDurability, iggcon.TopicOptionConsumerOffsetDurability} {
		index := slices.IndexFunc(merged, func(entry iggcon.HeaderEntry) bool { return string(entry.Key.Value) == string(key) })
		if index < 0 {
			merged = append(merged, stringOption(string(key), string(iggcon.DurabilityReplicated)))
			continue
		}
		value := merged[index].Value
		if value.Kind != iggcon.String || string(value.Value) != string(iggcon.DurabilityReplicated) && string(value.Value) != string(iggcon.DurabilityPersisted) {
			return nil, ierror.InvalidOptionValue{Key: string(key)}
		}
	}
	return merged, nil
}

func (t *CreateTopic) typedOptions() ([]iggcon.HeaderEntry, error) {
	options := t.Options
	var typed []iggcon.HeaderEntry
	if options.CompressionAlgorithm != nil {
		compression, err := options.CompressionAlgorithm.OptionValue()
		if err != nil {
			return nil, ierror.InvalidOptionValue{Key: string(iggcon.TopicOptionCompressionAlgorithm)}
		}
		if compression != "" {
			typed = append(typed, stringOption(string(iggcon.TopicOptionCompressionAlgorithm), compression))
		}
	}
	if options.MessageExpiry != nil && *options.MessageExpiry != iggcon.IggyExpiryServerDefault {
		typed = append(typed, uint64Option(string(iggcon.TopicOptionMessageExpiry), uint64(*options.MessageExpiry)))
	}
	if options.MaxTopicSize != nil && *options.MaxTopicSize != 0 {
		typed = append(typed, uint64Option(string(iggcon.TopicOptionMaxTopicSize), *options.MaxTopicSize))
	}
	if options.SegmentSize != nil {
		typed = append(typed, iggcon.SegmentSizeOption(*options.SegmentSize))
	}
	if options.Durability != nil {
		typed = append(typed, iggcon.DurabilityOption(*options.Durability))
	}
	if options.ConsumerOffsetDurability != nil {
		typed = append(typed, iggcon.ConsumerOffsetDurabilityOption(*options.ConsumerOffsetDurability))
	}
	if options.PartitionResizePolicy != nil {
		typed = append(typed, iggcon.PartitionResizePolicyOption(*options.PartitionResizePolicy))
	}
	if options.MessagesRequiredToSave != nil {
		typed = append(typed, iggcon.MessagesRequiredToSaveOption(*options.MessagesRequiredToSave))
	}
	if options.SizeOfMessagesRequiredToSave != nil {
		typed = append(typed, iggcon.SizeOfMessagesRequiredToSaveOption(*options.SizeOfMessagesRequiredToSave))
	}
	if options.PreallocateSegments != nil {
		typed = append(typed, iggcon.PreallocateSegmentsOption(*options.PreallocateSegments))
	}
	if options.ReplicationFactor != nil {
		typed = append(typed, iggcon.ReplicationFactorOption(*options.ReplicationFactor))
	}
	if options.SegmentMaxAge != nil && *options.SegmentMaxAge != iggcon.IggyExpiryServerDefault {
		typed = append(typed, uint64Option(string(iggcon.TopicOptionSegmentMaxAge), uint64(*options.SegmentMaxAge)))
	}
	if options.TieredStorage != nil {
		typed = append(typed, iggcon.TieredStorageOption(*options.TieredStorage))
	}
	if options.LocalRetentionAge != nil && *options.LocalRetentionAge != iggcon.IggyExpiryServerDefault {
		typed = append(typed, uint64Option(string(iggcon.TopicOptionLocalRetentionAge), uint64(*options.LocalRetentionAge)))
	}
	if options.LocalRetentionSize != nil && *options.LocalRetentionSize != 0 {
		typed = append(typed, uint64Option(string(iggcon.TopicOptionLocalRetentionSize), *options.LocalRetentionSize))
	}
	if options.TopicType != nil {
		typed = append(typed, iggcon.TopicTypeOption(*options.TopicType))
	}
	if options.GeoMode != nil {
		typed = append(typed, iggcon.GeoModeOption(*options.GeoMode))
	}
	typed = appendGeoDurations(typed,
		geoDuration{iggcon.TopicOptionFreshUploadInterval, options.FreshUploadInterval},
		geoDuration{iggcon.TopicOptionConsumerOffsetUploadInterval, options.ConsumerOffsetUploadInterval},
		geoDuration{iggcon.TopicOptionPositionsPublishInterval, options.PositionsPublishInterval},
		geoDuration{iggcon.TopicOptionTombstoneGrace, options.TombstoneGrace},
		geoDuration{iggcon.TopicOptionPositionsResumeTimeout, options.PositionsResumeTimeout},
		geoDuration{iggcon.TopicOptionLongTermRewriteMaxInterval, options.LongTermRewriteMaxInterval},
	)
	if options.ReadCacheSize != nil {
		typed = append(typed, uint64Option(string(iggcon.TopicOptionReadCacheSize), *options.ReadCacheSize))
	}
	if options.ReadAhead != nil {
		typed = append(typed, uint64Option(string(iggcon.TopicOptionReadAhead), *options.ReadAhead))
	}
	return typed, nil
}

// geoDuration is one duration of a geo topic with the key it travels under.
type geoDuration struct {
	key   iggcon.TopicOptionKey
	value *iggcon.Duration
}

// appendGeoDurations appends the durations that are set, in microseconds.
// Zero is a value of these keys, not the server-default sentinel of an
// expiry, so it is sent.
func appendGeoDurations(typed []iggcon.HeaderEntry, durations ...geoDuration) []iggcon.HeaderEntry {
	for _, duration := range durations {
		if duration.value != nil {
			typed = append(typed, uint64Option(string(duration.key), uint64(*duration.value)))
		}
	}
	return typed
}

type GetTopic struct {
	StreamId iggcon.Identifier
	TopicId  iggcon.Identifier
}

func (g *GetTopic) Code() Code {
	return GetTopicCode
}

func (g *GetTopic) MarshalBinary() ([]byte, error) {
	return iggcon.MarshalIdentifiers(g.StreamId, g.TopicId)
}

// GetTopics asks for one page of the topics of a stream.
type GetTopics struct {
	StreamId iggcon.Identifier
	Query    iggcon.NamedPageQuery
}

func (g *GetTopics) Code() Code {
	return GetTopicsCode
}

func (g *GetTopics) MarshalBinary() ([]byte, error) {
	streamId, err := g.StreamId.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return g.Query.AppendBinary(streamId)
}

type DeleteTopic struct {
	StreamId iggcon.Identifier
	TopicId  iggcon.Identifier
}

func (d *DeleteTopic) Code() Code {
	return DeleteTopicCode
}

func (d *DeleteTopic) MarshalBinary() ([]byte, error) {
	return iggcon.MarshalIdentifiers(d.StreamId, d.TopicId)
}

// UpdateTopic patches a topic through the generic UpdateResource command.
// Create-only settings, compression included, have no field here.
type UpdateTopic struct {
	StreamId                     iggcon.Identifier
	TopicId                      iggcon.Identifier
	Name                         *string
	MessageExpiry                *iggcon.Duration
	MaxTopicSize                 *uint64
	TieredStorage                *bool
	LocalRetentionAge            *iggcon.Duration
	LocalRetentionSize           *uint64
	FreshUploadInterval          *iggcon.Duration
	ConsumerOffsetUploadInterval *iggcon.Duration
	PositionsPublishInterval     *iggcon.Duration
	TombstoneGrace               *iggcon.Duration
	PositionsResumeTimeout       *iggcon.Duration
	LongTermRewriteMaxInterval   *iggcon.Duration
	ReadCacheSize                *uint64
	ReadAhead                    *uint64
	Reset                        []iggcon.TopicOptionKey
	RawReset                     []string
	Options                      []iggcon.HeaderEntry
}

func (u *UpdateTopic) Code() Code {
	return UpdateResourceCode
}

// MarshalBinary encodes the patch. A zero expiry, retention or size is the
// server-default sentinel and changes nothing, Reset returns a key to its
// default. A zero duration of a geo topic or read cache size is a value and is
// sent.
func (u *UpdateTopic) MarshalBinary() ([]byte, error) {
	var typed []iggcon.HeaderEntry
	if u.Name != nil {
		typed = append(typed, textOption(string(iggcon.TopicOptionName), *u.Name))
	}
	if u.MessageExpiry != nil && *u.MessageExpiry != iggcon.IggyExpiryServerDefault {
		typed = append(typed, uint64Option(string(iggcon.TopicOptionMessageExpiry), uint64(*u.MessageExpiry)))
	}
	if u.MaxTopicSize != nil && *u.MaxTopicSize != 0 {
		typed = append(typed, uint64Option(string(iggcon.TopicOptionMaxTopicSize), *u.MaxTopicSize))
	}
	if u.TieredStorage != nil {
		typed = append(typed, iggcon.TieredStorageOption(*u.TieredStorage))
	}
	if u.LocalRetentionAge != nil && *u.LocalRetentionAge != iggcon.IggyExpiryServerDefault {
		typed = append(typed, uint64Option(string(iggcon.TopicOptionLocalRetentionAge), uint64(*u.LocalRetentionAge)))
	}
	if u.LocalRetentionSize != nil && *u.LocalRetentionSize != 0 {
		typed = append(typed, uint64Option(string(iggcon.TopicOptionLocalRetentionSize), *u.LocalRetentionSize))
	}
	typed = appendGeoDurations(typed,
		geoDuration{iggcon.TopicOptionFreshUploadInterval, u.FreshUploadInterval},
		geoDuration{iggcon.TopicOptionConsumerOffsetUploadInterval, u.ConsumerOffsetUploadInterval},
		geoDuration{iggcon.TopicOptionPositionsPublishInterval, u.PositionsPublishInterval},
		geoDuration{iggcon.TopicOptionTombstoneGrace, u.TombstoneGrace},
		geoDuration{iggcon.TopicOptionPositionsResumeTimeout, u.PositionsResumeTimeout},
		geoDuration{iggcon.TopicOptionLongTermRewriteMaxInterval, u.LongTermRewriteMaxInterval},
	)
	if u.ReadCacheSize != nil {
		typed = append(typed, uint64Option(string(iggcon.TopicOptionReadCacheSize), *u.ReadCacheSize))
	}
	if u.ReadAhead != nil {
		typed = append(typed, uint64Option(string(iggcon.TopicOptionReadAhead), *u.ReadAhead))
	}
	reset := make([]string, 0, len(u.Reset))
	for _, key := range u.Reset {
		reset = append(reset, string(key))
	}
	return encodeUpdateResource(iggcon.OptionsScopeTopic, []iggcon.Identifier{u.StreamId, u.TopicId}, typed, u.Options, reset, u.RawReset)
}
