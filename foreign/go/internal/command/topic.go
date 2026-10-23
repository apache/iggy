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
// the server derives them and returns them as derived entries. Both
// durability keys are always sent, replicated unless named otherwise.
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
	return typed, nil
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

type GetTopics struct {
	StreamId iggcon.Identifier
}

func (g *GetTopics) Code() Code {
	return GetTopicsCode
}

func (g *GetTopics) MarshalBinary() ([]byte, error) {
	return g.StreamId.MarshalBinary()
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
	StreamId      iggcon.Identifier
	TopicId       iggcon.Identifier
	Name          *string
	MessageExpiry *iggcon.Duration
	MaxTopicSize  *uint64
	Reset         []iggcon.TopicOptionKey
	RawReset      []string
	Options       []iggcon.HeaderEntry
}

func (u *UpdateTopic) Code() Code {
	return UpdateResourceCode
}

// MarshalBinary encodes the patch. A zero expiry or size is the server-default
// sentinel and changes nothing, Reset returns a key to its default.
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
	reset := make([]string, 0, len(u.Reset))
	for _, key := range u.Reset {
		reset = append(reset, string(key))
	}
	return encodeUpdateResource(iggcon.OptionsScopeTopic, []iggcon.Identifier{u.StreamId, u.TopicId}, typed, u.Options, reset, u.RawReset)
}
