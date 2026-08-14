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

	"github.com/apache/iggy/foreign/go/contracts"
)

const (
	topicOptionCompressionAlgorithm = "compression_algorithm"
	topicOptionMessageExpiry        = "message_expiry"
	topicOptionMaxTopicSize         = "max_topic_size"
)

type CreateTopic struct {
	StreamId             iggcon.Identifier           `json:"streamId"`
	PartitionsCount      uint32                      `json:"partitionsCount"`
	CompressionAlgorithm iggcon.CompressionAlgorithm `json:"compressionAlgorithm"`
	MessageExpiry        iggcon.Duration             `json:"messageExpiry"`
	MaxTopicSize         uint64                      `json:"maxTopicSize"`
	Name                 string                      `json:"name"`
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
	optionsBytes := iggcon.GetHeadersBytes(t.options())

	bytes := make([]byte, 0, len(streamIdBytes)+4+1+len(nameBytes)+len(optionsBytes))
	bytes = append(bytes, streamIdBytes...)
	bytes = binary.LittleEndian.AppendUint32(bytes, t.PartitionsCount)
	bytes = append(bytes, byte(len(nameBytes)))
	bytes = append(bytes, nameBytes...)
	bytes = append(bytes, optionsBytes...)

	return bytes, nil
}

// options builds the trailing options block. partitions_count is not an
// option: it fills the command's own fixed field. Keys carrying the
// server-default sentinel (expiry 0, size 0, compression none) are omitted so
// the server derives them and returns them as derived entries.
func (t *CreateTopic) options() []iggcon.HeaderEntry {
	var options []iggcon.HeaderEntry
	if name := t.CompressionAlgorithm.String(); name != "none" {
		options = append(options, stringOption(topicOptionCompressionAlgorithm, name))
	}
	if t.MessageExpiry != 0 {
		options = append(options, uint64Option(topicOptionMessageExpiry, uint64(t.MessageExpiry)))
	}
	if t.MaxTopicSize != 0 {
		options = append(options, uint64Option(topicOptionMaxTopicSize, t.MaxTopicSize))
	}
	return options
}

func uint64Option(key string, value uint64) iggcon.HeaderEntry {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, value)
	return iggcon.HeaderEntry{
		Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte(key)},
		Value: iggcon.HeaderValue{Kind: iggcon.Uint64, Value: buf},
	}
}

func stringOption(key string, value string) iggcon.HeaderEntry {
	return iggcon.HeaderEntry{
		Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte(key)},
		Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte(value)},
	}
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

type UpdateTopic struct {
	StreamId iggcon.Identifier `json:"streamId"`
	TopicId  iggcon.Identifier `json:"topicId"`
	Name     string            `json:"name"`
	// Settings ride the options block. A zero value means "leave it alone",
	// so a rename does not silently reset the rest.
	CompressionAlgorithm iggcon.CompressionAlgorithm `json:"compressionAlgorithm"`
	MessageExpiry        iggcon.Duration             `json:"messageExpiry"`
	MaxTopicSize         uint64                      `json:"maxTopicSize"`
}

func (u *UpdateTopic) Code() Code {
	return UpdateTopicCode
}

// options builds the trailing options block. Only keys an update may change
// are allowed; the server rejects the create-time knobs by name.
// options builds the trailing options block. A zero value means the caller did
// not set the key, so it is omitted and the server leaves the topic's current
// value alone.
func (u *UpdateTopic) options() []iggcon.HeaderEntry {
	var options []iggcon.HeaderEntry
	if name := u.CompressionAlgorithm.String(); name != "none" {
		options = append(options, stringOption(topicOptionCompressionAlgorithm, name))
	}
	if u.MessageExpiry != 0 {
		options = append(options, uint64Option(topicOptionMessageExpiry, uint64(u.MessageExpiry)))
	}
	if u.MaxTopicSize != 0 {
		options = append(options, uint64Option(topicOptionMaxTopicSize, u.MaxTopicSize))
	}
	return options
}

func (u *UpdateTopic) MarshalBinary() ([]byte, error) {
	streamIdBytes, err := u.StreamId.MarshalBinary()
	if err != nil {
		return nil, err
	}
	topicIdBytes, err := u.TopicId.MarshalBinary()
	if err != nil {
		return nil, err
	}

	optionsBytes := iggcon.GetHeadersBytes(u.options())
	buffer := make([]byte, 1+len(streamIdBytes)+len(topicIdBytes)+len(u.Name)+len(optionsBytes))

	offset := 0

	offset += copy(buffer[offset:], streamIdBytes)
	offset += copy(buffer[offset:], topicIdBytes)

	buffer[offset] = uint8(len(u.Name))
	offset++

	offset += copy(buffer[offset:], u.Name)
	copy(buffer[offset:], optionsBytes)

	return buffer, nil
}
