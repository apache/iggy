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

package main

// The canonical result forms of core/conformance/src/canonical.rs. The
// orchestrator decodes each result into its form and refuses any field that
// the form does not have.

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
	"unicode/utf8"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

type optionEntry struct {
	Value    string `json:"value"`
	Kind     string `json:"kind"`
	Explicit bool   `json:"explicit"`
}

type identityResult struct {
	UserID uint32 `json:"user_id"`
}

type streamResult struct {
	ID            uint32                 `json:"id"`
	Name          string                 `json:"name"`
	CreatedAt     uint64                 `json:"created_at"`
	SizeBytes     uint64                 `json:"size_bytes"`
	MessagesCount uint64                 `json:"messages_count"`
	TopicsCount   uint32                 `json:"topics_count"`
	Options       map[string]optionEntry `json:"options"`
}

type streamDetailsResult struct {
	ID            uint32                 `json:"id"`
	Name          string                 `json:"name"`
	CreatedAt     uint64                 `json:"created_at"`
	SizeBytes     uint64                 `json:"size_bytes"`
	MessagesCount uint64                 `json:"messages_count"`
	TopicsCount   uint32                 `json:"topics_count"`
	Topics        []topicResult          `json:"topics"`
	Options       map[string]optionEntry `json:"options"`
}

type topicResult struct {
	ID              uint32                 `json:"id"`
	Name            string                 `json:"name"`
	CreatedAt       uint64                 `json:"created_at"`
	SizeBytes       uint64                 `json:"size_bytes"`
	MessagesCount   uint64                 `json:"messages_count"`
	PartitionsCount uint32                 `json:"partitions_count"`
	Options         map[string]optionEntry `json:"options"`
}

type topicDetailsResult struct {
	ID              uint32                 `json:"id"`
	Name            string                 `json:"name"`
	CreatedAt       uint64                 `json:"created_at"`
	SizeBytes       uint64                 `json:"size_bytes"`
	MessagesCount   uint64                 `json:"messages_count"`
	PartitionsCount uint32                 `json:"partitions_count"`
	Partitions      []partitionResult      `json:"partitions"`
	Options         map[string]optionEntry `json:"options"`
}

type partitionResult struct {
	ID            uint32 `json:"id"`
	CreatedAt     uint64 `json:"created_at"`
	SegmentsCount uint32 `json:"segments_count"`
	CurrentOffset uint64 `json:"current_offset"`
	SizeBytes     uint64 `json:"size_bytes"`
	MessagesCount uint64 `json:"messages_count"`
}

type polledMessagesResult struct {
	PartitionID   uint32          `json:"partition_id"`
	CurrentOffset uint64          `json:"current_offset"`
	Count         uint32          `json:"count"`
	Messages      []messageResult `json:"messages"`
}

type headerEntry struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

type messageResult struct {
	Offset          uint64                 `json:"offset"`
	ID              string                 `json:"id"`
	Timestamp       uint64                 `json:"timestamp"`
	OriginTimestamp uint64                 `json:"origin_timestamp"`
	Checksum        uint64                 `json:"checksum"`
	Payload         *string                `json:"payload,omitempty"`
	PayloadHex      string                 `json:"payload_hex"`
	Headers         map[string]headerEntry `json:"headers,omitempty"`
}

type consumerGroupResult struct {
	ID              uint32         `json:"id"`
	Name            string         `json:"name"`
	PartitionsCount uint32         `json:"partitions_count"`
	MembersCount    uint32         `json:"members_count"`
	Members         []memberResult `json:"members"`
}

type memberResult struct {
	ID              uint32   `json:"id"`
	PartitionsCount uint32   `json:"partitions_count"`
	Partitions      []uint32 `json:"partitions"`
}

type rawResult struct {
	PayloadHex string `json:"payload_hex"`
}

type optionSpecResult struct {
	Key        string `json:"key"`
	Kind       string `json:"kind"`
	Updatable  bool   `json:"updatable"`
	Resettable bool   `json:"resettable"`
	Secret     bool   `json:"secret"`
	ReadOnly   bool   `json:"read_only"`
	Critical   bool   `json:"critical"`
}

type userDetailsResult struct {
	ID          uint32                 `json:"id"`
	Username    string                 `json:"username"`
	Status      string                 `json:"status"`
	CreatedAt   uint64                 `json:"created_at"`
	Permissions *permissionsResult     `json:"permissions"`
	Options     map[string]optionEntry `json:"options"`
}

type permissionsResult struct {
	Global map[string]bool `json:"global"`
}

type receivedMessageResult struct {
	PartitionID uint32        `json:"partition_id"`
	Message     messageResult `json:"message"`
}

type connectionInfoResult struct {
	ServerAddress string `json:"server_address"`
}

func canonicalUser(user *iggcon.UserInfoDetails) (userDetailsResult, error) {
	options, err := canonicalOptions(user.Options, user.DerivedOptions)
	if err != nil {
		return userDetailsResult{}, err
	}
	converted := userDetailsResult{
		ID:        user.Id,
		Username:  user.Username,
		Status:    statusName(user.Status),
		CreatedAt: user.CreatedAt,
		Options:   options,
	}
	if user.Permissions != nil {
		converted.Permissions = &permissionsResult{Global: globalView(user.Permissions.Global)}
	}
	return converted, nil
}

func canonicalOptionSpec(spec iggcon.OptionSpec) optionSpecResult {
	return optionSpecResult{
		Key:        spec.Key,
		Kind:       kindNames[spec.DefaultValue.Kind],
		Updatable:  spec.Updatable,
		Resettable: spec.Resettable,
		Secret:     spec.Secret,
		ReadOnly:   spec.ReadOnly,
		Critical:   spec.Critical,
	}
}

func canonicalStream(stream iggcon.Stream) (streamResult, error) {
	options, err := canonicalOptions(stream.Options, stream.DerivedOptions)
	return streamResult{
		ID:            stream.Id,
		Name:          stream.Name,
		CreatedAt:     stream.CreatedAt,
		SizeBytes:     stream.SizeBytes,
		MessagesCount: stream.MessagesCount,
		TopicsCount:   stream.TopicsCount,
		Options:       options,
	}, err
}

func canonicalStreamDetails(stream *iggcon.StreamDetails) (streamDetailsResult, error) {
	base, err := canonicalStream(stream.Stream)
	if err != nil {
		return streamDetailsResult{}, err
	}
	topics := make([]topicResult, 0, len(stream.Topics))
	for _, topic := range stream.Topics {
		converted, err := canonicalTopic(topic)
		if err != nil {
			return streamDetailsResult{}, err
		}
		topics = append(topics, converted)
	}
	return streamDetailsResult{
		ID:            base.ID,
		Name:          base.Name,
		CreatedAt:     base.CreatedAt,
		SizeBytes:     base.SizeBytes,
		MessagesCount: base.MessagesCount,
		TopicsCount:   base.TopicsCount,
		Topics:        topics,
		Options:       base.Options,
	}, nil
}

func canonicalTopic(topic iggcon.Topic) (topicResult, error) {
	options, err := canonicalOptions(topic.Options, topic.DerivedOptions)
	return topicResult{
		ID:              topic.Id,
		Name:            topic.Name,
		CreatedAt:       topic.CreatedAt,
		SizeBytes:       topic.Size,
		MessagesCount:   topic.MessagesCount,
		PartitionsCount: topic.PartitionsCount,
		Options:         options,
	}, err
}

func canonicalTopicDetails(topic *iggcon.TopicDetails) (topicDetailsResult, error) {
	base, err := canonicalTopic(topic.Topic)
	if err != nil {
		return topicDetailsResult{}, err
	}
	partitions := make([]partitionResult, 0, len(topic.Partitions))
	for _, partition := range topic.Partitions {
		partitions = append(partitions, partitionResult{
			ID:            partition.Id,
			CreatedAt:     partition.CreatedAt,
			SegmentsCount: partition.SegmentsCount,
			CurrentOffset: partition.CurrentOffset,
			SizeBytes:     partition.SizeBytes,
			MessagesCount: partition.MessagesCount,
		})
	}
	return topicDetailsResult{
		ID:              base.ID,
		Name:            base.Name,
		CreatedAt:       base.CreatedAt,
		SizeBytes:       base.SizeBytes,
		MessagesCount:   base.MessagesCount,
		PartitionsCount: base.PartitionsCount,
		Partitions:      partitions,
		Options:         base.Options,
	}, nil
}

func canonicalPolled(polled *iggcon.PolledMessage) (polledMessagesResult, error) {
	messages := make([]messageResult, 0, len(polled.Messages))
	for _, message := range polled.Messages {
		converted, err := canonicalMessage(message)
		if err != nil {
			return polledMessagesResult{}, err
		}
		messages = append(messages, converted)
	}
	return polledMessagesResult{
		PartitionID:   polled.PartitionId,
		CurrentOffset: polled.CurrentOffset,
		Count:         polled.MessageCount,
		Messages:      messages,
	}, nil
}

func canonicalMessage(message iggcon.IggyMessage) (messageResult, error) {
	converted := messageResult{
		Offset:          message.Header.Offset,
		ID:              messageIDText(message.Header.Id),
		Timestamp:       message.Header.Timestamp,
		OriginTimestamp: message.Header.OriginTimestamp,
		Checksum:        message.Header.Checksum,
		PayloadHex:      hex.EncodeToString(message.Payload),
	}
	if utf8.Valid(message.Payload) {
		text := string(message.Payload)
		converted.Payload = &text
	}
	if len(message.UserHeaders) > 0 {
		entries, err := iggcon.DeserializeHeaders(message.UserHeaders)
		if err != nil {
			return messageResult{}, err
		}
		converted.Headers = map[string]headerEntry{}
		for _, entry := range entries {
			kind, value, err := valueText(entry.Value)
			if err != nil {
				return messageResult{}, err
			}
			converted.Headers[string(entry.Key.Value)] = headerEntry{Kind: kind, Value: value}
		}
	}
	return converted, nil
}

func canonicalGroup(group *iggcon.ConsumerGroupDetails) consumerGroupResult {
	members := make([]memberResult, 0, len(group.Members))
	for _, member := range group.Members {
		partitions := member.Partitions
		if partitions == nil {
			partitions = []uint32{}
		}
		members = append(members, memberResult{
			ID:              member.ID,
			PartitionsCount: member.PartitionsCount,
			Partitions:      partitions,
		})
	}
	return consumerGroupResult{
		ID:              group.Id,
		Name:            group.Name,
		PartitionsCount: group.PartitionsCount,
		MembersCount:    group.MembersCount,
		Members:         members,
	}
}

// canonicalOptions joins the keys that the client set and the keys that the
// server derived into one map with the explicit flag of each.
func canonicalOptions(explicit, derived map[string]iggcon.HeaderValue) (map[string]optionEntry, error) {
	options := map[string]optionEntry{}
	add := func(values map[string]iggcon.HeaderValue, isExplicit bool) error {
		for key, value := range values {
			kind, text, err := valueText(value)
			if err != nil {
				return fmt.Errorf("option %s: %w", key, err)
			}
			options[key] = optionEntry{Value: text, Kind: kind, Explicit: isExplicit}
		}
		return nil
	}
	if err := add(derived, false); err != nil {
		return nil, err
	}
	if err := add(explicit, true); err != nil {
		return nil, err
	}
	return options, nil
}

var kindNames = map[iggcon.HeaderKind]string{
	iggcon.Raw: "raw", iggcon.String: "string", iggcon.Bool: "bool",
	iggcon.Int8: "int8", iggcon.Int16: "int16", iggcon.Int32: "int32", iggcon.Int64: "int64",
	iggcon.Int128: "int128", iggcon.Uint8: "uint8", iggcon.Uint16: "uint16", iggcon.Uint32: "uint32",
	iggcon.Uint64: "uint64", iggcon.Uint128: "uint128", iggcon.Float: "float32", iggcon.Double: "float64",
}

// valueText is the reversible text of a value: UTF-8 for strings, true or
// false, base-10 integers, padded standard base64 for raw bytes, and 0x with
// the IEEE-754 bits in fixed-width lowercase hex for floats.
func valueText(value iggcon.HeaderValue) (string, string, error) {
	kind, known := kindNames[value.Kind]
	if !known {
		return "", "", fmt.Errorf("unknown header kind %d", value.Kind)
	}
	bytes := value.Value
	if size := value.Kind.ExpectedSize(); size > 0 && len(bytes) != size {
		return "", "", fmt.Errorf("a %s value of %d bytes", kind, len(bytes))
	}
	switch value.Kind {
	case iggcon.Raw:
		return kind, base64.StdEncoding.EncodeToString(bytes), nil
	case iggcon.String:
		return kind, string(bytes), nil
	case iggcon.Bool:
		return kind, strconv.FormatBool(bytes[0] == 1), nil
	case iggcon.Int8:
		return kind, strconv.FormatInt(int64(int8(bytes[0])), 10), nil
	case iggcon.Int16:
		return kind, strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(bytes))), 10), nil
	case iggcon.Int32:
		return kind, strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(bytes))), 10), nil
	case iggcon.Int64:
		return kind, strconv.FormatInt(int64(binary.LittleEndian.Uint64(bytes)), 10), nil
	case iggcon.Uint8:
		return kind, strconv.FormatUint(uint64(bytes[0]), 10), nil
	case iggcon.Uint16:
		return kind, strconv.FormatUint(uint64(binary.LittleEndian.Uint16(bytes)), 10), nil
	case iggcon.Uint32:
		return kind, strconv.FormatUint(uint64(binary.LittleEndian.Uint32(bytes)), 10), nil
	case iggcon.Uint64:
		return kind, strconv.FormatUint(binary.LittleEndian.Uint64(bytes), 10), nil
	case iggcon.Int128, iggcon.Uint128:
		return kind, littleEndian128(bytes, value.Kind == iggcon.Int128).String(), nil
	case iggcon.Float:
		return kind, fmt.Sprintf("0x%08x", binary.LittleEndian.Uint32(bytes)), nil
	default:
		return kind, fmt.Sprintf("0x%016x", binary.LittleEndian.Uint64(bytes)), nil
	}
}

// valueFromText reverses valueText.
func valueFromText(kindName, text string) (iggcon.HeaderValue, error) {
	var kind iggcon.HeaderKind
	for candidate, name := range kindNames {
		if name == kindName {
			kind = candidate
		}
	}
	value := iggcon.HeaderValue{Kind: kind}
	switch kind {
	case iggcon.Raw:
		decoded, err := base64.StdEncoding.DecodeString(text)
		value.Value = decoded
		return value, err
	case iggcon.String:
		value.Value = []byte(text)
		return value, nil
	case iggcon.Bool:
		parsed, err := strconv.ParseBool(text)
		value.Value = []byte{0}
		if parsed {
			value.Value[0] = 1
		}
		return value, err
	case iggcon.Int8, iggcon.Int16, iggcon.Int32, iggcon.Int64:
		parsed, err := strconv.ParseInt(text, 10, kind.ExpectedSize()*8)
		value.Value = littleEndianBytes(uint64(parsed), kind.ExpectedSize())
		return value, err
	case iggcon.Uint8, iggcon.Uint16, iggcon.Uint32, iggcon.Uint64:
		parsed, err := strconv.ParseUint(text, 10, kind.ExpectedSize()*8)
		value.Value = littleEndianBytes(parsed, kind.ExpectedSize())
		return value, err
	case iggcon.Int128, iggcon.Uint128:
		number, ok := new(big.Int).SetString(text, 10)
		if !ok {
			return value, fmt.Errorf("%q is not a %s", text, kindName)
		}
		value.Value = bytes128(number)
		return value, nil
	case iggcon.Float, iggcon.Double:
		if len(text) < 3 || text[:2] != "0x" {
			return value, fmt.Errorf("%q is not 0x and IEEE-754 bits", text)
		}
		bits, err := strconv.ParseUint(text[2:], 16, kind.ExpectedSize()*8)
		value.Value = littleEndianBytes(bits, kind.ExpectedSize())
		return value, err
	default:
		return value, fmt.Errorf("unknown header kind %q", kindName)
	}
}

func headerEntries(headers map[string]headerArg) ([]iggcon.HeaderEntry, error) {
	entries := make([]iggcon.HeaderEntry, 0, len(headers))
	for key, header := range headers {
		headerKey, err := iggcon.NewHeaderKeyString(key)
		if err != nil {
			return nil, err
		}
		value, err := valueFromText(header.Kind, header.Value)
		if err != nil {
			return nil, err
		}
		entries = append(entries, iggcon.HeaderEntry{Key: headerKey, Value: value})
	}
	return entries, nil
}

// messageIDText is the u128 id as decimal text. The id bytes are little endian,
// as on the wire.
func messageIDText(id iggcon.MessageID) string {
	return littleEndian128(id[:], false).String()
}

func messageIDFromText(text string) (iggcon.MessageID, error) {
	var id iggcon.MessageID
	number, ok := new(big.Int).SetString(text, 10)
	if !ok || number.Sign() < 0 || number.BitLen() > 128 {
		return id, fmt.Errorf("%q is not a u128", text)
	}
	copy(id[:], bytes128(number))
	return id, nil
}

func littleEndian128(bytes []byte, signed bool) *big.Int {
	bigEndian := make([]byte, len(bytes))
	for index, value := range bytes {
		bigEndian[len(bytes)-1-index] = value
	}
	number := new(big.Int).SetBytes(bigEndian)
	if signed && len(bytes) > 0 && bytes[len(bytes)-1]&0x80 != 0 {
		number.Sub(number, new(big.Int).Lsh(big.NewInt(1), uint(len(bytes)*8)))
	}
	return number
}

func bytes128(number *big.Int) []byte {
	value := new(big.Int).Set(number)
	if value.Sign() < 0 {
		value.Add(value, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	bigEndian := value.FillBytes(make([]byte, 16))
	littleEndian := make([]byte, 16)
	for index, byteValue := range bigEndian {
		littleEndian[15-index] = byteValue
	}
	return littleEndian
}

func littleEndianBytes(value uint64, size int) []byte {
	all := make([]byte, 8)
	binary.LittleEndian.PutUint64(all, value)
	return all[:size]
}
