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

package binaryserialization

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/klauspost/compress/s2"
)

func DeserializeLogInResponse(payload []byte) *iggcon.IdentityInfo {
	userId := binary.LittleEndian.Uint32(payload[0:4])
	return &iggcon.IdentityInfo{
		UserId: userId,
	}
}

func DeserializeOffset(payload []byte) *iggcon.ConsumerOffsetInfo {
	if len(payload) == 0 {
		return nil
	}

	partitionId := binary.LittleEndian.Uint32(payload[0:4])
	currentOffset := binary.LittleEndian.Uint64(payload[4:12])
	storedOffset := binary.LittleEndian.Uint64(payload[12:20])

	return &iggcon.ConsumerOffsetInfo{
		PartitionId:   partitionId,
		CurrentOffset: currentOffset,
		StoredOffset:  storedOffset,
	}
}

func DeserializeStream(payload []byte) (*iggcon.StreamDetails, error) {
	stream, pos, err := DeserializeToStream(payload, 0)
	if err != nil {
		return nil, err
	}
	// Count-driven: a topic element carries variable-length options blocks,
	// so "consume until the buffer ends" no longer delimits it. The declared
	// count is an unvalidated wire u32, so the allocation hint is capped by
	// what the remaining body could possibly hold.
	topics := make([]iggcon.Topic, 0, boundedCapacity(stream.TopicsCount, len(payload)-pos, topicMinimumSize))
	for i := uint32(0); i < stream.TopicsCount; i++ {
		topic, readBytes, err := DeserializeToTopic(payload, pos)
		if err != nil {
			return nil, err
		}
		topics = append(topics, topic)
		pos += readBytes
	}

	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Id < topics[j].Id
	})

	return &iggcon.StreamDetails{
		Stream: stream,
		Topics: topics,
	}, nil
}

func DeserializeStreams(payload []byte) ([]iggcon.Stream, error) {
	streams := make([]iggcon.Stream, 0)
	position := 0

	for position < len(payload) {
		stream, readBytes, err := DeserializeToStream(payload, position)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize stream at offset %d: %w", position, err)
		}
		streams = append(streams, stream)
		position += readBytes
	}

	return streams, nil
}

const streamFixedSize = 4 + 8 + 4 + 8 + 8 + 1 // 33 bytes: id + created_at + topics_count + size_bytes + messages_count + name_len

func DeserializeToStream(payload []byte, position int) (iggcon.Stream, int, error) {
	remaining := len(payload) - position
	if remaining < streamFixedSize {
		return iggcon.Stream{}, 0, fmt.Errorf(
			"not enough data to read stream header: need %d bytes, got %d",
			streamFixedSize, remaining)
	}

	id := binary.LittleEndian.Uint32(payload[position : position+4])
	createdAt := binary.LittleEndian.Uint64(payload[position+4 : position+12])
	topicsCount := binary.LittleEndian.Uint32(payload[position+12 : position+16])
	sizeBytes := binary.LittleEndian.Uint64(payload[position+16 : position+24])
	messagesCount := binary.LittleEndian.Uint64(payload[position+24 : position+32])
	nameLength := int(payload[position+32])

	if remaining < streamFixedSize+nameLength {
		return iggcon.Stream{}, 0, fmt.Errorf(
			"not enough data to read stream name: need %d bytes, got %d",
			streamFixedSize+nameLength, remaining)
	}

	name := string(payload[position+33 : position+33+nameLength])

	options, optionsSize, err := deserializeOptions(payload, position+streamFixedSize+nameLength)
	if err != nil {
		return iggcon.Stream{}, 0, fmt.Errorf("failed to read stream options: %w", err)
	}

	return iggcon.Stream{
		Id:            id,
		TopicsCount:   topicsCount,
		Name:          name,
		SizeBytes:     sizeBytes,
		MessagesCount: messagesCount,
		CreatedAt:     createdAt,
		Options:       options,
	}, streamFixedSize + nameLength + optionsSize, nil
}

// deserializeOptions reads a u32-length-prefixed options TLV block at
// position and returns the decoded options with the total bytes consumed
// (prefix included). A zero-length block decodes to a nil map.
func deserializeOptions(payload []byte, position int) (map[string]iggcon.HeaderValue, int, error) {
	if len(payload) < position+4 {
		return nil, 0, errors.New("not enough data to read options length")
	}
	optionsLength := int(binary.LittleEndian.Uint32(payload[position : position+4]))
	if len(payload) < position+4+optionsLength {
		return nil, 0, fmt.Errorf(
			"not enough data to read options block: need %d bytes, got %d",
			optionsLength, len(payload)-position-4)
	}
	if optionsLength == 0 {
		return nil, 4, nil
	}

	entries, err := iggcon.DeserializeHeaders(payload[position+4 : position+4+optionsLength])
	if err != nil {
		return nil, 0, err
	}
	options := make(map[string]iggcon.HeaderValue, len(entries))
	for _, entry := range entries {
		options[string(entry.Key.Value)] = entry.Value
	}
	return options, 4 + optionsLength, nil
}

// pollBatchHeaderLength covers [partition_id u32][current_offset u64][count u32].
const pollBatchHeaderLength = 16

// DeserializeFetchMessagesResponse decodes a poll reply. A truncated body is
// a decode error rather than a shorter batch: silently dropping the tail
// would let a consumer that commits CurrentOffset skip messages it never saw.
// The returned messages alias the reply buffer; a retained message pins it.
func DeserializeFetchMessagesResponse(payload []byte, compression iggcon.IggyMessageCompression) (*iggcon.PolledMessage, error) {
	if len(payload) == 0 {
		return &iggcon.PolledMessage{
			PartitionId:   0,
			CurrentOffset: 0,
			Messages:      make([]iggcon.IggyMessage, 0),
		}, nil
	}

	length := len(payload)
	if length < pollBatchHeaderLength {
		return nil, fmt.Errorf("poll response: %d bytes is short of the batch header", length)
	}
	partitionId := binary.LittleEndian.Uint32(payload[0:4])
	currentOffset := binary.LittleEndian.Uint64(payload[4:12])
	messagesCount := binary.LittleEndian.Uint32(payload[12:16])
	position := pollBatchHeaderLength

	// The declared count is server-controlled; the allocation hint is capped
	// by what the body could possibly hold.
	maxMessages := (length - pollBatchHeaderLength) / iggcon.MessageHeaderSize
	if int(messagesCount) < maxMessages {
		maxMessages = int(messagesCount)
	}
	messages := make([]iggcon.IggyMessage, 0, maxMessages)
	for position < length {
		if position+iggcon.MessageHeaderSize > length {
			return nil, fmt.Errorf("poll response: truncated message header at byte %d", position)
		}
		header, err := iggcon.MessageHeaderFromBytes(payload[position : position+iggcon.MessageHeaderSize])
		if err != nil {
			return nil, err
		}
		position += iggcon.MessageHeaderSize
		if uint64(header.PayloadLength) > uint64(length-position) {
			return nil, fmt.Errorf(
				"poll response: message payload of %d bytes overruns the body", header.PayloadLength)
		}
		payloadSlice := payload[position : position+int(header.PayloadLength)]
		position += int(header.PayloadLength)

		if uint64(header.UserHeaderLength) > uint64(length-position) {
			return nil, fmt.Errorf(
				"poll response: user headers of %d bytes overrun the body", header.UserHeaderLength)
		}
		var userHeaders []byte
		if header.UserHeaderLength > 0 {
			userHeaders = payload[position : position+int(header.UserHeaderLength)]
		}
		position += int(header.UserHeaderLength)

		switch compression {
		case iggcon.MESSAGE_COMPRESSION_S2, iggcon.MESSAGE_COMPRESSION_S2_BETTER, iggcon.MESSAGE_COMPRESSION_S2_BEST:
			if length < 32 {
				break
			}
			payloadSlice, err = s2.Decode(nil, payloadSlice)
			if err != nil {
				return nil, fmt.Errorf("failed to decode s2 payload: %w", err)
			}
		}

		messages = append(messages, iggcon.IggyMessage{
			Header:      *header,
			Payload:     payloadSlice,
			UserHeaders: userHeaders,
		})
	}
	if uint32(len(messages)) != messagesCount {
		return nil, fmt.Errorf(
			"poll response: %d decoded messages do not match the declared %d",
			len(messages), messagesCount)
	}

	// !TODO: Add message offset ordering
	return &iggcon.PolledMessage{
		PartitionId:   partitionId,
		CurrentOffset: currentOffset,
		Messages:      messages,
		MessageCount:  messagesCount,
	}, nil
}

// optionSpecMinimumSize is the smallest catalog entry: a one-character name
// (a length byte plus at least one byte), a kind byte, and empty
// length-prefixed default and description.
const optionSpecMinimumSize = 2 + 1 + 4 + 4

// DeserializeOptionSpecs reads a DescribeOptions response.
//
// Wire format: [count:u32][ [key_len:u8][key][kind:u8][default_len:u32][default]
// [description_len:u32][description] ]*
func DeserializeOptionSpecs(payload []byte) ([]iggcon.OptionSpec, error) {
	if len(payload) < 4 {
		return nil, fmt.Errorf(
			"not enough data to read option count: need 4 bytes, got %d", len(payload))
	}
	count := binary.LittleEndian.Uint32(payload[0:4])
	position := 4

	specs := make([]iggcon.OptionSpec, 0,
		boundedCapacity(count, len(payload)-position, optionSpecMinimumSize))
	for i := uint32(0); i < count; i++ {
		if len(payload)-position < 1 {
			return nil, fmt.Errorf("truncated option key length at offset %d", position)
		}
		keyLength := int(payload[position])
		position++
		if len(payload)-position < keyLength+1 {
			return nil, fmt.Errorf("truncated option key at offset %d", position)
		}
		key := string(payload[position : position+keyLength])
		position += keyLength

		kind := payload[position]
		position++

		defaultValue, read, err := readLengthPrefixed(payload, position, "option default value")
		if err != nil {
			return nil, err
		}
		position += read

		description, read, err := readLengthPrefixed(payload, position, "option description")
		if err != nil {
			return nil, err
		}
		position += read

		specs = append(specs, iggcon.OptionSpec{
			Key:          key,
			DefaultValue: iggcon.HeaderValue{Kind: iggcon.HeaderKind(kind), Value: defaultValue},
			Description:  string(description),
		})
	}

	return specs, nil
}

func readLengthPrefixed(payload []byte, position int, field string) ([]byte, int, error) {
	if len(payload)-position < 4 {
		return nil, 0, fmt.Errorf("truncated length prefix for %s at offset %d", field, position)
	}
	length := int(binary.LittleEndian.Uint32(payload[position : position+4]))
	position += 4
	if length < 0 || len(payload)-position < length {
		return nil, 0, fmt.Errorf("truncated %s at offset %d", field, position)
	}
	return payload[position : position+length], 4 + length, nil
}

func DeserializeTopics(payload []byte) ([]iggcon.Topic, error) {
	if len(payload) < 4 {
		return nil, fmt.Errorf(
			"not enough data to read topics count: need 4 bytes, got %d", len(payload))
	}
	topicsCount := binary.LittleEndian.Uint32(payload[0:4])
	position := 4

	// The declared count is server-controlled; the allocation hint is capped
	// by what the body could possibly hold.
	topics := make([]iggcon.Topic, 0, boundedCapacity(topicsCount, len(payload)-position, topicMinimumSize))
	for i := uint32(0); i < topicsCount; i++ {
		topic, readBytes, err := DeserializeToTopic(payload, position)
		if err != nil {
			return nil, err
		}
		topics = append(topics, topic)
		position += readBytes
	}

	return topics, nil
}

func DeserializeTopic(payload []byte) (*iggcon.TopicDetails, error) {
	topic, position, err := DeserializeToTopic(payload, 0)
	if err != nil {
		return &iggcon.TopicDetails{}, err
	}

	partitions := make([]iggcon.PartitionContract, 0)
	length := len(payload)

	for position < length {
		partition, readBytes := DeserializePartition(payload, position)
		partitions = append(partitions, partition)
		position += readBytes
	}
	return &iggcon.TopicDetails{
		Topic:      topic,
		Partitions: partitions,
	}, nil
}

// topicFixedSize covers the fields before the name:
// id + created_at + partitions_count + message_expiry + compression +
// max_topic_size + size_bytes + messages_count + name_len.
const topicFixedSize = 4 + 8 + 4 + 8 + 1 + 8 + 8 + 8 + 1 // 50 bytes

// topicMinimumSize is the smallest possible topic element: fixed fields, a
// one-character name (the server rejects an empty one), and the two u32
// options-length prefixes (explicit and derived), both zero.
const topicMinimumSize = topicFixedSize + 1 + 4 + 4

// boundedCapacity caps a wire-declared element count by what the remaining
// bytes could possibly hold.
//
// The count is an unvalidated u32: at max it asks for a multi-hundred-gigabyte
// reservation, and a Go allocation failure cannot be recovered from.
func boundedCapacity(declared uint32, remaining int, minItemSize int) int {
	if remaining <= 0 || minItemSize <= 0 {
		return 0
	}
	capacity := remaining / minItemSize
	if uint64(declared) < uint64(capacity) {
		return int(declared)
	}
	return capacity
}

func DeserializeToTopic(payload []byte, position int) (iggcon.Topic, int, error) {
	remaining := len(payload) - position
	if remaining < topicFixedSize {
		return iggcon.Topic{}, 0, fmt.Errorf(
			"not enough data to read topic header: need %d bytes, got %d",
			topicFixedSize, remaining)
	}

	topic := iggcon.Topic{}
	topic.Id = binary.LittleEndian.Uint32(payload[position : position+4])
	topic.CreatedAt = binary.LittleEndian.Uint64(payload[position+4 : position+12])
	topic.PartitionsCount = binary.LittleEndian.Uint32(payload[position+12 : position+16])
	topic.MessageExpiry = iggcon.Duration(binary.LittleEndian.Uint64(payload[position+16 : position+24]))
	topic.CompressionAlgorithm = payload[position+24]
	topic.MaxTopicSize = binary.LittleEndian.Uint64(payload[position+25 : position+33])
	topic.Size = binary.LittleEndian.Uint64(payload[position+33 : position+41])
	topic.MessagesCount = binary.LittleEndian.Uint64(payload[position+41 : position+49])
	// Replication factor left the wire protocol together with the old fixed
	// layout; every topic reports the single-copy default.

	nameLength := int(payload[position+49])
	if remaining < topicFixedSize+nameLength {
		return iggcon.Topic{}, 0, fmt.Errorf(
			"not enough data to read topic name: need %d bytes, got %d",
			topicFixedSize+nameLength, remaining)
	}
	topic.Name = string(payload[position+50 : position+50+nameLength])

	readBytes := topicFixedSize + nameLength
	options, optionsSize, err := deserializeOptions(payload, position+readBytes)
	if err != nil {
		return iggcon.Topic{}, 0, fmt.Errorf("failed to read topic options: %w", err)
	}
	topic.Options = options
	readBytes += optionsSize

	derivedOptions, derivedSize, err := deserializeOptions(payload, position+readBytes)
	if err != nil {
		return iggcon.Topic{}, 0, fmt.Errorf("failed to read topic derived options: %w", err)
	}
	topic.DerivedOptions = derivedOptions
	readBytes += derivedSize

	return topic, readBytes, nil
}

func DeserializePartition(payload []byte, position int) (iggcon.PartitionContract, int) {
	id := binary.LittleEndian.Uint32(payload[position : position+4])
	createdAt := binary.LittleEndian.Uint64(payload[position+4 : position+12])
	segmentsCount := binary.LittleEndian.Uint32(payload[position+12 : position+16])
	currentOffset := binary.LittleEndian.Uint64(payload[position+16 : position+24])
	sizeBytes := binary.LittleEndian.Uint64(payload[position+24 : position+32])
	messagesCount := binary.LittleEndian.Uint64(payload[position+32 : position+40])
	readBytes := 4 + 4 + 8 + 8 + 8 + 8

	partition := iggcon.PartitionContract{
		Id:            id,
		CreatedAt:     createdAt,
		SegmentsCount: segmentsCount,
		CurrentOffset: currentOffset,
		SizeBytes:     sizeBytes,
		MessagesCount: messagesCount,
	}

	return partition, readBytes
}

func DeserializeConsumerGroups(payload []byte) []iggcon.ConsumerGroup {
	var consumerGroups []iggcon.ConsumerGroup
	length := len(payload)
	position := 0

	for position < length {
		// use slices
		consumerGroup, readBytes := DeserializeToConsumerGroup(payload, position)
		consumerGroups = append(consumerGroups, *consumerGroup)
		position += readBytes
	}

	return consumerGroups
}

func DeserializeToConsumerGroup(payload []byte, position int) (*iggcon.ConsumerGroup, int) {
	id := binary.LittleEndian.Uint32(payload[position : position+4])
	partitionsCount := binary.LittleEndian.Uint32(payload[position+4 : position+8])
	membersCount := binary.LittleEndian.Uint32(payload[position+8 : position+12])
	nameLength := int(payload[position+12])
	name := string(payload[position+13 : position+13+nameLength])

	readBytes := 12 + 1 + nameLength

	consumerGroup := iggcon.ConsumerGroup{
		Id:              id,
		MembersCount:    membersCount,
		PartitionsCount: partitionsCount,
		Name:            name,
	}

	return &consumerGroup, readBytes
}

func DeserializeConsumerGroup(payload []byte) *iggcon.ConsumerGroupDetails {
	consumerGroup, pos := DeserializeToConsumerGroup(payload, 0)
	members := make([]iggcon.ConsumerGroupMember, 0)
	for pos < len(payload) {
		m, readBytes := DeserializeToConsumerGroupMember(payload, pos)
		members = append(members, m)
		pos += readBytes
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].ID < members[j].ID
	})
	return &iggcon.ConsumerGroupDetails{
		ConsumerGroup: *consumerGroup,
		Members:       members,
	}
}

func DeserializeToConsumerGroupMember(payload []byte, position int) (iggcon.ConsumerGroupMember, int) {
	id := binary.LittleEndian.Uint32(payload[position : position+4])
	partitionsCount := binary.LittleEndian.Uint32(payload[position+4 : position+8])
	var partitions []uint32
	for i := 0; i < int(partitionsCount); i++ {
		partitionId := binary.LittleEndian.Uint32(payload[position+8+i*4 : position+12+i*4])
		partitions = append(partitions, partitionId)
	}
	readBytes := 4 + 4 + int(partitionsCount)*4
	return iggcon.ConsumerGroupMember{
		ID:              id,
		PartitionsCount: partitionsCount,
		Partitions:      partitions,
	}, readBytes
}

func DeserializeUsers(payload []byte) ([]iggcon.UserInfo, error) {
	if len(payload) == 0 {
		return nil, errors.New("empty payload")
	}

	var result []iggcon.UserInfo
	length := len(payload)
	position := 0

	for position < length {
		response, readBytes, err := deserializeToUser(payload, position)
		if err != nil {
			return nil, err
		}
		result = append(result, *response)
		position += readBytes
	}

	return result, nil
}

func DeserializeUser(payload []byte) (*iggcon.UserInfoDetails, error) {
	response, position, err := deserializeToUser(payload, 0)
	if err != nil {
		return nil, err
	}
	hasPermissions := payload[position]
	userInfo := iggcon.UserInfo{
		Id:        response.Id,
		CreatedAt: response.CreatedAt,
		Username:  response.Username,
		Status:    response.Status,
		Options:   response.Options,
	}
	if hasPermissions == 1 {
		permissionLength := binary.LittleEndian.Uint32(payload[position+1 : position+5])
		permissionsPayload := payload[position+5 : position+5+int(permissionLength)]
		permissions := deserializePermissions(permissionsPayload)
		return &iggcon.UserInfoDetails{
			UserInfo:    userInfo,
			Permissions: permissions,
		}, err
	}
	return &iggcon.UserInfoDetails{
		UserInfo:    userInfo,
		Permissions: nil,
	}, err
}

func deserializePermissions(bytes []byte) *iggcon.Permissions {
	streamMap := make(map[int]*iggcon.StreamPermissions)
	index := 0

	globalPermissions := iggcon.GlobalPermissions{
		ManageServers: bytes[index] == 1,
		ReadServers:   bytes[index+1] == 1,
		ManageUsers:   bytes[index+2] == 1,
		ReadUsers:     bytes[index+3] == 1,
		ManageStreams: bytes[index+4] == 1,
		ReadStreams:   bytes[index+5] == 1,
		ManageTopics:  bytes[index+6] == 1,
		ReadTopics:    bytes[index+7] == 1,
		PollMessages:  bytes[index+8] == 1,
		SendMessages:  bytes[index+9] == 1,
	}

	index += 10

	if bytes[index] == 1 {
		for {
			index += 1
			streamId := int(binary.LittleEndian.Uint32(bytes[index : index+4]))
			index += 4

			manageStream := bytes[index] == 1
			readStream := bytes[index+1] == 1
			manageTopics := bytes[index+2] == 1
			readTopics := bytes[index+3] == 1
			pollMessagesStream := bytes[index+4] == 1
			sendMessagesStream := bytes[index+5] == 1
			topicsMap := make(map[int]*iggcon.TopicPermissions)

			index += 6

			if bytes[index] == 1 {
				for {
					index += 1
					topicId := int(binary.LittleEndian.Uint32(bytes[index : index+4]))
					index += 4

					manageTopic := bytes[index] == 1
					readTopic := bytes[index+1] == 1
					pollMessagesTopic := bytes[index+2] == 1
					sendMessagesTopic := bytes[index+3] == 1

					topicsMap[topicId] = &iggcon.TopicPermissions{
						ManageTopic:  manageTopic,
						ReadTopic:    readTopic,
						PollMessages: pollMessagesTopic,
						SendMessages: sendMessagesTopic,
					}

					index += 4

					if bytes[index] == 0 {
						break
					}
				}
			}

			streamMap[streamId] = &iggcon.StreamPermissions{
				ManageStream: manageStream,
				ReadStream:   readStream,
				ManageTopics: manageTopics,
				ReadTopics:   readTopics,
				PollMessages: pollMessagesStream,
				SendMessages: sendMessagesStream,
				Topics:       topicsMap,
			}

			index += 1

			if bytes[index] == 0 {
				break
			}
		}
	}

	return &iggcon.Permissions{
		Global:  globalPermissions,
		Streams: streamMap,
	}
}

func deserializeToUser(payload []byte, position int) (*iggcon.UserInfo, int, error) {
	if len(payload) < position+14 {
		return nil, 0, errors.New("not enough data to map UserInfo")
	}

	id := binary.LittleEndian.Uint32(payload[position : position+4])
	createdAt := binary.LittleEndian.Uint64(payload[position+4 : position+12])
	status := payload[position+12]
	var userStatus iggcon.UserStatus
	switch status {
	case 1:
		userStatus = iggcon.Active
	case 2:
		userStatus = iggcon.Inactive
	default:
		return nil, 0, fmt.Errorf("invalid user status: %d", status)
	}

	usernameLength := payload[position+13]
	if len(payload) < position+14+int(usernameLength) {
		return nil, 0, errors.New("not enough data to map username")
	}
	username := string(payload[position+14 : position+14+int(usernameLength)])

	readBytes := 4 + 8 + 1 + 1 + int(usernameLength)
	options, optionsSize, err := deserializeOptions(payload, position+readBytes)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read user options: %w", err)
	}
	readBytes += optionsSize

	return &iggcon.UserInfo{
		Id:        id,
		CreatedAt: createdAt,
		Status:    userStatus,
		Username:  username,
		Options:   options,
	}, readBytes, nil
}

func DeserializeClients(payload []byte) ([]iggcon.ClientInfo, error) {
	if len(payload) == 0 {
		return []iggcon.ClientInfo{}, nil
	}

	var response []iggcon.ClientInfo
	length := len(payload)
	position := 0

	for position < length {
		client, readBytes := MapClientInfo(payload, position)
		response = append(response, client)
		position += readBytes
	}

	return response, nil
}

func MapClientInfo(payload []byte, position int) (iggcon.ClientInfo, int) {
	var readBytes int
	id := binary.LittleEndian.Uint32(payload[position : position+4])
	userId := binary.LittleEndian.Uint32(payload[position+4 : position+8])
	transport := "Unknown"

	transportByte := payload[position+8]
	switch transportByte {
	case 1:
		transport = string(iggcon.Tcp)
	case 2:
		transport = string(iggcon.Quic)
	}

	addressLength := int(binary.LittleEndian.Uint32(payload[position+9 : position+13]))
	address := string(payload[position+13 : position+13+addressLength])
	readBytes = 4 + 1 + 4 + 4 + addressLength
	position += readBytes
	consumerGroupsCount := binary.LittleEndian.Uint32(payload[position : position+4])
	readBytes += 4

	return iggcon.ClientInfo{
		ID:                  id,
		UserID:              userId,
		Transport:           transport,
		Address:             address,
		ConsumerGroupsCount: consumerGroupsCount,
	}, readBytes
}

func DeserializeClient(payload []byte) *iggcon.ClientInfoDetails {
	clientInfo, position := MapClientInfo(payload, 0)
	consumerGroups := make([]iggcon.ConsumerGroupInfo, 0, clientInfo.ConsumerGroupsCount)

	for i := uint32(0); i < clientInfo.ConsumerGroupsCount; i++ {
		streamId := binary.LittleEndian.Uint32(payload[position : position+4])
		topicId := binary.LittleEndian.Uint32(payload[position+4 : position+8])
		groupId := binary.LittleEndian.Uint32(payload[position+8 : position+12])

		consumerGroup := iggcon.ConsumerGroupInfo{
			StreamId: streamId,
			TopicId:  topicId,
			GroupId:  groupId,
		}
		consumerGroups = append(consumerGroups, consumerGroup)
		position += 12
	}

	return &iggcon.ClientInfoDetails{
		ClientInfo:     clientInfo,
		ConsumerGroups: consumerGroups,
	}
}

func DeserializeAccessToken(payload []byte) (*iggcon.RawPersonalAccessToken, error) {
	tokenLength := int(payload[0])
	token := string(payload[1 : 1+tokenLength])
	return &iggcon.RawPersonalAccessToken{
		Token: token,
	}, nil
}

func DeserializeAccessTokens(payload []byte) ([]iggcon.PersonalAccessTokenInfo, error) {
	if len(payload) == 0 {
		return []iggcon.PersonalAccessTokenInfo{}, ierror.ErrEmptyMessagePayload
	}

	var result []iggcon.PersonalAccessTokenInfo
	position := 0
	length := len(payload)

	for position < length {
		response, readBytes := deserializeToPersonalAccessTokenResponse(payload, position)
		result = append(result, response)
		position += readBytes
	}

	return result, nil
}

func deserializeToPersonalAccessTokenResponse(payload []byte, position int) (iggcon.PersonalAccessTokenInfo, int) {
	nameLength := int(payload[position])
	name := string(payload[position+1 : position+1+nameLength])
	expiryBytes := payload[position+1+nameLength:]
	var expiry *time.Time

	if len(expiryBytes) >= 8 {
		unixMicroSeconds := binary.LittleEndian.Uint64(expiryBytes)
		expiryTime := time.Unix(0, int64(unixMicroSeconds))
		expiry = &expiryTime
	}

	readBytes := 1 + nameLength + 8

	return iggcon.PersonalAccessTokenInfo{
		Name:   name,
		Expiry: expiry,
	}, readBytes
}

// confirmationEntryLength is the width of one send confirmation:
// [stream_id u32][topic_id u32][partition_id u32][base_offset u64].
const confirmationEntryLength = 20

// DeserializeSendMessagesConfirmations decodes [count u32] followed by count
// confirmation entries. An empty payload is an empty list. Trailing bytes are
// a decode error, because a frame the client cannot account for means the
// stream is out of sync.
func DeserializeSendMessagesConfirmations(payload []byte) (*iggcon.SendMessagesResponse, error) {
	if len(payload) == 0 {
		return &iggcon.SendMessagesResponse{}, nil
	}
	if len(payload) < 4 {
		return nil, fmt.Errorf("send confirmations: %d bytes is short of the count", len(payload))
	}

	count := binary.LittleEndian.Uint32(payload)
	body := payload[4:]
	if uint64(len(body)) != uint64(count)*confirmationEntryLength {
		return nil, fmt.Errorf(
			"send confirmations: %d entries do not fill %d body bytes", count, len(body))
	}

	confirmations := make([]iggcon.SendMessagesConfirmation, 0, count)
	for offset := 0; offset < len(body); offset += confirmationEntryLength {
		entry := body[offset : offset+confirmationEntryLength]
		confirmations = append(confirmations, iggcon.SendMessagesConfirmation{
			StreamId:    binary.LittleEndian.Uint32(entry[0:4]),
			TopicId:     binary.LittleEndian.Uint32(entry[4:8]),
			PartitionId: binary.LittleEndian.Uint32(entry[8:12]),
			BaseOffset:  binary.LittleEndian.Uint64(entry[12:20]),
		})
	}
	return &iggcon.SendMessagesResponse{Confirmations: confirmations}, nil
}

// assignmentHeaderLength covers [generation u64][partitions_count u32].
const assignmentHeaderLength = 12

// DeserializeConsumerGroupAssignment decodes
// [generation u64][partitions_count u32][partition_id u32 x n]. An empty
// payload means the client is not a member of the group, reported as
// ErrConsumerGroupMemberNotFound. A member holding no partition arrives as a
// header with a zero count and decodes to an empty Partitions slice.
func DeserializeConsumerGroupAssignment(payload []byte) (*iggcon.ConsumerGroupAssignment, error) {
	if len(payload) == 0 {
		return nil, ierror.ErrConsumerGroupMemberNotFound
	}
	if len(payload) < assignmentHeaderLength {
		return nil, fmt.Errorf("group assignment: %d bytes is short of the header", len(payload))
	}

	count := binary.LittleEndian.Uint32(payload[8:12])
	body := payload[assignmentHeaderLength:]
	if uint64(len(body)) != uint64(count)*4 {
		return nil, fmt.Errorf(
			"group assignment: %d partitions do not fill %d body bytes", count, len(body))
	}

	partitions := make([]uint32, 0, count)
	for offset := 0; offset < len(body); offset += 4 {
		partitions = append(partitions, binary.LittleEndian.Uint32(body[offset:offset+4]))
	}
	return &iggcon.ConsumerGroupAssignment{
		Generation: binary.LittleEndian.Uint64(payload[0:8]),
		Partitions: partitions,
	}, nil
}
