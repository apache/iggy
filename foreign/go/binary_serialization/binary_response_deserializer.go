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
	"time"

	. "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/klauspost/compress/s2"
)

func DeserializeLogInResponse(payload []byte) *LogInResponse {
	userId := binary.LittleEndian.Uint32(payload[0:4])
	return &LogInResponse{
		UserId: userId,
	}
}

func DeserializeOffset(payload []byte) *OffsetResponse {
	partitionId := int(binary.LittleEndian.Uint32(payload[0:4]))
	currentOffset := binary.LittleEndian.Uint64(payload[4:12])
	storedOffset := binary.LittleEndian.Uint64(payload[12:20])

	return &OffsetResponse{
		PartitionId:   partitionId,
		CurrentOffset: currentOffset,
		StoredOffset:  storedOffset,
	}
}

func DeserializeStreams(payload []byte) []StreamResponse {
	streams := make([]StreamResponse, 0)
	position := 0

	//TODO there's a deserialization bug, investigate this
	//it occurs only with payload greater than 2 pow 16
	for position < len(payload) {
		stream, readBytes := DeserializeToStream(payload, position)
		streams = append(streams, stream)
		position += readBytes
	}

	return streams
}

func DeserializerStream(payload []byte) *StreamResponse {
	stream, position := DeserializeToStream(payload, 0)
	topics := make([]TopicResponse, 0)
	length := len(payload)

	for position < length {
		topic, readBytes, _ := DeserializeToTopic(payload, position)
		topics = append(topics, topic)
		position += readBytes
	}

	return &StreamResponse{
		Id:            stream.Id,
		TopicsCount:   stream.TopicsCount,
		Name:          stream.Name,
		Topics:        topics,
		MessagesCount: stream.MessagesCount,
		SizeBytes:     stream.SizeBytes,
		CreatedAt:     stream.CreatedAt,
	}
}

func DeserializeToStream(payload []byte, position int) (StreamResponse, int) {
	id := int(binary.LittleEndian.Uint32(payload[position : position+4]))
	createdAt := binary.LittleEndian.Uint64(payload[position+4 : position+12])
	topicsCount := int(binary.LittleEndian.Uint32(payload[position+12 : position+16]))
	sizeBytes := binary.LittleEndian.Uint64(payload[position+16 : position+24])
	messagesCount := binary.LittleEndian.Uint64(payload[position+24 : position+32])
	nameLength := int(payload[position+32])

	nameBytes := payload[position+33 : position+33+nameLength]
	name := string(nameBytes)

	readBytes := 4 + 8 + 4 + 8 + 8 + 1 + nameLength

	return StreamResponse{
		Id:            id,
		TopicsCount:   topicsCount,
		Name:          name,
		SizeBytes:     sizeBytes,
		MessagesCount: messagesCount,
		CreatedAt:     createdAt,
	}, readBytes
}

func DeserializeFetchMessagesResponse(payload []byte, compression IggyMessageCompression) (*FetchMessagesResponse, error) {
	if len(payload) == 0 {
		return &FetchMessagesResponse{
			PartitionId:   0,
			CurrentOffset: 0,
			Messages:      make([]IggyMessage, 0),
		}, nil
	}

	length := len(payload)
	partitionId := binary.LittleEndian.Uint32(payload[0:4])
	currentOffset := binary.LittleEndian.Uint64(payload[4:12])
	messagesCount := binary.LittleEndian.Uint32(payload[12:16])
	position := 16
	var messages = make([]IggyMessage, 0)
	for position < length {
		if position+MessageHeaderSize >= length {
			// body needs to be at least 1 byte
			break
		}
		header, err := MessageHeaderFromBytes(payload[position : position+MessageHeaderSize])
		if err != nil {
			return nil, err
		}
		position += MessageHeaderSize
		payload_end := position + int(header.PayloadLength)
		if int(payload_end) > length {
			break
		}
		payloadSlice := payload[position:payload_end]
		position = int(payload_end)

		var user_headers []byte = nil
		if header.UserHeaderLength > 0 {
			user_headers = payload[position : position+int(header.UserHeaderLength)]
		}
		position += int(header.UserHeaderLength)

		switch compression {
		case MESSAGE_COMPRESSION_S2, MESSAGE_COMPRESSION_S2_BETTER, MESSAGE_COMPRESSION_S2_BEST:
			if length < 32 {
				break
			}
			payloadSlice, err = s2.Decode(nil, payloadSlice)
			if err != nil {
				panic("iggy: failed to decode s2 payload: " + err.Error())
			}
		}

		messages = append(messages, IggyMessage{
			Header:      *header,
			Payload:     payloadSlice,
			UserHeaders: user_headers,
		})
	}

	// !TODO: Add message offset ordering
	return &FetchMessagesResponse{
		PartitionId:   partitionId,
		CurrentOffset: currentOffset,
		Messages:      messages,
		MessageCount:  messagesCount,
	}, nil
}

func DeserializeTopics(payload []byte) ([]TopicResponse, error) {
	topics := make([]TopicResponse, 0)
	length := len(payload)
	position := 0

	for position < length {
		topic, readBytes, err := DeserializeToTopic(payload, position)
		if err != nil {
			return nil, err
		}
		topics = append(topics, topic)
		position += readBytes
	}

	return topics, nil
}

func DeserializeTopic(payload []byte) (*TopicResponse, error) {
	topic, position, err := DeserializeToTopic(payload, 0)
	if err != nil {
		return &TopicResponse{}, err
	}

	partitions := make([]PartitionContract, 0)
	length := len(payload)

	for position < length {
		partition, readBytes := DeserializePartition(payload, position)
		if err != nil {
			return &TopicResponse{}, err
		}
		partitions = append(partitions, partition)
		position += readBytes
	}

	topic.Partitions = partitions

	return &topic, nil
}

func DeserializeToTopic(payload []byte, position int) (TopicResponse, int, error) {
	topic := TopicResponse{}
	topic.Id = int(binary.LittleEndian.Uint32(payload[position : position+4]))
	topic.CreatedAt = int(binary.LittleEndian.Uint64(payload[position+4 : position+12]))
	topic.PartitionsCount = int(binary.LittleEndian.Uint32(payload[position+12 : position+16]))
	topic.MessageExpiry = time.Microsecond * time.Duration(int(binary.LittleEndian.Uint64(payload[position+16:position+24])))
	topic.CompressionAlgorithm = payload[position+24]
	topic.MaxTopicSize = binary.LittleEndian.Uint64(payload[position+25 : position+33])
	topic.ReplicationFactor = payload[position+33]
	topic.SizeBytes = binary.LittleEndian.Uint64(payload[position+34 : position+42])
	topic.MessagesCount = binary.LittleEndian.Uint64(payload[position+42 : position+50])

	nameLength := int(payload[position+50])
	topic.Name = string(payload[position+51 : position+51+nameLength])

	readBytes := 4 + 8 + 4 + 8 + 8 + 8 + 8 + 1 + 1 + 1 + nameLength
	return topic, readBytes, nil
}

func DeserializePartition(payload []byte, position int) (PartitionContract, int) {
	id := int(binary.LittleEndian.Uint32(payload[position : position+4]))
	createdAt := binary.LittleEndian.Uint64(payload[position+4 : position+12])
	segmentsCount := int(binary.LittleEndian.Uint32(payload[position+12 : position+16]))
	currentOffset := binary.LittleEndian.Uint64(payload[position+16 : position+24])
	sizeBytes := binary.LittleEndian.Uint64(payload[position+24 : position+32])
	messagesCount := binary.LittleEndian.Uint64(payload[position+32 : position+40])
	readBytes := 4 + 4 + 8 + 8 + 8 + 8

	partition := PartitionContract{
		Id:            id,
		CreatedAt:     createdAt,
		SegmentsCount: segmentsCount,
		CurrentOffset: currentOffset,
		SizeBytes:     sizeBytes,
		MessagesCount: messagesCount,
	}

	return partition, readBytes
}

func DeserializeConsumerGroups(payload []byte) []ConsumerGroupResponse {
	var consumerGroups []ConsumerGroupResponse
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

func DeserializeConsumerGroup(payload []byte) (*ConsumerGroupResponse, error) {
	consumerGroup, _ := DeserializeToConsumerGroup(payload, 0)
	return consumerGroup, nil
}

func DeserializeToConsumerGroup(payload []byte, position int) (*ConsumerGroupResponse, int) {
	id := int(binary.LittleEndian.Uint32(payload[position : position+4]))
	partitionsCount := int(binary.LittleEndian.Uint32(payload[position+4 : position+8]))
	membersCount := int(binary.LittleEndian.Uint32(payload[position+8 : position+12]))
	nameLength := int(payload[position+12])
	name := string(payload[position+13 : position+13+nameLength])

	readBytes := 12 + 1 + nameLength

	consumerGroup := ConsumerGroupResponse{
		Id:              id,
		MembersCount:    membersCount,
		PartitionsCount: partitionsCount,
		Name:            name,
	}

	return &consumerGroup, readBytes
}

func DeserializeUsers(payload []byte) ([]*UserResponse, error) {
	if len(payload) == 0 {
		return nil, errors.New("empty payload")
	}

	var result []*UserResponse
	length := len(payload)
	position := 0

	for position < length {
		response, readBytes, err := deserializeUserResponse(payload, position)
		if err != nil {
			return nil, err
		}
		result = append(result, response)
		position += readBytes
	}

	return result, nil
}

func DeserializeUser(payload []byte) (*UserResponse, error) {
	response, position, err := deserializeUserResponse(payload, 0)
	hasPermissions := payload[position]
	if hasPermissions == 1 {
		permissionLength := binary.LittleEndian.Uint32(payload[position+1 : position+5])
		permissionsPayload := payload[position+5 : position+5+int(permissionLength)]
		permissions := deserializePermissions(permissionsPayload)
		return &UserResponse{
			Permissions: permissions,
			Id:          response.Id,
			CreatedAt:   response.CreatedAt,
			Username:    response.Username,
			Status:      response.Status,
		}, err
	}
	return &UserResponse{
		Id:          response.Id,
		CreatedAt:   response.CreatedAt,
		Username:    response.Username,
		Status:      response.Status,
		Permissions: nil,
	}, err
}

func deserializePermissions(bytes []byte) *Permissions {
	streamMap := make(map[int]*StreamPermissions)
	index := 0

	globalPermissions := GlobalPermissions{
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
			topicsMap := make(map[int]*TopicPermissions)

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

					topicsMap[topicId] = &TopicPermissions{
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

			streamMap[streamId] = &StreamPermissions{
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

	return &Permissions{
		Global:  globalPermissions,
		Streams: streamMap,
	}
}

func deserializeUserResponse(payload []byte, position int) (*UserResponse, int, error) {
	if len(payload) < position+14 {
		return nil, 0, errors.New("not enough data to map UserResponse")
	}

	id := binary.LittleEndian.Uint32(payload[position : position+4])
	createdAt := binary.LittleEndian.Uint64(payload[position+4 : position+12])
	status := payload[position+12]
	var userStatus UserStatus
	switch status {
	case 1:
		userStatus = Active
	case 2:
		userStatus = Inactive
	default:
		return nil, 0, fmt.Errorf("invalid user status: %d", status)
	}

	usernameLength := payload[position+13]
	if len(payload) < position+14+int(usernameLength) {
		return nil, 0, errors.New("not enough data to map username")
	}
	username := string(payload[position+14 : position+14+int(usernameLength)])

	readBytes := 4 + 8 + 1 + 1 + int(usernameLength)

	return &UserResponse{
		Id:        id,
		CreatedAt: createdAt,
		Status:    userStatus,
		Username:  username,
	}, readBytes, nil
}

func DeserializeClients(payload []byte) ([]ClientResponse, error) {
	if len(payload) == 0 {
		return []ClientResponse{}, nil
	}

	var response []ClientResponse
	length := len(payload)
	position := 0

	for position < length {
		client, readBytes := MapClientInfo(payload, position)
		response = append(response, client)
		position += readBytes
	}

	return response, nil
}

func MapClientInfo(payload []byte, position int) (ClientResponse, int) {
	var readBytes int
	id := binary.LittleEndian.Uint32(payload[position : position+4])
	userId := binary.LittleEndian.Uint32(payload[position+4 : position+8])
	transportByte := payload[position+8]
	transport := "Unknown"

	if transportByte == 1 {
		transport = string(Tcp)
	} else if transportByte == 2 {
		transport = string(Quic)
	}

	addressLength := int(binary.LittleEndian.Uint32(payload[position+9 : position+13]))
	address := string(payload[position+13 : position+13+addressLength])
	readBytes = 4 + 1 + 4 + 4 + addressLength
	position += readBytes
	consumerGroupsCount := binary.LittleEndian.Uint32(payload[position : position+4])
	readBytes += 4

	return ClientResponse{
		ID:                  id,
		UserID:              userId,
		Transport:           transport,
		Address:             address,
		ConsumerGroupsCount: consumerGroupsCount,
	}, readBytes
}

func DeserializeClient(payload []byte) *ClientResponse {
	response, position := MapClientInfo(payload, 0)
	consumerGroups := make([]ConsumerGroupInfo, response.ConsumerGroupsCount)
	length := len(payload)

	for position < length {
		for i := uint32(0); i < response.ConsumerGroupsCount; i++ {
			streamId := int32(binary.LittleEndian.Uint32(payload[position : position+4]))
			topicId := int32(binary.LittleEndian.Uint32(payload[position+4 : position+8]))
			consumerGroupId := int32(binary.LittleEndian.Uint32(payload[position+8 : position+12]))

			consumerGroup := ConsumerGroupInfo{
				StreamId:        int(streamId),
				TopicId:         int(topicId),
				ConsumerGroupId: int(consumerGroupId),
			}
			consumerGroups = append(consumerGroups, consumerGroup)
			position += 12
		}
	}
	response.ConsumerGroups = consumerGroups
	return &response
}

func DeserializeAccessToken(payload []byte) (*AccessToken, error) {
	tokenLength := int(payload[0])
	token := string(payload[1 : 1+tokenLength])
	return &AccessToken{
		Token: token,
	}, nil
}

func DeserializeAccessTokens(payload []byte) ([]AccessTokenResponse, error) {
	if len(payload) == 0 {
		return []AccessTokenResponse{}, ierror.CustomError("Empty payload")
	}

	var result []AccessTokenResponse
	position := 0
	length := len(payload)

	for position < length {
		response, readBytes := deserializeToPersonalAccessTokenResponse(payload, position)
		result = append(result, response)
		position += readBytes
	}

	return result, nil
}

func deserializeToPersonalAccessTokenResponse(payload []byte, position int) (AccessTokenResponse, int) {
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

	return AccessTokenResponse{
		Name:   name,
		Expiry: expiry,
	}, readBytes
}
