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

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func CreateGroup(request iggcon.CreateConsumerGroupRequest) []byte {
	if request.ConsumerGroupId == nil {
		request.ConsumerGroupId = new(uint32)
	}
	customIdOffset := 4 + request.StreamId.Length + request.TopicId.Length
	bytes := make([]byte, 4+request.StreamId.Length+request.TopicId.Length+1+4+len(request.Name))
	copy(bytes[0:customIdOffset], SerializeIdentifiers(request.StreamId, request.TopicId))
	binary.LittleEndian.PutUint32(bytes[customIdOffset:customIdOffset+4], *request.ConsumerGroupId)
	bytes[customIdOffset+4] = byte(len(request.Name))
	copy(bytes[customIdOffset+5:], request.Name)
	return bytes
}

func UpdateOffset(request iggcon.StoreConsumerOffsetRequest) []byte {
	if request.PartitionId == nil {
		request.PartitionId = new(uint32)
	}
	bytes := make([]byte, 6+request.StreamId.Length+request.TopicId.Length+request.Consumer.Id.Length+13)
	bytes[0] = byte(request.Consumer.Kind)
	position := 7 + request.StreamId.Length + request.TopicId.Length + request.Consumer.Id.Length
	copy(bytes[1:position], SerializeIdentifiers(request.Consumer.Id, request.StreamId, request.TopicId))

	binary.LittleEndian.PutUint32(bytes[position:position+4], *request.PartitionId)
	binary.LittleEndian.PutUint64(bytes[position+4:position+12], uint64(request.Offset))
	return bytes
}

func GetOffset(request iggcon.GetConsumerOffsetRequest) []byte {
	if request.PartitionId == nil {
		request.PartitionId = new(uint32)
	}
	bytes := make([]byte, 6+request.StreamId.Length+request.TopicId.Length+request.Consumer.Id.Length+5)
	bytes[0] = byte(request.Consumer.Kind)
	position := 7 + request.StreamId.Length + request.TopicId.Length + request.Consumer.Id.Length
	copy(bytes[1:position], SerializeIdentifiers(request.Consumer.Id, request.StreamId, request.TopicId))
	binary.LittleEndian.PutUint32(bytes[position:position+4], *request.PartitionId)
	return bytes
}

func CreatePartitions(request iggcon.CreatePartitionsRequest) []byte {
	bytes := make([]byte, 8+request.StreamId.Length+request.TopicId.Length)
	position := 4 + request.StreamId.Length + request.TopicId.Length
	copy(bytes[0:position], SerializeIdentifiers(request.StreamId, request.TopicId))
	binary.LittleEndian.PutUint32(bytes[position:position+4], uint32(request.PartitionsCount))

	return bytes
}

func DeletePartitions(request iggcon.DeletePartitionsRequest) []byte {
	bytes := make([]byte, 8+request.StreamId.Length+request.TopicId.Length)
	position := 4 + request.StreamId.Length + request.TopicId.Length
	copy(bytes[0:position], SerializeIdentifiers(request.StreamId, request.TopicId))
	binary.LittleEndian.PutUint32(bytes[position:position+4], uint32(request.PartitionsCount))

	return bytes
}

//USERS

func SerializeCreateUserRequest(request iggcon.CreateUserRequest) []byte {
	capacity := 4 + len(request.Username) + len(request.Password)
	if request.Permissions != nil {
		capacity += 1 + 4 + CalculatePermissionsSize(request.Permissions)
	}

	bytes := make([]byte, capacity)
	position := 0

	bytes[position] = byte(len(request.Username))
	position += 1
	copy(bytes[position:position+len(request.Username)], []byte(request.Username))
	position += len(request.Username)

	bytes[position] = byte(len(request.Password))
	position += 1
	copy(bytes[position:position+len(request.Password)], []byte(request.Password))
	position += len(request.Password)

	statusByte := byte(0)
	switch request.Status {
	case iggcon.Active:
		statusByte = byte(1)
	case iggcon.Inactive:
		statusByte = byte(2)
	}
	bytes[position] = statusByte
	position += 1

	if request.Permissions != nil {
		bytes[position] = byte(1)
		position += 1
		permissionsBytes := GetBytesFromPermissions(request.Permissions)
		binary.LittleEndian.PutUint32(bytes[position:position+4], uint32(len(permissionsBytes)))
		position += 4
		copy(bytes[position:position+len(permissionsBytes)], permissionsBytes)
	} else {
		bytes[position] = byte(0)
	}

	return bytes
}

func GetBytesFromPermissions(data *iggcon.Permissions) []byte {
	size := CalculatePermissionsSize(data)
	bytes := make([]byte, size)

	bytes[0] = boolToByte(data.Global.ManageServers)
	bytes[1] = boolToByte(data.Global.ReadServers)
	bytes[2] = boolToByte(data.Global.ManageUsers)
	bytes[3] = boolToByte(data.Global.ReadUsers)
	bytes[4] = boolToByte(data.Global.ManageStreams)
	bytes[5] = boolToByte(data.Global.ReadStreams)
	bytes[6] = boolToByte(data.Global.ManageTopics)
	bytes[7] = boolToByte(data.Global.ReadTopics)
	bytes[8] = boolToByte(data.Global.PollMessages)
	bytes[9] = boolToByte(data.Global.SendMessages)

	position := 10

	if data.Streams != nil {
		bytes[position] = byte(1)
		position += 1

		for streamID, stream := range data.Streams {
			binary.LittleEndian.PutUint32(bytes[position:position+4], uint32(streamID))
			position += 4

			bytes[position] = boolToByte(stream.ManageStream)
			bytes[position+1] = boolToByte(stream.ReadStream)
			bytes[position+2] = boolToByte(stream.ManageTopics)
			bytes[position+3] = boolToByte(stream.ReadTopics)
			bytes[position+4] = boolToByte(stream.PollMessages)
			bytes[position+5] = boolToByte(stream.SendMessages)
			position += 6

			if stream.Topics != nil {
				bytes[position] = byte(1)
				position += 1

				for topicID, topic := range stream.Topics {
					binary.LittleEndian.PutUint32(bytes[position:position+4], uint32(topicID))
					position += 4

					bytes[position] = boolToByte(topic.ManageTopic)
					bytes[position+1] = boolToByte(topic.ReadTopic)
					bytes[position+2] = boolToByte(topic.PollMessages)
					bytes[position+3] = boolToByte(topic.SendMessages)
					position += 4

					bytes[position] = byte(0)
					position += 1
				}
			} else {
				bytes[position] = byte(0)
				position += 1
			}
		}
	} else {
		bytes[0] = byte(0)
	}

	return bytes
}

func CalculatePermissionsSize(data *iggcon.Permissions) int {
	size := 10

	if data.Streams != nil {
		size += 1

		for _, stream := range data.Streams {
			size += 4
			size += 6
			size += 1

			if stream.Topics != nil {
				size += 1
				size += len(stream.Topics) * 9
			} else {
				size += 1
			}
		}
	} else {
		size += 1
	}

	return size
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func SerializeUpdateUser(request iggcon.UpdateUserRequest) []byte {
	length := request.UserID.Length + 2

	if request.Username == nil {
		request.Username = new(string)
	}

	username := *request.Username

	if len(username) != 0 {
		length += 2 + len(username)
	}

	if request.Status != nil {
		length += 2
	}

	bytes := make([]byte, length+1)
	position := 0

	copy(bytes[position:position+request.UserID.Length+2], SerializeIdentifier(request.UserID))
	position += position + request.UserID.Length + 2

	if len(username) != 0 {
		bytes[position] = 1
		position++
		bytes[position] = byte(len(username))
		position++
		copy(bytes[position:position+len(username)], username)
		position += len(username)
	} else {
		bytes[position] = 0
		position++
	}

	if request.Status != nil {
		bytes[position] = 1
		position++
		statusByte := byte(0)
		switch *request.Status {
		case iggcon.Active:
			statusByte = 1
		case iggcon.Inactive:
			statusByte = 2
		}
		bytes[position] = statusByte
	} else {
		bytes[position] = 0
	}

	return bytes
}

func SerializeChangePasswordRequest(request iggcon.ChangePasswordRequest) []byte {
	length := request.UserID.Length + 2 + len(request.CurrentPassword) + len(request.NewPassword) + 2
	bytes := make([]byte, length)
	position := 0

	copy(bytes[position:position+request.UserID.Length+2], SerializeIdentifier(request.UserID))
	position += request.UserID.Length + 2

	bytes[position] = byte(len(request.CurrentPassword))
	position++
	copy(bytes[position:position+len(request.CurrentPassword)], []byte(request.CurrentPassword))
	position += len(request.CurrentPassword)

	bytes[position] = byte(len(request.NewPassword))
	position++
	copy(bytes[position:position+len(request.NewPassword)], []byte(request.NewPassword))

	return bytes
}

func SerializeUpdateUserPermissionsRequest(request iggcon.UpdatePermissionsRequest) []byte {
	length := request.UserID.Length + 2

	if request.Permissions != nil {
		length += 1 + 4 + CalculatePermissionsSize(request.Permissions)
	}

	bytes := make([]byte, length)
	position := 0

	copy(bytes[position:position+request.UserID.Length+2], SerializeIdentifier(request.UserID))
	position += request.UserID.Length + 2

	if request.Permissions != nil {
		bytes[position] = 1
		position++
		permissionsBytes := GetBytesFromPermissions(request.Permissions)
		binary.LittleEndian.PutUint32(bytes[position:position+4], uint32(len(permissionsBytes)))
		position += 4
		copy(bytes[position:position+len(permissionsBytes)], permissionsBytes)
	} else {
		bytes[position] = 0
	}

	return bytes
}

func SerializeInt(value int) []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, uint32(value))
	return bytes
}

func SerializeLoginWithPersonalAccessToken(request iggcon.LoginWithPersonalAccessTokenRequest) []byte {
	length := 1 + len(request.Token)
	bytes := make([]byte, length)
	bytes[0] = byte(len(request.Token))
	copy(bytes[1:], []byte(request.Token))
	return bytes
}

func SerializeDeletePersonalAccessToken(request iggcon.DeletePersonalAccessTokenRequest) []byte {
	length := 1 + len(request.Name)
	bytes := make([]byte, length)
	bytes[0] = byte(len(request.Name))
	copy(bytes[1:], []byte(request.Name))
	return bytes
}

func SerializeCreatePersonalAccessToken(request iggcon.CreatePersonalAccessTokenRequest) []byte {
	length := 1 + len(request.Name) + 8
	bytes := make([]byte, length)
	bytes[0] = byte(len(request.Name))
	copy(bytes[1:], []byte(request.Name))
	binary.LittleEndian.PutUint32(bytes[len(bytes)-4:], request.Expiry)
	return bytes
}
