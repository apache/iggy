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

import (
	"encoding/binary"
	"errors"
	"time"
)

const MessageHeaderSize = 8 + 16 + 8 + 8 + 8 + 4 + 4 + 8

type MessageID [16]byte

type MessageHeader struct {
	Checksum         uint64    `json:"checksum"`
	Id               MessageID `json:"id"`
	Offset           uint64    `json:"offset"`
	Timestamp        uint64    `json:"timestamp"`
	OriginTimestamp  uint64    `json:"origin_timestamp"`
	UserHeaderLength uint32    `json:"user_header_length"`
	PayloadLength    uint32    `json:"payload_length"`
	Reserved         uint64    `json:"reserved"`
}

func NewMessageHeader(id MessageID, payloadLength uint32, userHeaderLength uint32) MessageHeader {
	return MessageHeader{
		Id:               id,
		OriginTimestamp:  uint64(time.Now().UnixMicro()),
		PayloadLength:    payloadLength,
		UserHeaderLength: userHeaderLength,
	}
}

func MessageHeaderFromBytes(data []byte) (*MessageHeader, error) {

	if len(data) != MessageHeaderSize {
		return nil, errors.New("data has incorrect size, must be 64")
	}
	checksum := binary.LittleEndian.Uint64(data[0:8])
	id := data[8:24]
	offset := binary.LittleEndian.Uint64(data[24:32])
	timestamp := binary.LittleEndian.Uint64(data[32:40])
	originTimestamp := binary.LittleEndian.Uint64(data[40:48])
	userHeaderLength := binary.LittleEndian.Uint32(data[48:52])
	payloadLength := binary.LittleEndian.Uint32(data[52:56])
	reserved := binary.LittleEndian.Uint64(data[56:64])

	return &MessageHeader{
		Checksum:         checksum,
		Id:               MessageID(id),
		Offset:           offset,
		Timestamp:        timestamp,
		OriginTimestamp:  originTimestamp,
		UserHeaderLength: userHeaderLength,
		PayloadLength:    payloadLength,
		Reserved:         reserved,
	}, nil
}

func (mh *MessageHeader) ToBytes() []byte {
	bytes, _ := mh.AppendBinary(make([]byte, 0, MessageHeaderSize))
	return bytes
}

func (mh *MessageHeader) AppendBinary(b []byte) ([]byte, error) {
	b = binary.LittleEndian.AppendUint64(b, mh.Checksum)
	b = append(b, mh.Id[:]...)
	b = binary.LittleEndian.AppendUint64(b, mh.Offset)
	b = binary.LittleEndian.AppendUint64(b, mh.Timestamp)
	b = binary.LittleEndian.AppendUint64(b, mh.OriginTimestamp)
	b = binary.LittleEndian.AppendUint32(b, mh.UserHeaderLength)
	b = binary.LittleEndian.AppendUint32(b, mh.PayloadLength)
	return binary.LittleEndian.AppendUint64(b, mh.Reserved), nil
}
