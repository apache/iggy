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
	"time"
)

type MessageID [16]byte

// MessageHeader carries a message's metadata. On send, Id and
// OriginTimestamp are the producer's (a zero Id is minted on encode); on
// poll, Offset and Timestamp are the absolute values stamped by the server.
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
