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
)

type TcpCreateStreamRequest struct {
	Name     string
	StreamId *uint32
}

const (
	streamIDOffset   = 0
	nameLengthOffset = 4
	payloadOffset    = 5
)

func (request *TcpCreateStreamRequest) Serialize() []byte {
	if request.StreamId == nil {
		request.StreamId = new(uint32)
	}

	nameLength := len(request.Name)
	serialized := make([]byte, payloadOffset+nameLength)

	binary.LittleEndian.PutUint32(serialized[streamIDOffset:], *request.StreamId)
	serialized[nameLengthOffset] = byte(nameLength)
	copy(serialized[payloadOffset:], []byte(request.Name))

	return serialized
}
