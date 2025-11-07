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
	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

type TcpUpdateStreamRequest struct {
	StreamId iggcon.Identifier `json:"streamId"`
	Name     string            `json:"name"`
}

func (request *TcpUpdateStreamRequest) Serialize() []byte {
	nameLength := len(request.Name)
	bytes := make([]byte, nameLength+request.StreamId.Length+3)
	copy(bytes[0:2+request.StreamId.Length], SerializeIdentifier(request.StreamId))
	position := 2 + request.StreamId.Length
	bytes[position] = byte(nameLength)
	copy(bytes[position+1:], []byte(request.Name))
	return bytes
}
