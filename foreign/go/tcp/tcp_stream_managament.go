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

package tcp

import (
	binaryserialization "github.com/iggy-rs/iggy-go-client/binary_serialization"
	. "github.com/iggy-rs/iggy-go-client/contracts"
	ierror "github.com/iggy-rs/iggy-go-client/errors"
)

func (tms *IggyTcpClient) GetStreams() ([]StreamResponse, error) {
	buffer, err := tms.sendAndFetchResponse([]byte{}, GetStreamsCode)
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeStreams(buffer), nil
}

func (tms *IggyTcpClient) GetStreamById(request GetStreamRequest) (*StreamResponse, error) {
	message := binaryserialization.SerializeIdentifier(request.StreamID)
	buffer, err := tms.sendAndFetchResponse(message, GetStreamCode)
	if err != nil {
		return nil, err
	}
	if len(buffer) == 0 {
		return nil, ierror.StreamIdNotFound
	}

	stream, _ := binaryserialization.DeserializeToStream(buffer, 0)
	return &stream, nil
}

func (tms *IggyTcpClient) CreateStream(request CreateStreamRequest) error {
	if MaxStringLength < len(request.Name) {
		return ierror.TextTooLong("stream_name")
	}
	serializedRequest := binaryserialization.TcpCreateStreamRequest{CreateStreamRequest: request}
	_, err := tms.sendAndFetchResponse(serializedRequest.Serialize(), CreateStreamCode)
	return err
}

func (tms *IggyTcpClient) UpdateStream(request UpdateStreamRequest) error {
	if MaxStringLength <= len(request.Name) {
		return ierror.TextTooLong("stream_name")
	}
	serializedRequest := binaryserialization.TcpUpdateStreamRequest{UpdateStreamRequest: request}
	_, err := tms.sendAndFetchResponse(serializedRequest.Serialize(), UpdateStreamCode)
	return err
}

func (tms *IggyTcpClient) DeleteStream(id Identifier) error {
	message := binaryserialization.SerializeIdentifier(id)
	_, err := tms.sendAndFetchResponse(message, DeleteStreamCode)
	return err
}
