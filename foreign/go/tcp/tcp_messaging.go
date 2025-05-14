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

func (tms *IggyTcpClient) SendMessages(request SendMessagesRequest) error {
	if len(request.Messages) == 0 {
		return ierror.CustomError("messages_count_should_be_greater_than_zero")
	}
	serializedRequest := binaryserialization.TcpSendMessagesRequest{SendMessagesRequest: request}
	_, err := tms.sendAndFetchResponse(serializedRequest.Serialize(tms.MessageCompression), SendMessagesCode)
	return err
}

func (tms *IggyTcpClient) PollMessages(request FetchMessagesRequest) (*FetchMessagesResponse, error) {
	serializedRequest := binaryserialization.TcpFetchMessagesRequest{FetchMessagesRequest: request}
	buffer, err := tms.sendAndFetchResponse(serializedRequest.Serialize(), PollMessagesCode)
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeFetchMessagesResponse(buffer, tms.MessageCompression)
}
