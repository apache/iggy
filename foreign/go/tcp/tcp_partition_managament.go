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
	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	. "github.com/apache/iggy/foreign/go/contracts"
)

func (tms *IggyTcpClient) CreatePartitions(streamId Identifier, topicId Identifier, partitionsCount uint32) error {
	message := binaryserialization.CreatePartitions(CreatePartitionsRequest{
		StreamId:        streamId,
		TopicId:         topicId,
		PartitionsCount: partitionsCount,
	})
	_, err := tms.sendAndFetchResponse(message, CreatePartitionsCode)
	return err
}

func (tms *IggyTcpClient) DeletePartitions(streamId Identifier, topicId Identifier, partitionsCount uint32) error {
	message := binaryserialization.DeletePartitions(DeletePartitionsRequest{
		StreamId:        streamId,
		TopicId:         topicId,
		PartitionsCount: partitionsCount,
	})
	_, err := tms.sendAndFetchResponse(message, DeletePartitionsCode)
	return err
}
