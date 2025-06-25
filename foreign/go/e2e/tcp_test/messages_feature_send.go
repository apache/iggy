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

package tcp_test

import (
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("SEND MESSAGES:", func() {
	prefix := "SendMessages"
	When("User is logged in", func() {
		Context("and tries to send messages to the topic with balanced partitioning", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream("1"+prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			messages := createDefaultMessages()
			request := iggcon.SendMessagesRequest{
				StreamId:     iggcon.NewIdentifier(streamId),
				TopicId:      iggcon.NewIdentifier(topicId),
				Partitioning: iggcon.None(),
				Messages:     messages,
			}
			err := client.SendMessages(request)
			itShouldNotReturnError(err)
			itShouldSuccessfullyPublishMessages(streamId, topicId, messages, client)
		})

		Context("and tries to send messages to the non existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream("2"+prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			messages := createDefaultMessages()
			request := iggcon.SendMessagesRequest{
				StreamId:     iggcon.NewIdentifier(streamId),
				TopicId:      iggcon.NewIdentifier(int(createRandomUInt32())),
				Partitioning: iggcon.None(),
				Messages:     messages,
			}
			err := client.SendMessages(request)
			itShouldReturnSpecificError(err, "topic_id_not_found")
		})

		Context("and tries to send messages to the non existing stream", func() {
			client := createAuthorizedConnection()
			messages := createDefaultMessages()
			request := iggcon.SendMessagesRequest{
				StreamId:     iggcon.NewIdentifier(int(createRandomUInt32())),
				TopicId:      iggcon.NewIdentifier(int(createRandomUInt32())),
				Partitioning: iggcon.None(),
				Messages:     messages,
			}
			err := client.SendMessages(request)
			itShouldReturnSpecificError(err, "stream_id_not_found")
		})

		Context("and tries to send messages to non existing partition", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream("3"+prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			messages := createDefaultMessages()
			request := iggcon.SendMessagesRequest{
				StreamId:     iggcon.NewIdentifier(streamId),
				TopicId:      iggcon.NewIdentifier(topicId),
				Partitioning: iggcon.PartitionId(int(createRandomUInt32())),
				Messages:     messages,
			}
			err := client.SendMessages(request)
			itShouldReturnSpecificError(err, "partition_not_found")
		})

		Context("and tries to send messages to valid topic but with 0 messages in payload", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, createAuthorizedConnection())
			topicId, _ := successfullyCreateTopic(streamId, client)
			request := iggcon.SendMessagesRequest{
				StreamId:     iggcon.NewIdentifier(streamId),
				TopicId:      iggcon.NewIdentifier(topicId),
				Partitioning: iggcon.PartitionId(int(createRandomUInt32())),
				Messages:     []iggcon.IggyMessage{},
			}
			err := client.SendMessages(request)
			itShouldReturnSpecificError(err, "messages_count_should_be_greater_than_zero")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to update stream", func() {
			client := createConnection()
			messages := createDefaultMessages()
			request := iggcon.SendMessagesRequest{
				StreamId:     iggcon.NewIdentifier(int(createRandomUInt32())),
				TopicId:      iggcon.NewIdentifier(int(createRandomUInt32())),
				Partitioning: iggcon.None(),
				Messages:     messages,
			}
			err := client.SendMessages(request)

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
