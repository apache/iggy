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

var _ = Describe("LEAVE CONSUMER GROUP:", func() {
	prefix := "LeaveConsumerGroup"
	When("User is logged in", func() {
		Context("and tries to leave consumer group, that he is a part of", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, _ := successfullyCreateConsumer(streamId, topicId, client)
			successfullyJoinConsumer(streamId, topicId, groupId, client)

			err := client.LeaveConsumerGroup(
				iggcon.NewIdentifier(streamId),
				iggcon.NewIdentifier(topicId),
				iggcon.NewIdentifier(groupId),
			)

			itShouldNotReturnError(err)
			itShouldSuccessfullyLeaveConsumer(streamId, topicId, groupId, client)
		})

		Context("and tries to leave non-existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId := int(createRandomUInt32())
			err := client.LeaveConsumerGroup(
				iggcon.NewIdentifier(streamId),
				iggcon.NewIdentifier(topicId),
				iggcon.NewIdentifier(groupId),
			)

			itShouldReturnSpecificError(err, "consumer_group_not_found")
		})

		Context("and tries to leave consumer non-existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId := int(createRandomUInt32())

			err := client.LeaveConsumerGroup(
				iggcon.NewIdentifier(streamId),
				iggcon.NewIdentifier(topicId),
				iggcon.NewIdentifier(int(createRandomUInt32())),
			)

			itShouldReturnSpecificError(err, "topic_id_not_found")
		})

		Context("and tries to leave consumer for non-existing topic and stream", func() {
			client := createAuthorizedConnection()
			streamId := int(createRandomUInt32())
			topicId := int(createRandomUInt32())

			err := client.LeaveConsumerGroup(
				iggcon.NewIdentifier(streamId),
				iggcon.NewIdentifier(topicId),
				iggcon.NewIdentifier(int(createRandomUInt32())),
			)

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to leave to the consumer group", func() {
			client := createClient()
			err := client.LeaveConsumerGroup(
				iggcon.NewIdentifier(int(createRandomUInt32())),
				iggcon.NewIdentifier(int(createRandomUInt32())),
				iggcon.NewIdentifier(int(createRandomUInt32())),
			)

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
