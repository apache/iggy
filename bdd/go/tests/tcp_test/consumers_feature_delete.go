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
	ierror "github.com/apache/iggy/foreign/go/errors"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("DELETE CONSUMER GROUP:", func() {
	prefix := "DeleteConsumerGroup"
	When("User is logged in", func() {
		Context("and tries to delete existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, _ := successfullyCreateConsumer(streamId, topicId, client)
			err := client.DeleteConsumerGroup(
				iggcon.NewIdentifier(streamId),
				iggcon.NewIdentifier(topicId),
				iggcon.NewIdentifier(groupId),
			)

			itShouldNotReturnError(err)
			itShouldSuccessfullyDeletedConsumer(streamId, topicId, groupId, client)
		})

		Context("and tries to delete non-existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId := int(createRandomUInt32())
			err := client.DeleteConsumerGroup(
				iggcon.NewIdentifier(streamId),
				iggcon.NewIdentifier(topicId),
				iggcon.NewIdentifier(groupId),
			)

			itShouldReturnSpecificIggyError(err, ierror.ConsumerGroupIdNotFound)
		})

		Context("and tries to delete consumer non-existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId := int(createRandomUInt32())

			err := client.DeleteConsumerGroup(
				iggcon.NewIdentifier(streamId),
				iggcon.NewIdentifier(topicId),
				iggcon.NewIdentifier(int(createRandomUInt32())),
			)

			itShouldReturnSpecificError(err, "topic_id_not_found")
		})

		Context("and tries to delete consumer for non-existing topic and stream", func() {
			client := createAuthorizedConnection()
			streamId := int(createRandomUInt32())
			topicId := int(createRandomUInt32())

			err := client.DeleteConsumerGroup(
				iggcon.NewIdentifier(streamId),
				iggcon.NewIdentifier(topicId),
				iggcon.NewIdentifier(int(createRandomUInt32())),
			)

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to delete consumer group", func() {
			client := createClient()
			err := client.DeleteConsumerGroup(
				iggcon.NewIdentifier(int(createRandomUInt32())),
				iggcon.NewIdentifier(int(createRandomUInt32())),
				iggcon.NewIdentifier(int(createRandomUInt32())),
			)

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
