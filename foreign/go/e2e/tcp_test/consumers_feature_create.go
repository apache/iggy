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
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("CREATE CONSUMER GROUP:", func() {
	prefix := "CreateConsumerGroup"
	When("User is logged in", func() {
		Context("and tries to create consumer group unique name and id", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)

			request := iggcon.CreateConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				ConsumerGroupId: int(createRandomUInt32()),
				Name:            createRandomString(16),
			}
			err := client.CreateConsumerGroup(request)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateConsumer(streamId, topicId, request.ConsumerGroupId, request.Name, client)
		})

		Context("and tries to create consumer group for a non existing stream", func() {
			client := createAuthorizedConnection()
			request := iggcon.CreateConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(int(createRandomUInt32())),
				TopicId:         iggcon.NewIdentifier(int(createRandomUInt32())),
				ConsumerGroupId: int(createRandomUInt32()),
				Name:            createRandomString(16),
			}
			err := client.CreateConsumerGroup(request)

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})

		Context("and tries to create consumer group for a non existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			request := iggcon.CreateConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(int(createRandomUInt32())),
				ConsumerGroupId: int(createRandomUInt32()),
				Name:            createRandomString(16),
			}
			err := client.CreateConsumerGroup(request)

			itShouldReturnSpecificError(err, "topic_id_not_found")
		})

		Context("and tries to create consumer group with duplicate group name", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			_, name := successfullyCreateConsumer(streamId, topicId, client)

			request := iggcon.CreateConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				ConsumerGroupId: int(createRandomUInt32()),
				Name:            name,
			}
			err := client.CreateConsumerGroup(request)

			itShouldReturnSpecificError(err, "cannot_create_consumer_groups_directory")
		})

		Context("and tries to create consumer group with duplicate group id", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, _ := successfullyCreateConsumer(streamId, topicId, client)

			request := iggcon.CreateConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				ConsumerGroupId: groupId,
				Name:            createRandomString(16),
			}
			err := client.CreateConsumerGroup(request)

			itShouldReturnSpecificError(err, "consumer_group_already_exists")
		})

		Context("and tries to create group with name that's over 255 characters", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)

			request := iggcon.CreateConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				ConsumerGroupId: int(createRandomUInt32()),
				Name:            createRandomString(256),
			}
			err := client.CreateConsumerGroup(request)

			itShouldReturnSpecificError(err, "consumer_group_name_too_long")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to create consumer group", func() {
			client := createConnection()
			request := iggcon.CreateConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(int(createRandomUInt32())),
				TopicId:         iggcon.NewIdentifier(int(createRandomUInt32())),
				ConsumerGroupId: int(createRandomUInt32()),
				Name:            createRandomString(16),
			}
			err := client.CreateConsumerGroup(request)

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
