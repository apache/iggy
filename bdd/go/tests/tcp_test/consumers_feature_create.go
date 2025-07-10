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
	"github.com/onsi/ginkgo/v2"
)

var _ = ginkgo.Describe("CREATE CONSUMER GROUP:", func() {
	prefix := "CreateConsumerGroup"
	ginkgo.When("User is logged in", func() {
		ginkgo.Context("and tries to create consumer group unique name and id", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId := createRandomUInt32()
			name := createRandomString(16)
			_, err := client.CreateConsumerGroup(
				iggcon.NewIdentifier(streamId),
				iggcon.NewIdentifier(topicId),
				name,
				&groupId,
			)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateConsumer(streamId, topicId, int(groupId), name, client)
		})

		ginkgo.Context("and tries to create consumer group for a non existing stream", func() {
			client := createAuthorizedConnection()
			groupId := createRandomUInt32()
			_, err := client.CreateConsumerGroup(
				iggcon.NewIdentifier(int(createRandomUInt32())),
				iggcon.NewIdentifier(int(createRandomUInt32())),
				createRandomString(16),
				&groupId)

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})

		ginkgo.Context("and tries to create consumer group for a non existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			groupId := createRandomUInt32()
			_, err := client.CreateConsumerGroup(
				iggcon.NewIdentifier(streamId),
				iggcon.NewIdentifier(int(createRandomUInt32())),
				createRandomString(16),
				&groupId,
			)

			itShouldReturnSpecificError(err, "topic_id_not_found")
		})

		ginkgo.Context("and tries to create consumer group with duplicate group name", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			_, name := successfullyCreateConsumer(streamId, topicId, client)

			groupId := createRandomUInt32()
			_, err := client.CreateConsumerGroup(
				iggcon.NewIdentifier(streamId),
				iggcon.NewIdentifier(topicId),
				name,
				&groupId,
			)

			itShouldReturnSpecificError(err, "cannot_create_consumer_groups_directory")
		})

		ginkgo.Context("and tries to create consumer group with duplicate group id", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, _ := successfullyCreateConsumer(streamId, topicId, client)

			uint32GroupId := uint32(groupId)
			_, err := client.CreateConsumerGroup(
				iggcon.NewIdentifier(streamId),
				iggcon.NewIdentifier(topicId),
				createRandomString(16),
				&uint32GroupId)

			itShouldReturnSpecificError(err, "consumer_group_already_exists")
		})

		ginkgo.Context("and tries to create group with name that's over 255 characters", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)

			groupId := createRandomUInt32()
			_, err := client.CreateConsumerGroup(iggcon.NewIdentifier(streamId),
				iggcon.NewIdentifier(topicId),
				createRandomString(256),
				&groupId)

			itShouldReturnSpecificError(err, "consumer_group_name_too_long")
		})
	})

	ginkgo.When("User is not logged in", func() {
		ginkgo.Context("and tries to create consumer group", func() {
			client := createClient()
			groupId := createRandomUInt32()
			_, err := client.CreateConsumerGroup(
				iggcon.NewIdentifier(int(createRandomUInt32())),
				iggcon.NewIdentifier(int(createRandomUInt32())),
				createRandomString(16),
				&groupId,
			)

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
