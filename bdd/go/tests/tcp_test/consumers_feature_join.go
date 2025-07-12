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

var _ = ginkgo.Describe("JOIN CONSUMER GROUP:", func() {
	prefix := "JoinConsumerGroup"
	ginkgo.When("User is logged in", func() {
		ginkgo.Context("and tries to join existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, _ := successfullyCreateConsumer(streamId, topicId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			groupIdentifier, _ := iggcon.NewIdentifier(groupId)
			err := client.JoinConsumerGroup(
				streamIdentifier,
				topicIdentifier,
				groupIdentifier,
			)

			itShouldNotReturnError(err)
			itShouldSuccessfullyJoinConsumer(streamId, topicId, groupId, client)
		})

		ginkgo.Context("and tries to join non-existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			err := client.JoinConsumerGroup(
				streamIdentifier,
				topicIdentifier,
				randomU32Identifier(),
			)

			itShouldReturnSpecificError(err, "consumer_group_not_found")
		})

		ginkgo.Context("and tries to join consumer non-existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			err := client.JoinConsumerGroup(
				streamIdentifier,
				randomU32Identifier(),
				randomU32Identifier(),
			)

			itShouldReturnSpecificError(err, "topic_id_not_found")
		})

		ginkgo.Context("and tries to join consumer for non-existing topic and stream", func() {
			client := createAuthorizedConnection()
			err := client.JoinConsumerGroup(
				randomU32Identifier(),
				randomU32Identifier(),
				randomU32Identifier(),
			)

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})
	})

	ginkgo.When("User is not logged in", func() {
		ginkgo.Context("and tries to join to the consumer group", func() {
			client := createClient()
			err := client.JoinConsumerGroup(
				randomU32Identifier(),
				randomU32Identifier(),
				randomU32Identifier(),
			)

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
