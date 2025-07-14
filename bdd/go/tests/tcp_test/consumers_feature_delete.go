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
	"github.com/onsi/ginkgo/v2"
)

var _ = ginkgo.Describe("DELETE CONSUMER GROUP:", func() {
	prefix := "DeleteConsumerGroup"
	ginkgo.When("User is logged in", func() {
		ginkgo.Context("and tries to delete existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, _ := successfullyCreateConsumer(streamId, topicId, client)

			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			groupIdentifier, _ := iggcon.NewIdentifier(groupId)
			err := client.DeleteConsumerGroup(
				streamIdentifier,
				topicIdentifier,
				groupIdentifier,
			)

			itShouldNotReturnError(err)
			itShouldSuccessfullyDeletedConsumer(streamId, topicId, groupId, client)
		})

		ginkgo.Context("and tries to delete non-existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)

			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			err := client.DeleteConsumerGroup(
				streamIdentifier,
				topicIdentifier,
				randomU32Identifier(),
			)

			itShouldReturnSpecificIggyError(err, ierror.ConsumerGroupIdNotFound)
		})

		ginkgo.Context("and tries to delete consumer non-existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)

			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			err := client.DeleteConsumerGroup(
				streamIdentifier,
				randomU32Identifier(),
				randomU32Identifier(),
			)

			itShouldReturnSpecificError(err, "topic_id_not_found")
		})

		ginkgo.Context("and tries to delete consumer for non-existing topic and stream", func() {
			client := createAuthorizedConnection()
			err := client.DeleteConsumerGroup(
				randomU32Identifier(),
				randomU32Identifier(),
				randomU32Identifier(),
			)

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})
	})

	ginkgo.When("User is not logged in", func() {
		ginkgo.Context("and tries to delete consumer group", func() {
			client := createClient()
			err := client.DeleteConsumerGroup(
				randomU32Identifier(),
				randomU32Identifier(),
				randomU32Identifier(),
			)

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
