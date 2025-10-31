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

var _ = ginkgo.Describe("CREATE CONSUMER GROUP:", func() {
	prefix := "CreateConsumerGroup"
	ginkgo.When("User is logged in", func() {
		ginkgo.Context("and tries to create consumer group unique name and id", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			name := createRandomString(16)
			group, err := client.CreateConsumerGroup(
				streamIdentifier,
				topicIdentifier,
				name,
			)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateConsumer(streamId, topicId, group.Id, name, client)
		})

		ginkgo.Context("and tries to create consumer group for a non existing stream", func() {
			client := createAuthorizedConnection()
			_, err := client.CreateConsumerGroup(
				randomU32Identifier(),
				randomU32Identifier(),
				createRandomString(16),
			)

			itShouldReturnSpecificError(err, ierror.ErrStreamIdNotFound)
		})

		ginkgo.Context("and tries to create consumer group for a non existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			_, err := client.CreateConsumerGroup(
				streamIdentifier,
				randomU32Identifier(),
				createRandomString(16),
			)

			itShouldReturnSpecificError(err, ierror.ErrTopicIdNotFound)
		})

		ginkgo.Context("and tries to create consumer group with duplicate group name", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			_, name := successfullyCreateConsumer(streamId, topicId, client)

			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			_, err := client.CreateConsumerGroup(
				streamIdentifier,
				topicIdentifier,
				name,
			)
			itShouldReturnSpecificError(err, ierror.ErrConsumerGroupNameAlreadyExists)
		})

		ginkgo.Context("and tries to create group with name that's over 255 characters", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)

			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			_, err := client.CreateConsumerGroup(
				streamIdentifier,
				topicIdentifier,
				createRandomString(256),
			)

			itShouldReturnSpecificError(err, ierror.ErrInvalidConsumerGroupName)
		})
	})

	ginkgo.When("User is not logged in", func() {
		ginkgo.Context("and tries to create consumer group", func() {
			client := createClient()
			_, err := client.CreateConsumerGroup(
				randomU32Identifier(),
				randomU32Identifier(),
				createRandomString(16),
			)

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
