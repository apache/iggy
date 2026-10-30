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
	"context"
	"math"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/onsi/ginkgo/v2"
)

var _ = ginkgo.Describe("UPDATE TOPIC:", func() {
	prefix := "UpdateTopic"
	ginkgo.When("User is logged in", func() {
		ginkgo.Context("and tries to update existing topic with a valid data", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			newName := createRandomString(128)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			expiry := iggcon.Microsecond
			maxTopicSize := uint64(math.MaxUint64)
			err := client.UpdateTopic(
				context.Background(),
				streamIdentifier,
				topicIdentifier,
				iggcon.TopicUpdate{Name: &newName, MessageExpiry: &expiry, MaxTopicSize: &maxTopicSize})
			itShouldNotReturnError(err)
			itShouldSuccessfullyUpdateTopic(streamId, topicId, newName, client)
		})

		ginkgo.Context("and tries to create topic with duplicate topic name", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			_, topic1Name := successfullyCreateTopic(streamId, client)
			topic2Id, _ := successfullyCreateTopic(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topic2Identifier, _ := iggcon.NewIdentifier(topic2Id)
			maxTopicSize := uint64(math.MaxUint64)
			err := client.UpdateTopic(
				context.Background(),
				streamIdentifier,
				topic2Identifier,
				iggcon.TopicUpdate{Name: &topic1Name, MaxTopicSize: &maxTopicSize})

			itShouldReturnSpecificError(err, ierror.ErrTopicNameAlreadyExists)
		})

		ginkgo.Context("and tries to update non-existing topic", func() {
			client := createAuthorizedConnection()
			name := createRandomString(128)
			maxTopicSize := uint64(math.MaxUint64)
			err := client.UpdateTopic(
				context.Background(),
				randomU32Identifier(),
				randomU32Identifier(),
				iggcon.TopicUpdate{Name: &name, MaxTopicSize: &maxTopicSize})

			itShouldReturnSpecificError(err, ierror.ErrStreamIdNotFound)
		})

		ginkgo.Context("and tries to update non-existing stream", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, createAuthorizedConnection())
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			name := createRandomString(128)
			maxTopicSize := uint64(math.MaxUint64)
			err := client.UpdateTopic(
				context.Background(),
				streamIdentifier,
				randomU32Identifier(),
				iggcon.TopicUpdate{Name: &name, MaxTopicSize: &maxTopicSize})

			itShouldReturnSpecificError(err, ierror.ErrTopicIdNotFound)
		})

		ginkgo.Context("and tries to update existing topic with a name that's over 255 characters", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, createAuthorizedConnection())
			topicId, _ := successfullyCreateTopic(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			name := createRandomString(256)
			maxTopicSize := uint64(math.MaxUint64)
			err := client.UpdateTopic(
				context.Background(),
				streamIdentifier,
				topicIdentifier,
				iggcon.TopicUpdate{Name: &name, MaxTopicSize: &maxTopicSize})

			itShouldReturnSpecificError(err, ierror.ErrInvalidTopicName)
		})
	})

	ginkgo.When("User is not logged in", func() {
		ginkgo.Context("and tries to update stream", func() {
			client := createClient()
			name := createRandomString(128)
			err := client.UpdateStream(context.Background(), randomU32Identifier(), iggcon.StreamUpdate{Name: &name})

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
