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

var _ = ginkgo.Describe("DELETE CONSUMER OFFSET:", func() {
	prefix := "DeleteConsumerOffset"
	ginkgo.When("User is logged in", func() {
		ginkgo.Context("and deletes an existing consumer offset after storing it", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, _ := successfullyCreateConsumer(streamId, topicId, client)

			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			groupIdentifier, _ := iggcon.NewIdentifier(groupId)

			partitionId := uint32(0)
			testOffset := uint64(1)
			consumer := iggcon.NewSingleConsumer(groupIdentifier)

			// send test messages
			messages := createDefaultMessages()
			err := client.SendMessages(
				streamIdentifier,
				topicIdentifier,
				iggcon.PartitionId(partitionId),
				messages,
			)
			itShouldNotReturnError(err)

			// store consumer offset
			err = client.StoreConsumerOffset(
				consumer,
				streamIdentifier,
				topicIdentifier,
				testOffset,
				&partitionId,
			)
			itShouldNotReturnError(err)

			// verify offset was stored
			storedOffset, err := client.GetConsumerOffset(
				consumer, streamIdentifier, topicIdentifier, &partitionId,
			)
			itShouldNotReturnError(err)
			itShouldReturnStoredConsumerOffset(storedOffset, partitionId, testOffset)

			// delete the offset
			err = client.DeleteConsumerOffset(
				consumer, streamIdentifier, topicIdentifier, &partitionId,
			)
			itShouldNotReturnError(err)

			// verify offset was deleted
			storedOffset, err = client.GetConsumerOffset(
				consumer, streamIdentifier, topicIdentifier, &partitionId,
			)
			itShouldNotReturnError(err)
			itShouldReturnNilOffsetForNewConsumerGroup(storedOffset)
		})

		ginkgo.Context("and attempts to delete a non-existing consumer offset", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, _ := successfullyCreateConsumer(streamId, topicId, client)

			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			groupIdentifier, _ := iggcon.NewIdentifier(groupId)

			partitionId := uint32(1)
			consumer := iggcon.NewSingleConsumer(groupIdentifier)

			err := client.DeleteConsumerOffset(
				consumer,
				streamIdentifier,
				topicIdentifier,
				&partitionId,
			)
			itShouldReturnSpecificError(err, ierror.ErrConsumerOffsetNotFound)
		})

		ginkgo.Context("and attempts to delete an offset from a non-existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)

			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			consumer := iggcon.NewGroupConsumer(randomU32Identifier())
			partitionId := uint32(1)

			err := client.DeleteConsumerOffset(
				consumer,
				streamIdentifier,
				topicIdentifier,
				&partitionId,
			)

			itShouldReturnSpecificError(err, ierror.ErrConsumerGroupIdNotFound)
		})

		ginkgo.Context("and attempts to delete an offset from a non-existing stream", func() {
			client := createAuthorizedConnection()
			consumer := iggcon.NewGroupConsumer(randomU32Identifier())
			partitionId := uint32(1)

			err := client.DeleteConsumerOffset(
				consumer,
				randomU32Identifier(),
				randomU32Identifier(),
				&partitionId)
			itShouldReturnSpecificError(err, ierror.ErrStreamIdNotFound)
		})
	})

	ginkgo.When("User is not logged in", func() {
		ginkgo.Context("and attempts to delete a consumer offset", func() {
			client := createClient()
			consumer := iggcon.NewGroupConsumer(randomU32Identifier())
			partitionId := uint32(1)

			err := client.DeleteConsumerOffset(
				consumer, randomU32Identifier(), randomU32Identifier(), &partitionId,
			)
			itShouldReturnUnauthenticatedError(err)
		})
	})
})
