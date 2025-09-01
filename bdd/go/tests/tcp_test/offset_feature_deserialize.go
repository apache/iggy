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
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("GET CONSUMER OFFSET:", func() {
	prefix := "GetConsumerOffset"

	ginkgo.When("User is logged in", func() {
		ginkgo.Context("and tries to get offset for existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, _ := successfullyCreateConsumer(streamId, topicId, client)

			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			groupIdentifier, _ := iggcon.NewIdentifier(groupId)

			joinErr := client.JoinConsumerGroup(
				streamIdentifier,
				topicIdentifier,
				groupIdentifier,
			)

			consumer := iggcon.NewGroupConsumer(groupIdentifier)
			partitionId := uint32(1)

			offset, err := client.GetConsumerOffset(
				consumer,
				streamIdentifier,
				topicIdentifier,
				&partitionId,
			)

			itShouldNotReturnError(joinErr)
			itShouldNotReturnError(err)
			itShouldReturnNilOffsetForNewConsumerGroup(offset)
		})

		ginkgo.Context("and gets valid offset after sending messages and storing offset", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix+"Success", client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, _ := successfullyCreateConsumer(streamId, topicId, client)

			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			consumerIdentifier, _ := iggcon.NewIdentifier(groupId)

			partitionId := uint32(1)
			testOffset := uint64(1)

			consumer := iggcon.NewSingleConsumer(consumerIdentifier)

			messages := createDefaultMessages()
			sendErr := client.SendMessages(
				streamIdentifier,
				topicIdentifier,
				iggcon.PartitionId(partitionId),
				messages,
			)

			storeErr := client.StoreConsumerOffset(
				consumer,
				streamIdentifier,
				topicIdentifier,
				testOffset,
				&partitionId,
			)

			offset, getErr := client.GetConsumerOffset(
				consumer,
				streamIdentifier,
				topicIdentifier,
				&partitionId,
			)

			itShouldNotReturnError(sendErr)
			itShouldNotReturnError(storeErr)
			itShouldNotReturnError(getErr)
			itShouldReturnStoredConsumerOffset(offset, partitionId, testOffset)
		})

		ginkgo.Context("and tries to store and retrieve consumer offset", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix+"Store", client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, _ := successfullyCreateConsumer(streamId, topicId, client)

			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			groupIdentifier, _ := iggcon.NewIdentifier(groupId)

			joinErr := client.JoinConsumerGroup(
				streamIdentifier,
				topicIdentifier,
				groupIdentifier,
			)

			consumer := iggcon.NewGroupConsumer(groupIdentifier)
			partitionId := uint32(1)

			// Don't store any offset - we want to test that a new consumer group has no stored offset

			storedOffset, getErr := client.GetConsumerOffset(
				consumer,
				streamIdentifier,
				topicIdentifier,
				&partitionId,
			)

			itShouldNotReturnError(joinErr)
			itShouldNotReturnError(getErr)
			itShouldReturnNilOffsetForNewConsumerGroup(storedOffset)
		})

		ginkgo.Context("and tries to get offset from non-existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)

			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			consumer := iggcon.NewGroupConsumer(randomU32Identifier())
			partitionId := uint32(1)

			offset, err := client.GetConsumerOffset(
				consumer,
				streamIdentifier,
				topicIdentifier,
				&partitionId,
			)

			itShouldNotReturnError(err)
			itShouldReturnNilOffsetForNewConsumerGroup(offset)
		})

		ginkgo.Context("and tries to get offset from non-existing stream", func() {
			client := createAuthorizedConnection()
			consumer := iggcon.NewGroupConsumer(randomU32Identifier())
			partitionId := uint32(1)

			offset, err := client.GetConsumerOffset(
				consumer,
				randomU32Identifier(),
				randomU32Identifier(),
				&partitionId,
			)

			itShouldNotReturnError(err)
			itShouldReturnNilOffsetForNewConsumerGroup(offset)
		})
	})

	ginkgo.When("User is not logged in", func() {
		ginkgo.Context("and tries to get consumer offset", func() {
			client := createClient()
			consumer := iggcon.NewGroupConsumer(randomU32Identifier())
			partitionId := uint32(1)

			offset, err := client.GetConsumerOffset(
				consumer,
				randomU32Identifier(),
				randomU32Identifier(),
				&partitionId,
			)

			itShouldNotReturnError(err)
			itShouldReturnNilOffsetForNewConsumerGroup(offset)
		})
	})
})

func itShouldReturnNilOffsetForNewConsumerGroup(offset *iggcon.ConsumerOffsetInfo) {
	ginkgo.It("should return nil offset for new consumer group with no stored offset", func() {
		gomega.Expect(offset).To(gomega.BeNil(), "Offset should be nil for new consumer group")
	})
}

func itShouldReturnStoredConsumerOffset(offset *iggcon.ConsumerOffsetInfo, expectedPartitionId uint32, expectedStoredOffset uint64) {
	ginkgo.It("should return the stored consumer offset", func() {
		gomega.Expect(offset).NotTo(gomega.BeNil(), "Offset should not be nil")
		gomega.Expect(offset.PartitionId).To(gomega.Equal(expectedPartitionId), "PartitionId should match")
		gomega.Expect(offset.StoredOffset).To(gomega.Equal(expectedStoredOffset), "StoredOffset should match the value we set")
	})
}
