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

var _ = ginkgo.Describe("SEND MESSAGES:", func() {
	prefix := "SendMessages"
	ginkgo.When("User is logged in", func() {
		ginkgo.Context("and tries to send messages to the topic with balanced partitioning", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream("1"+prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			messages := createDefaultMessages()
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			err := client.SendMessages(
				streamIdentifier,
				topicIdentifier,
				iggcon.None(),
				messages,
			)
			itShouldNotReturnError(err)
			itShouldSuccessfullyPublishMessages(streamId, topicId, messages, client)
		})

		ginkgo.Context("and tries to send messages to the non existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream("2"+prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			messages := createDefaultMessages()
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			err := client.SendMessages(
				streamIdentifier,
				randomU32Identifier(),
				iggcon.None(),
				messages,
			)
			itShouldReturnSpecificError(err, ierror.ErrTopicIdNotFound)
		})

		ginkgo.Context("and tries to send messages to the non existing stream", func() {
			client := createAuthorizedConnection()
			messages := createDefaultMessages()
			err := client.SendMessages(
				randomU32Identifier(),
				randomU32Identifier(),
				iggcon.None(),
				messages,
			)
			itShouldReturnSpecificError(err, ierror.ErrStreamIdNotFound)
		})

		ginkgo.Context("and tries to send messages to non existing partition", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream("3"+prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			messages := createDefaultMessages()
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			err := client.SendMessages(
				streamIdentifier,
				topicIdentifier,
				iggcon.PartitionId(createRandomUInt32()),
				messages,
			)
			itShouldReturnSpecificError(err, ierror.ErrPartitionNotFound)
		})

		ginkgo.Context("and tries to send messages to valid topic but with 0 messages in payload", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, createAuthorizedConnection())
			topicId, _ := successfullyCreateTopic(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			err := client.SendMessages(
				streamIdentifier,
				topicIdentifier,
				iggcon.PartitionId(createRandomUInt32()),
				[]iggcon.IggyMessage{},
			)
			itShouldReturnSpecificError(err, ierror.ErrInvalidMessagesCount)
		})
	})
})
