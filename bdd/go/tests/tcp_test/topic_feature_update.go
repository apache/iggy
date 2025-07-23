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
	"math"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
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
			replicationFactor := uint8(1)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			err := client.UpdateTopic(
				streamIdentifier,
				topicIdentifier,
				newName,
				iggcon.CompressionAlgorithmNone,
				iggcon.Microsecond,
				math.MaxUint64,
				&replicationFactor)
			itShouldNotReturnError(err)
			itShouldSuccessfullyUpdateTopic(streamId, topicId, newName, client)
		})

		ginkgo.Context("and tries to create topic with duplicate topic name", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			_, topic1Name := successfullyCreateTopic(streamId, client)
			topic2Id, _ := successfullyCreateTopic(streamId, client)
			replicationFactor := uint8(1)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topic2Identifier, _ := iggcon.NewIdentifier(topic2Id)
			err := client.UpdateTopic(
				streamIdentifier,
				topic2Identifier,
				topic1Name,
				iggcon.CompressionAlgorithmNone,
				iggcon.IggyExpiryServerDefault,
				math.MaxUint64,
				&replicationFactor)

			itShouldReturnSpecificError(err, "topic_name_already_exists")
		})

		ginkgo.Context("and tries to update non-existing topic", func() {
			client := createAuthorizedConnection()
			replicationFactor := uint8(1)
			err := client.UpdateTopic(
				randomU32Identifier(),
				randomU32Identifier(),
				createRandomString(128),
				1,
				0,
				math.MaxUint64,
				&replicationFactor)

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})

		ginkgo.Context("and tries to update non-existing stream", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, createAuthorizedConnection())
			replicationFactor := uint8(1)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			err := client.UpdateTopic(
				streamIdentifier,
				randomU32Identifier(),
				createRandomString(128),
				1,
				0,
				math.MaxUint64,
				&replicationFactor)

			itShouldReturnSpecificError(err, "topic_id_not_found")
		})

		ginkgo.Context("and tries to update existing topic with a name that's over 255 characters", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, createAuthorizedConnection())
			topicId, _ := successfullyCreateTopic(streamId, client)
			replicationFactor := uint8(1)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			err := client.UpdateTopic(
				streamIdentifier,
				topicIdentifier,
				createRandomString(256),
				1,
				0,
				math.MaxUint64,
				&replicationFactor)

			itShouldReturnSpecificError(err, "topic_name_too_long")
		})
	})

	ginkgo.When("User is not logged in", func() {
		ginkgo.Context("and tries to update stream", func() {
			client := createClient()
			err := client.UpdateStream(randomU32Identifier(), createRandomString(128))

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
