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
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("UPDATE TOPIC:", func() {
	prefix := "UpdateTopic"
	When("User is logged in", func() {
		Context("and tries to update existing topic with a valid data", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			newName := createRandomString(128)
			request := iggcon.UpdateTopicRequest{
				TopicId:              iggcon.NewIdentifier(topicId),
				StreamId:             iggcon.NewIdentifier(streamId),
				Name:                 newName,
				MessageExpiry:        1,
				CompressionAlgorithm: 1,
				MaxTopicSize:         math.MaxUint64,
				ReplicationFactor:    1,
			}
			err := client.UpdateTopic(request)
			itShouldNotReturnError(err)
			itShouldSuccessfullyUpdateTopic(streamId, topicId, newName, client)
		})

		Context("and tries to create topic with duplicate topic name", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			_, topic1Name := successfullyCreateTopic(streamId, client)
			topic2Id, _ := successfullyCreateTopic(streamId, client)

			request := iggcon.UpdateTopicRequest{
				TopicId:              iggcon.NewIdentifier(topic2Id),
				StreamId:             iggcon.NewIdentifier(streamId),
				Name:                 topic1Name,
				MessageExpiry:        0,
				CompressionAlgorithm: 1,
				MaxTopicSize:         math.MaxUint64,
				ReplicationFactor:    1,
			}
			err := client.UpdateTopic(request)

			itShouldReturnSpecificError(err, "topic_name_already_exists")
		})

		Context("and tries to update non-existing topic", func() {
			client := createAuthorizedConnection()
			streamId := int(createRandomUInt32())
			topicId := int(createRandomUInt32())
			request := iggcon.UpdateTopicRequest{
				TopicId:              iggcon.NewIdentifier(topicId),
				StreamId:             iggcon.NewIdentifier(streamId),
				Name:                 createRandomString(128),
				MessageExpiry:        0,
				CompressionAlgorithm: 1,
				MaxTopicSize:         math.MaxUint64,
				ReplicationFactor:    1,
			}
			err := client.UpdateTopic(request)

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})

		Context("and tries to update non-existing stream", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, createAuthorizedConnection())
			topicId := int(createRandomUInt32())
			request := iggcon.UpdateTopicRequest{
				TopicId:              iggcon.NewIdentifier(topicId),
				StreamId:             iggcon.NewIdentifier(streamId),
				Name:                 createRandomString(128),
				MessageExpiry:        0,
				CompressionAlgorithm: 1,
				MaxTopicSize:         math.MaxUint64,
				ReplicationFactor:    1,
			}
			err := client.UpdateTopic(request)

			itShouldReturnSpecificError(err, "topic_id_not_found")
		})

		Context("and tries to update existing topic with a name that's over 255 characters", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, createAuthorizedConnection())
			topicId, _ := successfullyCreateTopic(streamId, client)
			request := iggcon.UpdateTopicRequest{
				TopicId:              iggcon.NewIdentifier(topicId),
				StreamId:             iggcon.NewIdentifier(streamId),
				Name:                 createRandomString(256),
				MessageExpiry:        0,
				CompressionAlgorithm: 1,
				MaxTopicSize:         math.MaxUint64,
				ReplicationFactor:    1,
			}

			err := client.UpdateTopic(request)

			itShouldReturnSpecificError(err, "topic_name_too_long")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to update stream", func() {
			client := createConnection()
			err := client.UpdateStream(iggcon.UpdateStreamRequest{
				StreamId: iggcon.NewIdentifier(int(createRandomUInt32())),
				Name:     createRandomString(128),
			})

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
