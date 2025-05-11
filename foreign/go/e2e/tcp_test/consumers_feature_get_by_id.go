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
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	ierror "github.com/iggy-rs/iggy-go-client/errors"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("GET CONSUMER GROUP BY ID:", func() {
	prefix := "GetConsumerGroupById"
	When("User is logged in", func() {
		Context("and tries to get existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, name := successfullyCreateConsumer(streamId, topicId, client)
			group, err := client.GetConsumerGroupById(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId), iggcon.NewIdentifier(groupId))

			itShouldNotReturnError(err)
			itShouldReturnSpecificConsumer(groupId, name, group)
		})

		Context("and tries to get consumer from non-existing stream", func() {
			client := createAuthorizedConnection()

			_, err := client.GetConsumerGroupById(
				iggcon.NewIdentifier(int(createRandomUInt32())),
				iggcon.NewIdentifier(int(createRandomUInt32())),
				iggcon.NewIdentifier(int(createRandomUInt32())))

			itShouldReturnSpecificIggyError(err, ierror.ConsumerGroupIdNotFound)
		})

		Context("and tries to get consumer from non-existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)

			_, err := client.GetConsumerGroupById(
				iggcon.NewIdentifier(streamId),
				iggcon.NewIdentifier(int(createRandomUInt32())),
				iggcon.NewIdentifier(int(createRandomUInt32())))

			itShouldReturnSpecificIggyError(err, ierror.ConsumerGroupIdNotFound)
		})

		Context("and tries to get from non-existing consumer", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)

			_, err := client.GetConsumerGroupById(
				iggcon.NewIdentifier(streamId),
				iggcon.NewIdentifier(topicId),
				iggcon.NewIdentifier(int(createRandomUInt32())))

			itShouldReturnSpecificIggyError(err, ierror.ConsumerGroupIdNotFound)
		})
	})

	// ! TODO: review if needed to implement into sdk
	// When("User is not logged in", func() {
	// 	Context("and tries to get topic by id", func() {
	// 		client := createConnection()
	// 		_, err := client.GetConsumerGroupById(
	// 			iggcon.NewIdentifier(int(createRandomUInt32())),
	// 			iggcon.NewIdentifier(int(createRandomUInt32())),
	// 			iggcon.NewIdentifier(int(createRandomUInt32())))

	// 		itShouldReturnUnauthenticatedError(err)
	// 	})
	// })
})
