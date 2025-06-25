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
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("CREATE PARTITION:", func() {
	prefix := "CreatePartition"
	When("User is logged in", func() {
		Context("and tries to create partitions for existing stream", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)

			request := iggcon.CreatePartitionsRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				PartitionsCount: 10,
			}
			err := client.CreatePartition(request)

			itShouldNotReturnError(err)
			itShouldHaveExpectedNumberOfPartitions(streamId, topicId, request.PartitionsCount+2, client)
		})

		Context("and tries to create partitions for a non existing stream", func() {
			client := createAuthorizedConnection()
			request := iggcon.CreatePartitionsRequest{
				StreamId:        iggcon.NewIdentifier(int(createRandomUInt32())),
				TopicId:         iggcon.NewIdentifier(int(createRandomUInt32())),
				PartitionsCount: 10,
			}
			err := client.CreatePartition(request)

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})

		Context("and tries to create partitions for a non existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			request := iggcon.CreatePartitionsRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(int(createRandomUInt32())),
				PartitionsCount: 10,
			}
			err := client.CreatePartition(request)

			itShouldReturnSpecificError(err, "topic_id_not_found")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to create partitions", func() {
			client := createConnection()
			request := iggcon.CreatePartitionsRequest{
				StreamId:        iggcon.NewIdentifier(int(createRandomUInt32())),
				TopicId:         iggcon.NewIdentifier(int(createRandomUInt32())),
				PartitionsCount: 10,
			}
			err := client.CreatePartition(request)

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
