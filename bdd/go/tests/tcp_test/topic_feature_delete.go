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

var _ = ginkgo.Describe("DELETE TOPIC:", func() {
	prefix := "DeleteTopic"
	ginkgo.When("User is logged in", func() {
		ginkgo.Context("and tries to delete existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			err := client.DeleteTopic(streamIdentifier, topicIdentifier)

			itShouldNotReturnError(err)
			itShouldSuccessfullyDeleteTopic(streamId, topicId, client)
		})

		ginkgo.Context("and tries to delete non-existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			err := client.DeleteTopic(streamIdentifier, randomU32Identifier())

			itShouldReturnSpecificIggyError(err, ierror.TopicIdNotFound)
		})

		ginkgo.Context("and tries to delete non-existing topic and stream", func() {
			client := createAuthorizedConnection()

			err := client.DeleteTopic(randomU32Identifier(), randomU32Identifier())

			itShouldReturnSpecificIggyError(err, ierror.StreamIdNotFound)
		})
	})

	ginkgo.When("User is not logged in", func() {
		ginkgo.Context("and tries to delete topic", func() {
			client := createClient()
			err := client.DeleteTopic(randomU32Identifier(), randomU32Identifier())

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
