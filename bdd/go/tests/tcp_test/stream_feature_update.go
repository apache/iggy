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
)

var _ = ginkgo.Describe("UPDATE STREAM:", func() {
	prefix := "UpdateStream"
	ginkgo.When("User is logged in", func() {
		ginkgo.Context("and tries to update existing stream with a valid name", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			newName := createRandomString(128)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			err := client.UpdateStream(streamIdentifier, newName)
			itShouldNotReturnError(err)
			itShouldSuccessfullyUpdateStream(streamId, newName, client)
		})

		ginkgo.Context("and tries to update stream with duplicate stream name", func() {
			client := createAuthorizedConnection()
			stream1Id, stream1Name := successfullyCreateStream(prefix, client)
			stream2Id, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(stream1Id, client)
			defer deleteStreamAfterTests(stream2Id, client)

			stream2Identifier, _ := iggcon.NewIdentifier(stream2Id)
			err := client.UpdateStream(stream2Identifier, stream1Name)

			itShouldReturnSpecificError(err, "stream_name_already_exists")
		})

		ginkgo.Context("and tries to update non-existing stream", func() {
			client := createAuthorizedConnection()
			err := client.UpdateStream(randomU32Identifier(), createRandomString(128))

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})

		ginkgo.Context("and tries to update existing stream with a name that's over 255 characters", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, createAuthorizedConnection())
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			err := client.UpdateStream(streamIdentifier, createRandomString(256))

			itShouldReturnSpecificError(err, "stream_name_too_long")
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
