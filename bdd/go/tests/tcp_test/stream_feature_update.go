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

var _ = Describe("UPDATE STREAM:", func() {
	prefix := "UpdateStream"
	When("User is logged in", func() {
		Context("and tries to update existing stream with a valid name", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			newName := createRandomString(128)

			err := client.UpdateStream(iggcon.NewIdentifier(streamId), newName)
			itShouldNotReturnError(err)
			itShouldSuccessfullyUpdateStream(streamId, newName, client)
		})

		Context("and tries to update stream with duplicate stream name", func() {
			client := createAuthorizedConnection()
			stream1Id, stream1Name := successfullyCreateStream(prefix, client)
			stream2Id, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(stream1Id, client)
			defer deleteStreamAfterTests(stream2Id, client)

			err := client.UpdateStream(iggcon.NewIdentifier(stream2Id), stream1Name)

			itShouldReturnSpecificError(err, "stream_name_already_exists")
		})

		Context("and tries to update non-existing stream", func() {
			client := createAuthorizedConnection()
			err := client.UpdateStream(iggcon.NewIdentifier(int(createRandomUInt32())), createRandomString(128))

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})

		Context("and tries to update existing stream with a name that's over 255 characters", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, createAuthorizedConnection())

			err := client.UpdateStream(iggcon.NewIdentifier(streamId), createRandomString(256))

			itShouldReturnSpecificError(err, "stream_name_too_long")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to update stream", func() {
			client := createClient()
			err := client.UpdateStream(iggcon.NewIdentifier(int(createRandomUInt32())), createRandomString(128))

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
