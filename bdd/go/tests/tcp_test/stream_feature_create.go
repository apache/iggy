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
	"github.com/onsi/ginkgo/v2"
)

var _ = ginkgo.Describe("CREATE STREAM:", func() {
	ginkgo.When("User is logged in", func() {
		ginkgo.Context("and tries to create stream with unique name and id", func() {
			client := createAuthorizedConnection()
			streamId := createRandomUInt32()
			name := createRandomString(32)

			_, err := client.CreateStream(name, &streamId)
			defer deleteStreamAfterTests(streamId, client)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateStream(streamId, name, client)
		})

		ginkgo.Context("and tries to create stream with duplicate stream name", func() {
			client := createAuthorizedConnection()
			streamId := createRandomUInt32()
			name := createRandomString(32)

			_, err := client.CreateStream(name, &streamId)
			defer deleteStreamAfterTests(streamId, client)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateStream(streamId, name, client)

			anotherStreamId := createRandomUInt32()
			_, err = client.CreateStream(name, &anotherStreamId)

			itShouldReturnSpecificError(err, "stream_name_already_exists")
		})

		ginkgo.Context("and tries to create stream with duplicate stream id", func() {
			client := createAuthorizedConnection()
			streamId := createRandomUInt32()
			name := createRandomString(32)

			_, err := client.CreateStream(name, &streamId)
			defer deleteStreamAfterTests(streamId, client)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateStream(streamId, name, client)

			_, err = client.CreateStream(createRandomString(32), &streamId)

			itShouldReturnSpecificError(err, "stream_id_already_exists")
		})

		ginkgo.Context("and tries to create stream name that's over 255 characters", func() {
			client := createAuthorizedConnection()
			streamId := createRandomUInt32()
			name := createRandomString(256)

			_, err := client.CreateStream(name, &streamId)

			itShouldReturnSpecificError(err, "stream_name_too_long")
		})
	})

	ginkgo.When("User is not logged in", func() {
		ginkgo.Context("and tries to create stream", func() {
			client := createClient()
			streamId := createRandomUInt32()
			_, err := client.CreateStream(createRandomString(32), &streamId)

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
