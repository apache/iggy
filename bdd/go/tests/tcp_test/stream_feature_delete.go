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

var _ = Describe("DELETE STREAM:", func() {
	prefix := "DeleteStream"
	When("User is logged in", func() {
		Context("and tries to delete existing stream", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			err := client.DeleteStream(iggcon.NewIdentifier(streamId))

			itShouldNotReturnError(err)
			itShouldSuccessfullyDeleteStream(streamId, client)
		})

		Context("and tries to delete non-existing stream", func() {
			client := createAuthorizedConnection()
			streamId := int(createRandomUInt32())

			err := client.DeleteStream(iggcon.NewIdentifier(streamId))

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to delete stream", func() {
			client := createClient()
			err := client.DeleteStream(iggcon.NewIdentifier(int(createRandomUInt32())))

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
