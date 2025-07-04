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

var _ = Describe("GET STREAM BY ID:", func() {
	prefix := "GetStream"
	When("User is logged in", func() {
		Context("and tries to get existing stream", func() {
			client := createAuthorizedConnection()
			streamId, name := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			stream, err := client.GetStream(iggcon.NewIdentifier(streamId))

			itShouldNotReturnError(err)
			itShouldReturnSpecificStream(streamId, name, *stream)
		})

		Context("and tries to get non-existing stream", func() {
			client := createAuthorizedConnection()
			streamId := int(createRandomUInt32())

			_, err := client.GetStream(iggcon.NewIdentifier(streamId))

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})
	})

	// ! TODO: review if needed to implement into sdk
	// When("User is not logged in", func() {
	// 	Context("and tries to get stream by id", func() {
	// 		client := createConnection()
	// 		_, err := client.GetStreamById(iggcon.GetStreamRequest{StreamID: iggcon.NewIdentifier(int(createRandomUInt32()))})

	// 		itShouldReturnUnauthenticatedError(err)
	// 	})
	// })
})
