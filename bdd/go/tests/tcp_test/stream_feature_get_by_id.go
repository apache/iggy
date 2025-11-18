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
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("GET STREAM BY ID:", func() {
	prefix := "GetStream"
	ginkgo.When("User is logged in", func() {
		ginkgo.Context("and tries to get existing stream", func() {
			client := createAuthorizedConnection()
			streamId, name := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			stream, err := client.GetStream(streamIdentifier)

			itShouldNotReturnError(err)
			itShouldReturnSpecificStream(streamId, name, *stream)
		})

		ginkgo.Context("and tries to get non-existing stream", func() {
			client := createAuthorizedConnection()

			_, err := client.GetStream(randomU32Identifier())

			itShouldReturnSpecificError(err, ierror.ErrStreamIdNotFound)
		})

		ginkgo.Context("and tries to get stream after creating some topics", func() {
			client := createAuthorizedConnection()
			streamId, name := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)

			// create two topics
			t1Name := createRandomString(32)
			t2Name := createRandomString(32)
            t1, err := client.CreateTopic(streamIdentifier,
				t1Name,
				2,
				iggcon.CompressionAlgorithmNone,
				iggcon.Millisecond,
				math.MaxUint64,
                nil)
			itShouldNotReturnError(err)
            t2, err := client.CreateTopic(
				streamIdentifier,
				t2Name,
				2,
				iggcon.CompressionAlgorithmNone,
				iggcon.Millisecond,
				math.MaxUint64,
                nil)
			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateTopic(streamId, t1.Id, t1Name, client)
			itShouldSuccessfullyCreateTopic(streamId, t2.Id, t2Name, client)

			// check stream details
			stream, err := client.GetStream(streamIdentifier)
			itShouldNotReturnError(err)
			itShouldReturnSpecificStream(streamId, name, *stream)
			ginkgo.It("should have exactly 2 topics", func() {
				gomega.Expect(len(stream.Topics)).To(gomega.Equal(2))
			})
			itShouldContainSpecificTopic(t1.Id, t1Name, stream.Topics)
			itShouldContainSpecificTopic(t2.Id, t2Name, stream.Topics)
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
