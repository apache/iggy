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
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/onsi/ginkgo/v2"
)

var _ = ginkgo.Describe("CREATE STREAM:", func() {
	ginkgo.When("User is logged in", func() {
		ginkgo.Context("and tries to create stream with unique name and id", func() {
			client := createAuthorizedConnection()
			name := createRandomString(32)

            stream, err := client.CreateStream(name)
			defer deleteStreamAfterTests(stream.Id, client)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateStream(stream.Id, name, client)
		})

		ginkgo.Context("and tries to create stream with duplicate stream name", func() {
			client := createAuthorizedConnection()
			name := createRandomString(32)

            stream, err := client.CreateStream(name)
			defer deleteStreamAfterTests(stream.Id, client)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateStream(stream.Id, name, client)

            _, err = client.CreateStream(name)

			itShouldReturnSpecificError(err, ierror.ErrStreamNameAlreadyExists)
		})

		ginkgo.Context("and tries to create stream name that's over 255 characters", func() {
            client := createAuthorizedConnection()
            name := createRandomString(256)

            _, err := client.CreateStream(name)

            itShouldReturnSpecificError(err, ierror.ErrInvalidStreamName)
        })
	})

	ginkgo.When("User is not logged in", func() {
		ginkgo.Context("and tries to create stream", func() {
            client := createClient()
            _, err := client.CreateStream(createRandomString(32))

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
