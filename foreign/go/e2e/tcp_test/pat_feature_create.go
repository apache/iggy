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
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("CREATE PAT:", func() {
	When("User is logged in", func() {
		Context("tries to create PAT with correct data", func() {
			client := createAuthorizedConnection()
			request := iggcon.CreateAccessTokenRequest{
				Name:   createRandomString(16),
				Expiry: 0,
			}

			response, err := client.CreateAccessToken(request)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateAccessToken(request.Name, client)
			itShouldBePossibleToLogInWithAccessToken(response.Token)
		})
	})

	When("User is not logged in", func() {
		Context("and tries to create PAT", func() {
			client := createConnection()
			request := iggcon.CreateAccessTokenRequest{
				Name:   createRandomString(16),
				Expiry: 0,
			}

			_, err := client.CreateAccessToken(request)
			itShouldReturnUnauthenticatedError(err)
		})
	})
})
