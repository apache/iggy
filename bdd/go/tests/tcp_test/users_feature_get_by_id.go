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

var _ = ginkgo.Describe("GET USER:", func() {
	ginkgo.When("User is logged in", func() {
		ginkgo.Context("tries to get existing user", func() {
			client := createAuthorizedConnection()
			name := createRandomString(16)
			userId := successfullyCreateUser(name, client)
			defer deleteUserAfterTests(int(userId), client)

			user, err := client.GetUser(iggcon.NewIdentifier(int(userId)))

			itShouldNotReturnError(err)
			itShouldReturnSpecificUser(name, user.UserInfo)
		})
	})

	// ! TODO: review if needed to implement into sdk
	// When("User is not logged in", func() {
	//		Context("and tries to get user", func() {
	//			client := createConnection()
	//			_, err := client.GetUser(iggcon.NewIdentifier(int(createRandomUInt32())))
	//			itShouldReturnUnauthenticatedError(err)
	//		})
	//	})
})
