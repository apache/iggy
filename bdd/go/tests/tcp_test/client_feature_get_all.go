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
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("GET ALL CLIENT FEATURE:", func() {
	ginkgo.When("user is logged in", func() {
		ginkgo.Context("and tries to log with correct data", func() {
			client := createAuthorizedConnection()
			clients, err := client.GetClients()

			itShouldNotReturnError(err)
			ginkgo.It("should return stats", func() {
				gomega.Expect(clients).ToNot(gomega.BeNil())
			})

			ginkgo.It("should return at least one client", func() {
				gomega.Expect(len(clients)).ToNot(gomega.BeZero())
			})
		})
	})

	ginkgo.When("user is not logged in", func() {
		ginkgo.Context("and tries get all clients", func() {
			client := createClient()
			clients, err := client.GetClients()

			itShouldReturnUnauthenticatedError(err)
			ginkgo.It("should not return clients", func() {
				gomega.Expect(clients).To(gomega.BeNil())
			})
		})
	})
})
