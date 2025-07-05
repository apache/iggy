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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("STAT FEATURE:", func() {
	When("user is logged in", func() {
		Context("and tries to log with correct data", func() {
			client := createAuthorizedConnection()
			stats, err := client.GetStats()

			itShouldNotReturnError(err)
			It("should return stats", func() {
				Expect(stats).ToNot(BeNil())
			})
		})
	})

	// When("user is not logged in", func() {
	// 	Context("and tries get iggy statistics", func() {
	// 		client := createConnection()
	// 		stats, err := client.GetStats()

	// 		itShouldReturnUnauthenticatedError(err)
	// 		It("should not return stats", func() {
	// 			Expect(stats).To(BeNil())
	// 		})
	// 	})
	// })
})
