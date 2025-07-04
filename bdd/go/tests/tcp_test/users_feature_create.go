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

var _ = Describe("CREATE USER:", func() {
	When("User is logged in", func() {
		Context("tries to create user with correct data", func() {
			client := createAuthorizedConnection()

			username := createRandomString(16)
			_, err := client.CreateUser(
				username,
				createRandomString(16),
				iggcon.Active,
				&iggcon.Permissions{
					Global: iggcon.GlobalPermissions{
						ManageServers: true,
						ReadServers:   true,
						ManageUsers:   true,
						ReadUsers:     true,
						ManageStreams: true,
						ReadStreams:   true,
						ManageTopics:  true,
						ReadTopics:    true,
						PollMessages:  true,
						SendMessages:  true,
					},
				})
			defer deleteUserAfterTests(username, client)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateUser(username, client)
			//itShouldBePossibleToLogInWithCredentials(request.Username, request.Password)
		})

		Context("tries to create user with correct data and custom permissions", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream("ss", client)
			topicId, _ := successfullyCreateTopic(streamId, client)

			topicPermissionRequest := iggcon.TopicPermissions{
				ManageTopic:  false,
				ReadTopic:    true,
				PollMessages: true,
				SendMessages: true,
			}
			streamPermissionRequest := iggcon.StreamPermissions{
				ManageStream: false,
				ReadStream:   true,
				ManageTopics: true,
				ReadTopics:   true,
				PollMessages: false,
				SendMessages: true,
				Topics: map[int]*iggcon.TopicPermissions{
					int(topicId): &topicPermissionRequest,
				},
			}

			userStreamPermissions := map[int]*iggcon.StreamPermissions{
				int(streamId): &streamPermissionRequest,
			}

			username := createRandomString(16)
			_, err := client.CreateUser(
				username,
				createRandomString(16),
				iggcon.Active,
				&iggcon.Permissions{
					Global: iggcon.GlobalPermissions{
						ManageServers: true,
						ReadServers:   true,
						ManageUsers:   true,
						ReadUsers:     true,
						ManageStreams: true,
						ReadStreams:   true,
						ManageTopics:  true,
						ReadTopics:    true,
						PollMessages:  true,
						SendMessages:  true,
					},
					Streams: userStreamPermissions,
				})
			defer deleteUserAfterTests(username, client)
			defer deleteStreamAfterTests(streamId, client)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateUserWithPermissions(username, client, userStreamPermissions)
			//itShouldBePossibleToLogInWithCredentials(request.Username, request.Password)
		})
	})

	When("User is not logged in", func() {
		Context("and tries to create user", func() {
			client := createClient()

			_, err := client.CreateUser(
				createRandomString(16),
				createRandomString(16),
				iggcon.Active,
				&iggcon.Permissions{
					Global: iggcon.GlobalPermissions{
						ManageServers: true,
						ReadServers:   true,
						ManageUsers:   true,
						ReadUsers:     true,
						ManageStreams: true,
						ReadStreams:   true,
						ManageTopics:  true,
						ReadTopics:    true,
						PollMessages:  true,
						SendMessages:  true,
					},
				})

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
