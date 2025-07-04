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
	"github.com/apache/iggy/foreign/go/iggycli"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// OPERATIONS

func successfullyCreateUser(name string, client iggycli.Client) uint32 {
	_, err := client.CreateUser(
		name,
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
	itShouldNotReturnError(err)
	user, err := client.GetUser(iggcon.NewIdentifier(name))
	itShouldNotReturnError(err)

	return user.Id
}

// ASSERTIONS

func itShouldSuccessfullyCreateUser(name string, client iggycli.Client) {
	user, err := client.GetUser(iggcon.NewIdentifier(name))

	itShouldNotReturnError(err)

	It("should create user with name "+name, func() {
		Expect(user.Username).To(Equal(name))
	})
}

func itShouldSuccessfullyCreateUserWithPermissions(name string, client iggycli.Client, permissions map[int]*iggcon.StreamPermissions) {
	user, err := client.GetUser(iggcon.NewIdentifier(name))

	itShouldNotReturnError(err)

	It("should create user with name "+name, func() {
		Expect(user.Username).To(Equal(name))
	})

	It("should create user with correct permissions", func() {

		for streamId, streamPermission := range user.Permissions.Streams {

			Expect(streamPermission.ManageStream).To(Equal(permissions[streamId].ManageStream))
			Expect(streamPermission.ReadStream).To(Equal(permissions[streamId].ReadStream))
			Expect(streamPermission.ManageTopics).To(Equal(permissions[streamId].ManageTopics))
			Expect(streamPermission.ReadTopics).To(Equal(permissions[streamId].ReadTopics))
			Expect(streamPermission.PollMessages).To(Equal(permissions[streamId].PollMessages))
			Expect(streamPermission.SendMessages).To(Equal(permissions[streamId].SendMessages))

			for topicId, topicPermission := range streamPermission.Topics {
				Expect(topicPermission.ManageTopic).To(Equal(permissions[streamId].Topics[topicId].ManageTopic))
				Expect(topicPermission.ReadTopic).To(Equal(permissions[streamId].Topics[topicId].ReadTopic))
				Expect(topicPermission.PollMessages).To(Equal(permissions[streamId].Topics[topicId].PollMessages))
				Expect(topicPermission.SendMessages).To(Equal(permissions[streamId].Topics[topicId].SendMessages))
			}
		}
	})
}

func itShouldSuccessfullyUpdateUser(id uint32, name string, client iggycli.Client) {
	user, err := client.GetUser(iggcon.NewIdentifier(name))

	itShouldNotReturnError(err)

	It("should update user with id "+string(rune(id)), func() {
		Expect(user.Id).To(Equal(id))
	})

	It("should update user with name "+name, func() {
		Expect(user.Username).To(Equal(name))
	})
}

func itShouldSuccessfullyDeleteUser(userId int, client iggycli.Client) {
	user, err := client.GetUser(iggcon.NewIdentifier(userId))

	itShouldReturnSpecificError(err, "resource_not_found")
	It("should not return user", func() {
		Expect(user).To(BeNil())
	})
}

func itShouldSuccessfullyUpdateUserPermissions(userId uint32, client iggycli.Client) {
	user, err := client.GetUser(iggcon.NewIdentifier(int(userId)))

	itShouldNotReturnError(err)

	It("should update user permissions with id "+string(rune(userId)), func() {
		Expect(user.Permissions.Global.ManageServers).To(BeFalse())
		Expect(user.Permissions.Global.ReadServers).To(BeFalse())
		Expect(user.Permissions.Global.ManageUsers).To(BeFalse())
		Expect(user.Permissions.Global.ReadUsers).To(BeFalse())
		Expect(user.Permissions.Global.ManageStreams).To(BeFalse())
		Expect(user.Permissions.Global.ReadStreams).To(BeFalse())
		Expect(user.Permissions.Global.ManageTopics).To(BeFalse())
		Expect(user.Permissions.Global.ReadTopics).To(BeFalse())
		Expect(user.Permissions.Global.PollMessages).To(BeFalse())
		Expect(user.Permissions.Global.SendMessages).To(BeFalse())
	})
}

func itShouldBePossibleToLogInWithCredentials(username string, password string) {
	ms := createClient()
	userId, err := ms.LoginUser(username, password)

	itShouldNotReturnError(err)
	It("should return userId", func() {
		Expect(userId).NotTo(BeNil())
	})
}

func itShouldReturnSpecificUser(name string, user iggcon.UserInfo) {
	It("should fetch user with name "+name, func() {
		Expect(user.Username).To(Equal(name))
	})
}

func itShouldContainSpecificUser(name string, users []iggcon.UserInfo) {
	It("should fetch at least one user", func() {
		Expect(len(users)).NotTo(Equal(0))
	})

	var user iggcon.UserInfo
	found := false

	for _, s := range users {
		if s.Username == name {
			user = s
			found = true
			break
		}
	}

	It("should fetch user with name "+name, func() {
		Expect(found).To(BeTrue(), "User with name %s not found", name)
		Expect(user.Username).To(Equal(name))
	})
}

//CLEANUP

func deleteUserAfterTests(name any, client iggycli.Client) {
	_ = client.DeleteUser(iggcon.NewIdentifier(name))
}
