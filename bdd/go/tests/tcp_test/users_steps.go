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
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
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
	nameIdentifier, _ := iggcon.NewIdentifier(name)
	user, err := client.GetUser(nameIdentifier)
	itShouldNotReturnError(err)

	return user.Id
}

// ASSERTIONS

func itShouldSuccessfullyCreateUser(name string, client iggycli.Client) {
	nameIdentifier, _ := iggcon.NewIdentifier(name)
	user, err := client.GetUser(nameIdentifier)

	itShouldNotReturnError(err)

	ginkgo.It("should create user with name "+name, func() {
		gomega.Expect(user.Username).To(gomega.Equal(name))
	})
}

func itShouldSuccessfullyCreateUserWithPermissions(name string, client iggycli.Client, permissions map[int]*iggcon.StreamPermissions) {
	nameIdentifier, _ := iggcon.NewIdentifier(name)
	user, err := client.GetUser(nameIdentifier)

	itShouldNotReturnError(err)

	ginkgo.It("should create user with name "+name, func() {
		gomega.Expect(user.Username).To(gomega.Equal(name))
	})

	ginkgo.It("should create user with correct permissions", func() {

		for streamId, streamPermission := range user.Permissions.Streams {

			gomega.Expect(streamPermission.ManageStream).To(gomega.Equal(permissions[streamId].ManageStream))
			gomega.Expect(streamPermission.ReadStream).To(gomega.Equal(permissions[streamId].ReadStream))
			gomega.Expect(streamPermission.ManageTopics).To(gomega.Equal(permissions[streamId].ManageTopics))
			gomega.Expect(streamPermission.ReadTopics).To(gomega.Equal(permissions[streamId].ReadTopics))
			gomega.Expect(streamPermission.PollMessages).To(gomega.Equal(permissions[streamId].PollMessages))
			gomega.Expect(streamPermission.SendMessages).To(gomega.Equal(permissions[streamId].SendMessages))

			for topicId, topicPermission := range streamPermission.Topics {
				gomega.Expect(topicPermission.ManageTopic).To(gomega.Equal(permissions[streamId].Topics[topicId].ManageTopic))
				gomega.Expect(topicPermission.ReadTopic).To(gomega.Equal(permissions[streamId].Topics[topicId].ReadTopic))
				gomega.Expect(topicPermission.PollMessages).To(gomega.Equal(permissions[streamId].Topics[topicId].PollMessages))
				gomega.Expect(topicPermission.SendMessages).To(gomega.Equal(permissions[streamId].Topics[topicId].SendMessages))
			}
		}
	})
}

func itShouldSuccessfullyUpdateUser(id uint32, name string, client iggycli.Client) {
	nameIdentifier, _ := iggcon.NewIdentifier(name)
	user, err := client.GetUser(nameIdentifier)

	itShouldNotReturnError(err)

	ginkgo.It("should update user with id "+string(rune(id)), func() {
		gomega.Expect(user.Id).To(gomega.Equal(id))
	})

	ginkgo.It("should update user with name "+name, func() {
		gomega.Expect(user.Username).To(gomega.Equal(name))
	})
}

func itShouldSuccessfullyDeleteUser(userId uint32, client iggycli.Client) {
	identifier, _ := iggcon.NewIdentifier(userId)
	user, err := client.GetUser(identifier)

	itShouldReturnSpecificError(err, "resource_not_found")
	ginkgo.It("should not return user", func() {
		gomega.Expect(user).To(gomega.BeNil())
	})
}

func itShouldSuccessfullyUpdateUserPermissions(userId uint32, client iggycli.Client) {
	identifier, _ := iggcon.NewIdentifier(userId)
	user, err := client.GetUser(identifier)

	itShouldNotReturnError(err)

	ginkgo.It("should update user permissions with id "+string(rune(userId)), func() {
		gomega.Expect(user.Permissions.Global.ManageServers).To(gomega.BeFalse())
		gomega.Expect(user.Permissions.Global.ReadServers).To(gomega.BeFalse())
		gomega.Expect(user.Permissions.Global.ManageUsers).To(gomega.BeFalse())
		gomega.Expect(user.Permissions.Global.ReadUsers).To(gomega.BeFalse())
		gomega.Expect(user.Permissions.Global.ManageStreams).To(gomega.BeFalse())
		gomega.Expect(user.Permissions.Global.ReadStreams).To(gomega.BeFalse())
		gomega.Expect(user.Permissions.Global.ManageTopics).To(gomega.BeFalse())
		gomega.Expect(user.Permissions.Global.ReadTopics).To(gomega.BeFalse())
		gomega.Expect(user.Permissions.Global.PollMessages).To(gomega.BeFalse())
		gomega.Expect(user.Permissions.Global.SendMessages).To(gomega.BeFalse())
	})
}

func itShouldReturnSpecificUser(name string, user iggcon.UserInfo) {
	ginkgo.It("should fetch user with name "+name, func() {
		gomega.Expect(user.Username).To(gomega.Equal(name))
	})
}

func itShouldContainSpecificUser(name string, users []iggcon.UserInfo) {
	ginkgo.It("should fetch at least one user", func() {
		gomega.Expect(len(users)).NotTo(gomega.Equal(0))
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

	ginkgo.It("should fetch user with name "+name, func() {
		gomega.Expect(found).To(gomega.BeTrue(), "User with name %s not found", name)
		gomega.Expect(user.Username).To(gomega.Equal(name))
	})
}

//CLEANUP

func deleteUserAfterTests(identifier iggcon.Identifier, client iggycli.Client) {
	_ = client.DeleteUser(identifier)
}
