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
	"fmt"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/iggycli"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// operations
func successfullyCreateConsumer(streamId uint32, topicId uint32, cli iggycli.Client) (uint32, string) {
	name := createRandomString(16)
	streamIdentifier, _ := iggcon.NewIdentifier(streamId)
	topicIdentifier, _ := iggcon.NewIdentifier(topicId)
    group, err := cli.CreateConsumerGroup(
		streamIdentifier,
		topicIdentifier,
        name)
    groupId := group.Id
    itShouldSuccessfullyCreateConsumer(streamId, topicId, groupId, name, cli)
	itShouldNotReturnError(err)
	return groupId, name
}

func successfullyJoinConsumer(streamId uint32, topicId uint32, groupId uint32, client iggycli.Client) {
	streamIdentifier, _ := iggcon.NewIdentifier(streamId)
	topicIdentifier, _ := iggcon.NewIdentifier(topicId)
	groupIdentifier, _ := iggcon.NewIdentifier(groupId)
	err := client.JoinConsumerGroup(
		streamIdentifier,
		topicIdentifier,
		groupIdentifier,
	)

	itShouldSuccessfullyJoinConsumer(streamId, topicId, groupId, client)
	itShouldNotReturnError(err)
}

//assertions

func itShouldReturnSpecificConsumer(id uint32, name string, consumer *iggcon.ConsumerGroup) {
	ginkgo.It("should fetch consumer with id "+string(rune(id)), func() {
		gomega.Expect(consumer).NotTo(gomega.BeNil())
		gomega.Expect(consumer.Id).To(gomega.Equal(id))
	})

	ginkgo.It("should fetch consumer with name "+name, func() {
		gomega.Expect(consumer).NotTo(gomega.BeNil())
		gomega.Expect(consumer.Name).To(gomega.Equal(name))
	})
}

func itShouldContainSpecificConsumer(id uint32, name string, consumers []iggcon.ConsumerGroup) {
	ginkgo.It("should fetch at least one consumer", func() {
		gomega.Expect(len(consumers)).NotTo(gomega.Equal(0))
	})

	var consumer iggcon.ConsumerGroup
	found := false

	for _, s := range consumers {
		if s.Id == id && s.Name == name {
			consumer = s
			found = true
			break
		}
	}

	ginkgo.It(fmt.Sprintf("should fetch consumer with id %d", id), func() {
		gomega.Expect(found).To(gomega.BeTrue(), "Consumer with id %d and name %s not found", id, name)
		gomega.Expect(consumer.Id).To(gomega.Equal(id))
	})

	ginkgo.It("should fetch consumer with name "+name, func() {
		gomega.Expect(found).To(gomega.BeTrue(), "Consumer with id %d and name %s not found", id, name)
		gomega.Expect(consumer.Name).To(gomega.Equal(name))
	})
}

func itShouldSuccessfullyCreateConsumer(streamId uint32, topicId uint32, groupId uint32, expectedName string, client iggycli.Client) {
	streamIdentifier, _ := iggcon.NewIdentifier(streamId)
	topicIdentifier, _ := iggcon.NewIdentifier(topicId)
	groupIdentifier, _ := iggcon.NewIdentifier(groupId)
	consumer, err := client.GetConsumerGroup(streamIdentifier, topicIdentifier, groupIdentifier)
	ginkgo.It("should create consumer with id "+string(rune(groupId)), func() {
		gomega.Expect(consumer).NotTo(gomega.BeNil())
		gomega.Expect(consumer.Id).To(gomega.Equal(groupId))
	})

	ginkgo.It("should create consumer with name "+expectedName, func() {
		gomega.Expect(consumer).NotTo(gomega.BeNil())
		gomega.Expect(consumer.Name).To(gomega.Equal(expectedName))
	})
	itShouldNotReturnError(err)
}

func itShouldSuccessfullyDeletedConsumer(streamId uint32, topicId uint32, groupId uint32, client iggycli.Client) {
	streamIdentifier, _ := iggcon.NewIdentifier(streamId)
	topicIdentifier, _ := iggcon.NewIdentifier(topicId)
	groupIdentifier, _ := iggcon.NewIdentifier(groupId)
	consumer, err := client.GetConsumerGroup(streamIdentifier, topicIdentifier, groupIdentifier)
	itShouldReturnSpecificError(err, ierror.ErrConsumerGroupIdNotFound)
	ginkgo.It("should not return consumer", func() {
		gomega.Expect(consumer).To(gomega.BeNil())
	})
}

func itShouldSuccessfullyJoinConsumer(streamId uint32, topicId uint32, groupId uint32, client iggycli.Client) {
	streamIdentifier, _ := iggcon.NewIdentifier(streamId)
	topicIdentifier, _ := iggcon.NewIdentifier(topicId)
	groupIdentifier, _ := iggcon.NewIdentifier(groupId)
	consumer, err := client.GetConsumerGroup(streamIdentifier, topicIdentifier, groupIdentifier)

	ginkgo.It("should join consumer with id "+string(rune(groupId)), func() {
		gomega.Expect(consumer).NotTo(gomega.BeNil())
		gomega.Expect(consumer.MembersCount).ToNot(gomega.Equal(0))
	})

	ginkgo.It("should contain 1 member with 2 partitions", func() {
		gomega.Expect(len(consumer.Members)).To(gomega.Equal(1))
		gomega.Expect(consumer.Members[0].PartitionsCount).To(gomega.Equal(uint32(2)))
		gomega.Expect(len(consumer.Members[0].Partitions)).To(gomega.Equal(2))
	})

	itShouldNotReturnError(err)
}

func itShouldSuccessfullyLeaveConsumer(streamId uint32, topicId uint32, groupId uint32, client iggycli.Client) {
	streamIdentifier, _ := iggcon.NewIdentifier(streamId)
	topicIdentifier, _ := iggcon.NewIdentifier(topicId)
	groupIdentifier, _ := iggcon.NewIdentifier(groupId)
	consumer, err := client.GetConsumerGroup(streamIdentifier, topicIdentifier, groupIdentifier)
	ginkgo.It("should leave consumer with id "+string(rune(groupId)), func() {
		gomega.Expect(consumer).NotTo(gomega.BeNil())
		gomega.Expect(consumer.MembersCount).To(gomega.Equal(uint32(0)))
	})

	itShouldNotReturnError(err)
}
