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
	"strconv"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/iggycli"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// operations
func successfullyCreateConsumer(streamId int, topicId int, cli iggycli.Client) (int, string) {
	groupId := createRandomUInt32()
	name := createRandomString(16)
	_, err := cli.CreateConsumerGroup(iggcon.NewIdentifier(streamId),
		iggcon.NewIdentifier(topicId),
		name,
		&groupId,
	)

	itShouldSuccessfullyCreateConsumer(streamId, topicId, int(groupId), name, cli)
	itShouldNotReturnError(err)
	return int(groupId), name
}

func successfullyJoinConsumer(streamId int, topicId int, groupId int, client iggycli.Client) {

	err := client.JoinConsumerGroup(
		iggcon.NewIdentifier(streamId),
		iggcon.NewIdentifier(topicId),
		iggcon.NewIdentifier(groupId),
	)

	itShouldSuccessfullyJoinConsumer(streamId, topicId, groupId, client)
	itShouldNotReturnError(err)
}

//assertions

func itShouldReturnSpecificConsumer(id int, name string, consumer *iggcon.ConsumerGroup) {
	It("should fetch consumer with id "+string(rune(id)), func() {
		Expect(consumer).NotTo(BeNil())
		Expect(consumer.Id).To(Equal(id))
	})

	It("should fetch consumer with name "+name, func() {
		Expect(consumer).NotTo(BeNil())
		Expect(consumer.Name).To(Equal(name))
	})
}

func itShouldContainSpecificConsumer(id int, name string, consumers []iggcon.ConsumerGroup) {
	It("should fetch at least one consumer", func() {
		Expect(len(consumers)).NotTo(Equal(0))
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

	It("should fetch consumer with id "+strconv.Itoa(id), func() {
		Expect(found).To(BeTrue(), "Consumer with id %d and name %s not found", id, name)
		Expect(consumer.Id).To(Equal(id))
	})

	It("should fetch consumer with name "+name, func() {
		Expect(found).To(BeTrue(), "Consumer with id %d and name %s not found", id, name)
		Expect(consumer.Name).To(Equal(name))
	})
}

func itShouldSuccessfullyCreateConsumer(streamId int, topicId int, groupId int, expectedName string, client iggycli.Client) {
	consumer, err := client.GetConsumerGroup(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId), iggcon.NewIdentifier(groupId))

	It("should create consumer with id "+string(rune(groupId)), func() {
		Expect(consumer).NotTo(BeNil())
		Expect(consumer.Id).To(Equal(groupId))
	})

	It("should create consumer with name "+expectedName, func() {
		Expect(consumer).NotTo(BeNil())
		Expect(consumer.Name).To(Equal(expectedName))
	})
	itShouldNotReturnError(err)
}

func itShouldSuccessfullyDeletedConsumer(streamId int, topicId int, groupId int, client iggycli.Client) {
	consumer, err := client.GetConsumerGroup(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId), iggcon.NewIdentifier(groupId))

	itShouldReturnSpecificError(err, "consumer_group_not_found")
	It("should not return consumer", func() {
		Expect(consumer).To(BeNil())
	})
}

func itShouldSuccessfullyJoinConsumer(streamId int, topicId int, groupId int, client iggycli.Client) {
	consumer, err := client.GetConsumerGroup(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId), iggcon.NewIdentifier(groupId))

	It("should join consumer with id "+string(rune(groupId)), func() {
		Expect(consumer).NotTo(BeNil())
		Expect(consumer.MembersCount).ToNot(Equal(0))
	})

	itShouldNotReturnError(err)
}

func itShouldSuccessfullyLeaveConsumer(streamId int, topicId int, groupId int, client iggycli.Client) {
	consumer, err := client.GetConsumerGroup(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId), iggcon.NewIdentifier(groupId))

	It("should leave consumer with id "+string(rune(groupId)), func() {
		Expect(consumer).NotTo(BeNil())
		Expect(consumer.MembersCount).To(Equal(0))
	})

	itShouldNotReturnError(err)
}
