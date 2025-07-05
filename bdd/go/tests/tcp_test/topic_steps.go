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
	"math"
	"strconv"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/iggycli"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

//operations

func successfullyCreateTopic(streamId int, client iggycli.Client) (int, string) {
	topicId := int(createRandomUInt32())
	replicationFactor := uint8(1)
	name := createRandomString(128)
	_, err := client.CreateTopic(
		iggcon.NewIdentifier(streamId),
		name,
		2,
		1,
		0,
		math.MaxUint64,
		&replicationFactor,
		&topicId)

	itShouldSuccessfullyCreateTopic(streamId, topicId, name, client)
	itShouldNotReturnError(err)
	return topicId, name
}

//assertions

func itShouldReturnSpecificTopic(id int, name string, topic iggcon.TopicDetails) {
	It("should fetch topic with id "+string(rune(id)), func() {
		Expect(topic.Id).To(Equal(id))
	})

	It("should fetch topic with name "+name, func() {
		Expect(topic.Name).To(Equal(name))
	})
}

func itShouldContainSpecificTopic(id int, name string, topics []iggcon.Topic) {
	It("should fetch at least one topic", func() {
		Expect(len(topics)).NotTo(Equal(0))
	})

	var topic iggcon.Topic
	found := false

	for _, s := range topics {
		if s.Id == id && s.Name == name {
			topic = s
			found = true
			break
		}
	}

	It("should fetch topic with id "+strconv.Itoa(id), func() {
		Expect(found).To(BeTrue(), "Topic with id %d and name %s not found", id, name)
		Expect(topic.Id).To(Equal(id))
	})

	It("should fetch topic with name "+name, func() {
		Expect(found).To(BeTrue(), "Topic with id %d and name %s not found", id, name)
		Expect(topic.Name).To(Equal(name))
	})
}

func itShouldSuccessfullyCreateTopic(streamId int, topicId int, expectedName string, client iggycli.Client) {
	topic, err := client.GetTopic(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId))
	It("should create topic with id "+string(rune(topicId)), func() {
		Expect(topic).NotTo(BeNil())
		Expect(topic.Id).To(Equal(topicId))
	})

	It("should create topic with name "+expectedName, func() {
		Expect(topic).NotTo(BeNil())
		Expect(topic.Name).To(Equal(expectedName))
	})
	itShouldNotReturnError(err)
}

func itShouldSuccessfullyUpdateTopic(streamId int, topicId int, expectedName string, client iggycli.Client) {
	topic, err := client.GetTopic(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId))

	It("should update topic with id "+string(rune(topicId)), func() {
		Expect(topic).NotTo(BeNil())
		Expect(topic.Id).To(Equal(topicId))
	})

	It("should update topic with name "+expectedName, func() {
		Expect(topic).NotTo(BeNil())
		Expect(topic.Name).To(Equal(expectedName))
	})
	itShouldNotReturnError(err)
}

func itShouldSuccessfullyDeleteTopic(streamId int, topicId int, client iggycli.Client) {
	topic, err := client.GetTopic(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId))

	itShouldReturnSpecificIggyError(err, ierror.TopicIdNotFound)
	It("should not return topic", func() {
		Expect(topic).To(BeNil())
	})
}
