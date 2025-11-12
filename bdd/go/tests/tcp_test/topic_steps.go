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
	"math"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/iggycli"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

//operations

func successfullyCreateTopic(streamId uint32, client iggycli.Client) (uint32, string) {
	replicationFactor := uint8(1)
	name := createRandomString(128)
	streamIdentifier, _ := iggcon.NewIdentifier(streamId)
    topic, err := client.CreateTopic(
		streamIdentifier,
		name,
		2,
		1,
		0,
		math.MaxUint64,
        &replicationFactor)

    itShouldSuccessfullyCreateTopic(streamId, topic.Id, name, client)
	itShouldNotReturnError(err)
    return topic.Id, name
}

//assertions

func itShouldReturnSpecificTopic(id uint32, name string, topic iggcon.TopicDetails) {
	ginkgo.It("should fetch topic with id "+string(rune(id)), func() {
		gomega.Expect(topic.Id).To(gomega.Equal(id))
	})

	ginkgo.It("should fetch topic with name "+name, func() {
		gomega.Expect(topic.Name).To(gomega.Equal(name))
	})
}

func itShouldContainSpecificTopic(id uint32, name string, topics []iggcon.Topic) {
	ginkgo.It("should fetch at least one topic", func() {
		gomega.Expect(len(topics)).NotTo(gomega.Equal(0))
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

	ginkgo.It(fmt.Sprintf("should fetch topic with id %d", id), func() {
		gomega.Expect(found).To(gomega.BeTrue(), "Topic with id %d and name %s not found", id, name)
		gomega.Expect(topic.Id).To(gomega.Equal(id))
	})

	ginkgo.It("should fetch topic with name "+name, func() {
		gomega.Expect(found).To(gomega.BeTrue(), "Topic with id %d and name %s not found", id, name)
		gomega.Expect(topic.Name).To(gomega.Equal(name))
	})
}

func itShouldSuccessfullyCreateTopic(streamId uint32, topicId uint32, expectedName string, client iggycli.Client) {
	streamIdentifier, _ := iggcon.NewIdentifier(streamId)
	topicIdentifier, _ := iggcon.NewIdentifier(topicId)
	topic, err := client.GetTopic(streamIdentifier, topicIdentifier)
	ginkgo.It("should create topic with id "+string(rune(topicId)), func() {
		gomega.Expect(topic).NotTo(gomega.BeNil())
		gomega.Expect(topic.Id).To(gomega.Equal(topicId))
	})

	ginkgo.It("should create topic with name "+expectedName, func() {
		gomega.Expect(topic).NotTo(gomega.BeNil())
		gomega.Expect(topic.Name).To(gomega.Equal(expectedName))
	})
	itShouldNotReturnError(err)
}

func itShouldSuccessfullyUpdateTopic(streamId uint32, topicId uint32, expectedName string, client iggycli.Client) {
	streamIdentifier, _ := iggcon.NewIdentifier(streamId)
	topicIdentifier, _ := iggcon.NewIdentifier(topicId)
	topic, err := client.GetTopic(streamIdentifier, topicIdentifier)

	ginkgo.It("should update topic with id "+string(rune(topicId)), func() {
		gomega.Expect(topic).NotTo(gomega.BeNil())
		gomega.Expect(topic.Id).To(gomega.Equal(topicId))
	})

	ginkgo.It("should update topic with name "+expectedName, func() {
		gomega.Expect(topic).NotTo(gomega.BeNil())
		gomega.Expect(topic.Name).To(gomega.Equal(expectedName))
	})
	itShouldNotReturnError(err)
}

func itShouldSuccessfullyDeleteTopic(streamId uint32, topicId uint32, client iggycli.Client) {
	streamIdentifier, _ := iggcon.NewIdentifier(streamId)
	topicIdentifier, _ := iggcon.NewIdentifier(topicId)
	topic, err := client.GetTopic(streamIdentifier, topicIdentifier)

	itShouldReturnSpecificError(err, ierror.ErrTopicIdNotFound)
	ginkgo.It("should not return topic", func() {
		gomega.Expect(topic).To(gomega.BeNil())
	})
}
