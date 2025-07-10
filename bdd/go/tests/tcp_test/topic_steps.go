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
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
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
	ginkgo.It("should fetch topic with id "+string(rune(id)), func() {
		gomega.Expect(topic.Id).To(gomega.Equal(id))
	})

	ginkgo.It("should fetch topic with name "+name, func() {
		gomega.Expect(topic.Name).To(gomega.Equal(name))
	})
}

func itShouldContainSpecificTopic(id int, name string, topics []iggcon.Topic) {
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

	ginkgo.It("should fetch topic with id "+strconv.Itoa(id), func() {
		gomega.Expect(found).To(gomega.BeTrue(), "Topic with id %d and name %s not found", id, name)
		gomega.Expect(topic.Id).To(gomega.Equal(id))
	})

	ginkgo.It("should fetch topic with name "+name, func() {
		gomega.Expect(found).To(gomega.BeTrue(), "Topic with id %d and name %s not found", id, name)
		gomega.Expect(topic.Name).To(gomega.Equal(name))
	})
}

func itShouldSuccessfullyCreateTopic(streamId int, topicId int, expectedName string, client iggycli.Client) {
	topic, err := client.GetTopic(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId))
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

func itShouldSuccessfullyUpdateTopic(streamId int, topicId int, expectedName string, client iggycli.Client) {
	topic, err := client.GetTopic(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId))

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

func itShouldSuccessfullyDeleteTopic(streamId int, topicId int, client iggycli.Client) {
	topic, err := client.GetTopic(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId))

	itShouldReturnSpecificIggyError(err, ierror.TopicIdNotFound)
	ginkgo.It("should not return topic", func() {
		gomega.Expect(topic).To(gomega.BeNil())
	})
}
