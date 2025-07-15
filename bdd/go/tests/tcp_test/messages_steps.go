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
	"bytes"
	"reflect"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/iggycli"
	"github.com/google/uuid"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

func createDefaultMessageHeaders() map[iggcon.HeaderKey]iggcon.HeaderValue {
	return map[iggcon.HeaderKey]iggcon.HeaderValue{
		{Value: createRandomString(4)}: {Kind: iggcon.String, Value: []byte(createRandomString(8))},
		{Value: createRandomString(8)}: {Kind: iggcon.Uint32, Value: []byte{0x01, 0x02, 0x03, 0x04}},
	}
}

func createDefaultMessages() []iggcon.IggyMessage {
	headers := createDefaultMessageHeaders()
	msg1, _ := iggcon.NewIggyMessage([]byte(createRandomString(256)), iggcon.WithID(uuid.New()), iggcon.WithUserHeaders(headers))
	msg2, _ := iggcon.NewIggyMessage([]byte(createRandomString(256)), iggcon.WithID(uuid.New()), iggcon.WithUserHeaders(headers))
	return []iggcon.IggyMessage{msg1, msg2}
}

func itShouldSuccessfullyPublishMessages(streamId uint32, topicId uint32, messages []iggcon.IggyMessage, client iggycli.Client) {
	streamIdentifier, _ := iggcon.NewIdentifier(streamId)
	topicIdentifier, _ := iggcon.NewIdentifier(topicId)
	result, err := client.PollMessages(
		streamIdentifier,
		topicIdentifier,
		iggcon.NewSingleConsumer(randomU32Identifier()),
		iggcon.FirstPollingStrategy(),
		uint32(len(messages)),
		true,
		nil)

	ginkgo.It("It should not be nil", func() {
		gomega.Expect(result).NotTo(gomega.BeNil())
	})

	ginkgo.It("It should contain 2 messages", func() {
		gomega.Expect(len(result.Messages)).To(gomega.Equal(len(messages)))
	})

	for _, expectedMsg := range messages {
		ginkgo.It("It should contain published messages", func() {
			found := compareMessage(result.Messages, expectedMsg)
			gomega.Expect(found).To(gomega.BeTrue(), "Message not found or does not match expected values")
		})
	}

	ginkgo.It("Should not return error", func() {
		gomega.Expect(err).To(gomega.BeNil())
	})
}

func compareMessage(resultMessages []iggcon.IggyMessage, expectedMessage iggcon.IggyMessage) bool {
	for _, msg := range resultMessages {
		if msg.Header.Id == expectedMessage.Header.Id && bytes.Equal(msg.Payload, expectedMessage.Payload) {
			if reflect.DeepEqual(msg.UserHeaders, expectedMessage.UserHeaders) {
				return true
			}
		}
	}
	return false
}
