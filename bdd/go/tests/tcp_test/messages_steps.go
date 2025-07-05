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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

func itShouldSuccessfullyPublishMessages(streamId int, topicId int, messages []iggcon.IggyMessage, client iggycli.Client) {
	result, err := client.PollMessages(
		iggcon.NewIdentifier(streamId),
		iggcon.NewIdentifier(topicId),
		iggcon.Consumer{
			Kind: iggcon.ConsumerKindSingle,
			Id:   iggcon.NewIdentifier(int(createRandomUInt32())),
		},
		iggcon.FirstPollingStrategy(),
		uint32(len(messages)),
		true,
		nil)

	It("It should not be nil", func() {
		Expect(result).NotTo(BeNil())
	})

	It("It should contain 2 messages", func() {
		Expect(len(result.Messages)).To(Equal(len(messages)))
	})

	for _, expectedMsg := range messages {
		It("It should contain published messages", func() {
			found := compareMessage(result.Messages, expectedMsg)
			Expect(found).To(BeTrue(), "Message not found or does not match expected values")
		})
	}

	It("Should not return error", func() {
		Expect(err).To(BeNil())
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
