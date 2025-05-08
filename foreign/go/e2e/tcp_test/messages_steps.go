package tcp_test

import (
	"bytes"
	"reflect"

	"github.com/iggy-rs/iggy-go-client"
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func createDefaultMessageHeaders() map[iggcon.HeaderKey]iggcon.HeaderValue {
	return map[iggcon.HeaderKey]iggcon.HeaderValue{
		{Value: createRandomString(4)}: {Kind: iggcon.String, Value: []byte(createRandomString(8))},
		{Value: createRandomString(8)}: {Kind: iggcon.Uint32, Value: []byte{0x01, 0x02, 0x03, 0x04}},
	}
}

func createDefaultMessages() []iggcon.Message {
	headers := createDefaultMessageHeaders()
	messages := []iggcon.Message{
		iggcon.NewMessage([]byte(createRandomString(256)), headers),
		iggcon.NewMessage([]byte(createRandomString(256)), headers),
	}

	return messages
}

func itShouldSuccessfullyPublishMessages(streamId int, topicId int, messages []iggcon.Message, client iggy.MessageStream) {
	result, err := client.PollMessages(iggcon.FetchMessagesRequest{
		StreamId: iggcon.NewIdentifier(streamId),
		TopicId:  iggcon.NewIdentifier(topicId),
		Consumer: iggcon.Consumer{
			Kind: iggcon.ConsumerSingle,
			Id:   iggcon.NewIdentifier(int(createRandomUInt32())),
		},
		PollingStrategy: iggcon.FirstPollingStrategy(),
		Count:           len(messages),
		AutoCommit:      true,
	})

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

func compareMessage(resultMessages []iggcon.MessageResponse, expectedMessage iggcon.Message) bool {
	for _, msg := range resultMessages {
		if msg.Id == expectedMessage.Id && bytes.Equal(msg.Payload, expectedMessage.Payload) {
			if reflect.DeepEqual(msg.Headers, expectedMessage.Headers) {
				return true
			}
		}
	}
	return false
}
