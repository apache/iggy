package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("SEND MESSAGES:", func() {
	prefix := "SendMessages"
	When("User is logged in", func() {
		Context("and tries to send messages to the topic with balanced partitioning", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream("1"+prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			messages := createDefaultMessages()
			request := iggcon.SendMessagesRequest{
				StreamId:     iggcon.NewIdentifier(streamId),
				TopicId:      iggcon.NewIdentifier(topicId),
				Partitioning: iggcon.None(),
				Messages:     messages,
			}
			err := client.SendMessages(request)
			itShouldNotReturnError(err)
			itShouldSuccessfullyPublishMessages(streamId, topicId, messages, client)
		})

		Context("and tries to send messages to the non existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream("2"+prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			messages := createDefaultMessages()
			request := iggcon.SendMessagesRequest{
				StreamId:     iggcon.NewIdentifier(streamId),
				TopicId:      iggcon.NewIdentifier(int(createRandomUInt32())),
				Partitioning: iggcon.None(),
				Messages:     messages,
			}
			err := client.SendMessages(request)
			itShouldReturnSpecificError(err, "topic_id_not_found")
		})

		Context("and tries to send messages to the non existing stream", func() {
			client := createAuthorizedConnection()
			messages := createDefaultMessages()
			request := iggcon.SendMessagesRequest{
				StreamId:     iggcon.NewIdentifier(int(createRandomUInt32())),
				TopicId:      iggcon.NewIdentifier(int(createRandomUInt32())),
				Partitioning: iggcon.None(),
				Messages:     messages,
			}
			err := client.SendMessages(request)
			itShouldReturnSpecificError(err, "stream_id_not_found")
		})

		Context("and tries to send messages to non existing partition", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream("3"+prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			messages := createDefaultMessages()
			request := iggcon.SendMessagesRequest{
				StreamId:     iggcon.NewIdentifier(streamId),
				TopicId:      iggcon.NewIdentifier(topicId),
				Partitioning: iggcon.PartitionId(int(createRandomUInt32())),
				Messages:     messages,
			}
			err := client.SendMessages(request)
			itShouldReturnSpecificError(err, "partition_not_found")
		})

		Context("and tries to send messages to valid topic but with 0 messages in payload", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, createAuthorizedConnection())
			topicId, _ := successfullyCreateTopic(streamId, client)
			request := iggcon.SendMessagesRequest{
				StreamId:     iggcon.NewIdentifier(streamId),
				TopicId:      iggcon.NewIdentifier(topicId),
				Partitioning: iggcon.PartitionId(int(createRandomUInt32())),
				Messages:     []iggcon.Message{},
			}
			err := client.SendMessages(request)
			itShouldReturnSpecificError(err, "messages_count_should_be_greater_than_zero")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to update stream", func() {
			client := createConnection()
			messages := createDefaultMessages()
			request := iggcon.SendMessagesRequest{
				StreamId:     iggcon.NewIdentifier(int(createRandomUInt32())),
				TopicId:      iggcon.NewIdentifier(int(createRandomUInt32())),
				Partitioning: iggcon.None(),
				Messages:     messages,
			}
			err := client.SendMessages(request)

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
