package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("JOIN CONSUMER GROUP:", func() {
	prefix := "JoinConsumerGroup"
	When("User is logged in", func() {
		Context("and tries to join existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, _ := successfullyCreateConsumer(streamId, topicId, client)
			err := client.JoinConsumerGroup(iggcon.JoinConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				ConsumerGroupId: iggcon.NewIdentifier(groupId),
			})

			itShouldNotReturnError(err)
			itShouldSuccessfullyJoinConsumer(streamId, topicId, groupId, client)
		})

		Context("and tries to join non-existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId := int(createRandomUInt32())
			err := client.JoinConsumerGroup(iggcon.JoinConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				ConsumerGroupId: iggcon.NewIdentifier(groupId),
			})

			itShouldReturnSpecificError(err, "consumer_group_not_found")
		})

		Context("and tries to join consumer non-existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId := int(createRandomUInt32())

			err := client.JoinConsumerGroup(iggcon.JoinConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				ConsumerGroupId: iggcon.NewIdentifier(int(createRandomUInt32())),
			})

			itShouldReturnSpecificError(err, "topic_id_not_found")
		})

		Context("and tries to join consumer for non-existing topic and stream", func() {
			client := createAuthorizedConnection()
			streamId := int(createRandomUInt32())
			topicId := int(createRandomUInt32())

			err := client.JoinConsumerGroup(iggcon.JoinConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				ConsumerGroupId: iggcon.NewIdentifier(int(createRandomUInt32())),
			})

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to join to the consumer group", func() {
			client := createConnection()
			err := client.JoinConsumerGroup(iggcon.JoinConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(int(createRandomUInt32())),
				TopicId:         iggcon.NewIdentifier(int(createRandomUInt32())),
				ConsumerGroupId: iggcon.NewIdentifier(int(createRandomUInt32())),
			})

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
