package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("LEAVE CONSUMER GROUP:", func() {
	prefix := "LeaveConsumerGroup"
	When("User is logged in", func() {
		Context("and tries to leave consumer group, that he is a part of", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, _ := successfullyCreateConsumer(streamId, topicId, client)
			successfullyJoinConsumer(streamId, topicId, groupId, client)

			err := client.LeaveConsumerGroup(iggcon.LeaveConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				ConsumerGroupId: iggcon.NewIdentifier(groupId),
			})

			itShouldNotReturnError(err)
			itShouldSuccessfullyLeaveConsumer(streamId, topicId, groupId, client)
		})

		Context("and tries to leave non-existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId := int(createRandomUInt32())
			err := client.LeaveConsumerGroup(iggcon.LeaveConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				ConsumerGroupId: iggcon.NewIdentifier(groupId),
			})

			itShouldReturnSpecificError(err, "consumer_group_not_found")
		})

		Context("and tries to leave consumer non-existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId := int(createRandomUInt32())

			err := client.LeaveConsumerGroup(iggcon.LeaveConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				ConsumerGroupId: iggcon.NewIdentifier(int(createRandomUInt32())),
			})

			itShouldReturnSpecificError(err, "topic_id_not_found")
		})

		Context("and tries to leave consumer for non-existing topic and stream", func() {
			client := createAuthorizedConnection()
			streamId := int(createRandomUInt32())
			topicId := int(createRandomUInt32())

			err := client.LeaveConsumerGroup(iggcon.LeaveConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				ConsumerGroupId: iggcon.NewIdentifier(int(createRandomUInt32())),
			})

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to leave to the consumer group", func() {
			client := createConnection()
			err := client.LeaveConsumerGroup(iggcon.LeaveConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(int(createRandomUInt32())),
				TopicId:         iggcon.NewIdentifier(int(createRandomUInt32())),
				ConsumerGroupId: iggcon.NewIdentifier(int(createRandomUInt32())),
			})

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
