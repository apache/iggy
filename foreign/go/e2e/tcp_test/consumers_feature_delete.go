package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	ierror "github.com/iggy-rs/iggy-go-client/errors"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("DELETE CONSUMER GROUP:", func() {
	prefix := "DeleteConsumerGroup"
	When("User is logged in", func() {
		Context("and tries to delete existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, _ := successfullyCreateConsumer(streamId, topicId, client)
			err := client.DeleteConsumerGroup(iggcon.DeleteConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				ConsumerGroupId: iggcon.NewIdentifier(groupId),
			})

			itShouldNotReturnError(err)
			itShouldSuccessfullyDeletedConsumer(streamId, topicId, groupId, client)
		})

		Context("and tries to delete non-existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId := int(createRandomUInt32())
			err := client.DeleteConsumerGroup(iggcon.DeleteConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				ConsumerGroupId: iggcon.NewIdentifier(groupId),
			})

			itShouldReturnSpecificIggyError(err, ierror.ConsumerGroupIdNotFound)
		})

		Context("and tries to delete consumer non-existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId := int(createRandomUInt32())

			err := client.DeleteConsumerGroup(iggcon.DeleteConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				ConsumerGroupId: iggcon.NewIdentifier(int(createRandomUInt32())),
			})

			itShouldReturnSpecificError(err, "topic_id_not_found")
		})

		Context("and tries to delete consumer for non-existing topic and stream", func() {
			client := createAuthorizedConnection()
			streamId := int(createRandomUInt32())
			topicId := int(createRandomUInt32())

			err := client.DeleteConsumerGroup(iggcon.DeleteConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				ConsumerGroupId: iggcon.NewIdentifier(int(createRandomUInt32())),
			})

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to delete consumer group", func() {
			client := createConnection()
			err := client.DeleteConsumerGroup(iggcon.DeleteConsumerGroupRequest{
				StreamId:        iggcon.NewIdentifier(int(createRandomUInt32())),
				TopicId:         iggcon.NewIdentifier(int(createRandomUInt32())),
				ConsumerGroupId: iggcon.NewIdentifier(int(createRandomUInt32())),
			})

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
