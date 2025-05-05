package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	ierror "github.com/iggy-rs/iggy-go-client/errors"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("DELETE TOPIC:", func() {
	prefix := "DeleteTopic"
	When("User is logged in", func() {
		Context("and tries to delete existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			err := client.DeleteTopic(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId))

			itShouldNotReturnError(err)
			itShouldSuccessfullyDeleteTopic(streamId, topicId, client)
		})

		Context("and tries to delete non-existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId := int(createRandomUInt32())

			err := client.DeleteTopic(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId))

			itShouldReturnSpecificIggyError(err, ierror.TopicIdNotFound)
		})

		Context("and tries to delete non-existing topic and stream", func() {
			client := createAuthorizedConnection()
			streamId := int(createRandomUInt32())
			topicId := int(createRandomUInt32())

			err := client.DeleteTopic(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId))

			itShouldReturnSpecificIggyError(err, ierror.StreamIdNotFound)
		})
	})

	When("User is not logged in", func() {
		Context("and tries to delete topic", func() {
			client := createConnection()
			err := client.DeleteTopic(iggcon.NewIdentifier(int(createRandomUInt32())), iggcon.NewIdentifier(int(createRandomUInt32())))

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
