package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	ierror "github.com/iggy-rs/iggy-go-client/errors"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("GET TOPIC BY ID:", func() {
	prefix := "GetTopicById"
	When("User is logged in", func() {
		Context("and tries to get existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, name := successfullyCreateTopic(streamId, client)
			topic, err := client.GetTopicById(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId))

			itShouldNotReturnError(err)
			itShouldReturnSpecificTopic(topicId, name, *topic)
		})

		Context("and tries to get topic from non-existing stream", func() {
			client := createAuthorizedConnection()
			streamId := int(createRandomUInt32())

			_, err := client.GetTopicById(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(int(createRandomUInt32())))

			itShouldReturnSpecificIggyError(err, ierror.TopicIdNotFound)
		})

		Context("and tries to get non-existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)

			_, err := client.GetTopicById(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(int(createRandomUInt32())))

			itShouldReturnSpecificIggyError(err, ierror.TopicIdNotFound)
		})
	})

	// ! TODO: review if needed to implement into sdk
	// When("User is not logged in", func() {
	// 	Context("and tries to get topic by id", func() {
	// 		client := createConnection()
	// 		_, err := client.GetTopicById(iggcon.NewIdentifier(int(createRandomUInt32())), iggcon.NewIdentifier(int(createRandomUInt32())))

	// 		itShouldReturnUnauthenticatedError(err)
	// 	})
	// })
})
