package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("GET ALL TOPICS:", func() {
	prefix := "GetAllTopics"
	When("User is logged in", func() {
		Context("and tries to get all topics", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, name := successfullyCreateTopic(streamId, client)
			topics, err := client.GetTopics(iggcon.NewIdentifier(streamId))

			itShouldNotReturnError(err)
			itShouldContainSpecificTopic(topicId, name, topics)
		})
	})

	When("User is not logged in", func() {
		Context("and tries to get all topics", func() {
			client := createConnection()
			_, err := client.GetTopics(iggcon.NewIdentifier(int(createRandomUInt32())))

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
