package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("GET ALL CONSUMER GROUPS:", func() {
	prefix := "GetAllConsumerGroups"
	When("User is logged in", func() {
		Context("and tries to get all consumer groups", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, name := successfullyCreateConsumer(streamId, topicId, client)
			groups, err := client.GetConsumerGroups(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId))

			itShouldNotReturnError(err)
			itShouldContainSpecificConsumer(groupId, name, groups)
		})
	})

	When("User is not logged in", func() {
		Context("and tries to get all consumer groups", func() {
			client := createConnection()
			_, err := client.GetConsumerGroups(iggcon.NewIdentifier(int(createRandomUInt32())), iggcon.NewIdentifier(int(createRandomUInt32())))

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
