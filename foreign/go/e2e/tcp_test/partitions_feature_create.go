package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("CREATE PARTITION:", func() {
	prefix := "CreatePartition"
	When("User is logged in", func() {
		Context("and tries to create partitions for existing stream", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)

			request := iggcon.CreatePartitionsRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				PartitionsCount: 10,
			}
			err := client.CreatePartition(request)

			itShouldNotReturnError(err)
			itShouldHaveExpectedNumberOfPartitions(streamId, topicId, request.PartitionsCount+2, client)
		})

		Context("and tries to create partitions for a non existing stream", func() {
			client := createAuthorizedConnection()
			request := iggcon.CreatePartitionsRequest{
				StreamId:        iggcon.NewIdentifier(int(createRandomUInt32())),
				TopicId:         iggcon.NewIdentifier(int(createRandomUInt32())),
				PartitionsCount: 10,
			}
			err := client.CreatePartition(request)

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})

		Context("and tries to create partitions for a non existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			request := iggcon.CreatePartitionsRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(int(createRandomUInt32())),
				PartitionsCount: 10,
			}
			err := client.CreatePartition(request)

			itShouldReturnSpecificError(err, "topic_id_not_found")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to create partitions", func() {
			client := createConnection()
			request := iggcon.CreatePartitionsRequest{
				StreamId:        iggcon.NewIdentifier(int(createRandomUInt32())),
				TopicId:         iggcon.NewIdentifier(int(createRandomUInt32())),
				PartitionsCount: 10,
			}
			err := client.CreatePartition(request)

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
