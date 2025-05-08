package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("DELETE PARTITION:", func() {
	prefix := "DeletePartition"
	When("User is logged in", func() {
		Context("and tries to delete partitions for existing stream", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)

			request := iggcon.DeletePartitionRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(topicId),
				PartitionsCount: 1,
			}
			err := client.DeletePartition(request)

			itShouldNotReturnError(err)
			itShouldHaveExpectedNumberOfPartitions(streamId, topicId, 2-request.PartitionsCount, client)
		})

		Context("and tries to delete partitions for a non existing stream", func() {
			client := createAuthorizedConnection()
			request := iggcon.DeletePartitionRequest{
				StreamId:        iggcon.NewIdentifier(int(createRandomUInt32())),
				TopicId:         iggcon.NewIdentifier(int(createRandomUInt32())),
				PartitionsCount: 10,
			}
			err := client.DeletePartition(request)

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})

		Context("and tries to delete partitions for a non existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			request := iggcon.DeletePartitionRequest{
				StreamId:        iggcon.NewIdentifier(streamId),
				TopicId:         iggcon.NewIdentifier(int(createRandomUInt32())),
				PartitionsCount: 10,
			}
			err := client.DeletePartition(request)

			itShouldReturnSpecificError(err, "topic_id_not_found")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to delete partitions", func() {
			client := createConnection()
			request := iggcon.DeletePartitionRequest{
				StreamId:        iggcon.NewIdentifier(int(createRandomUInt32())),
				TopicId:         iggcon.NewIdentifier(int(createRandomUInt32())),
				PartitionsCount: 10,
			}
			err := client.DeletePartition(request)

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
