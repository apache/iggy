package tcp_test

import (
	"math"

	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("CREATE TOPIC:", func() {
	prefix := "CreateTopic"
	When("User is logged in", func() {
		Context("and tries to create topic unique name and id", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)

			request := iggcon.CreateTopicRequest{
				TopicId:              1,
				StreamId:             iggcon.NewIdentifier(streamId),
				Name:                 createRandomString(32),
				MessageExpiry:        1000,
				PartitionsCount:      2,
				CompressionAlgorithm: 1,
				MaxTopicSize:         math.MaxUint64,
				ReplicationFactor:    1,
			}
			err := client.CreateTopic(request)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateTopic(streamId, request.TopicId, request.Name, client)
		})

		Context("and tries to create topic for a non existing stream", func() {
			client := createAuthorizedConnection()
			streamId := int(createRandomUInt32())

			request := iggcon.CreateTopicRequest{
				TopicId:              1,
				StreamId:             iggcon.NewIdentifier(streamId),
				Name:                 createRandomString(32),
				MessageExpiry:        1000,
				PartitionsCount:      2,
				CompressionAlgorithm: 1,
				MaxTopicSize:         math.MaxUint64,
				ReplicationFactor:    1,
			}
			err := client.CreateTopic(request)

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})

		Context("and tries to create topic with duplicate topic name", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			_, name := successfullyCreateTopic(streamId, client)

			request := iggcon.CreateTopicRequest{
				TopicId:              int(createRandomUInt32()),
				StreamId:             iggcon.NewIdentifier(streamId),
				Name:                 name,
				MessageExpiry:        0,
				PartitionsCount:      2,
				CompressionAlgorithm: 1,
				MaxTopicSize:         math.MaxUint64,
				ReplicationFactor:    1,
			}
			err := client.CreateTopic(request)
			itShouldReturnSpecificError(err, "topic_name_already_exists")
		})

		Context("and tries to create topic with duplicate topic id", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)

			request := iggcon.CreateTopicRequest{
				TopicId:              topicId,
				StreamId:             iggcon.NewIdentifier(streamId),
				Name:                 createRandomString(32),
				MessageExpiry:        0,
				PartitionsCount:      2,
				CompressionAlgorithm: 1,
				MaxTopicSize:         math.MaxUint64,
				ReplicationFactor:    1,
			}
			err := client.CreateTopic(request)
			itShouldReturnSpecificError(err, "topic_id_already_exists")
		})

		Context("and tries to create topic with name that's over 255 characters", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, createAuthorizedConnection())

			request := iggcon.CreateTopicRequest{
				TopicId:              int(createRandomUInt32()),
				StreamId:             iggcon.NewIdentifier(streamId),
				Name:                 createRandomString(256),
				MessageExpiry:        0,
				PartitionsCount:      2,
				CompressionAlgorithm: 1,
				MaxTopicSize:         math.MaxUint64,
				ReplicationFactor:    1,
			}
			err := client.CreateTopic(request)

			itShouldReturnSpecificError(err, "topic_name_too_long")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to create topic", func() {
			client := createConnection()
			err := client.CreateTopic(iggcon.CreateTopicRequest{
				TopicId:              1,
				StreamId:             iggcon.NewIdentifier(10),
				Name:                 "name",
				MessageExpiry:        0,
				PartitionsCount:      2,
				CompressionAlgorithm: 1,
				MaxTopicSize:         math.MaxUint64,
				ReplicationFactor:    1,
			})

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
