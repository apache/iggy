package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("CREATE STREAM:", func() {
	When("User is logged in", func() {
		Context("and tries to create stream with unique name and id", func() {
			client := createAuthorizedConnection()
			streamId := int(createRandomUInt32())
			name := createRandomString(32)

			err := client.CreateStream(iggcon.CreateStreamRequest{
				StreamId: streamId,
				Name:     name,
			})
			defer deleteStreamAfterTests(streamId, client)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateStream(streamId, name, client)
		})

		Context("and tries to create stream with duplicate stream name", func() {
			client := createAuthorizedConnection()
			streamId := int(createRandomUInt32())
			name := createRandomString(32)

			err := client.CreateStream(iggcon.CreateStreamRequest{
				StreamId: streamId,
				Name:     name,
			})
			defer deleteStreamAfterTests(streamId, client)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateStream(streamId, name, client)

			err = client.CreateStream(iggcon.CreateStreamRequest{
				StreamId: int(createRandomUInt32()),
				Name:     name,
			})

			itShouldReturnSpecificError(err, "stream_name_already_exists")
		})

		Context("and tries to create stream with duplicate stream id", func() {
			client := createAuthorizedConnection()
			streamId := int(createRandomUInt32())
			name := createRandomString(32)

			err := client.CreateStream(iggcon.CreateStreamRequest{
				StreamId: streamId,
				Name:     name,
			})
			defer deleteStreamAfterTests(streamId, client)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateStream(streamId, name, client)

			err = client.CreateStream(iggcon.CreateStreamRequest{
				StreamId: streamId,
				Name:     createRandomString(32),
			})

			itShouldReturnSpecificError(err, "stream_id_already_exists")
		})

		Context("and tries to create stream name that's over 255 characters", func() {
			client := createAuthorizedConnection()
			streamId := int(createRandomUInt32())
			name := createRandomString(256)

			err := client.CreateStream(iggcon.CreateStreamRequest{
				StreamId: streamId,
				Name:     name,
			})

			itShouldReturnSpecificError(err, "stream_name_too_long")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to create stream", func() {
			client := createConnection()
			err := client.CreateStream(iggcon.CreateStreamRequest{
				StreamId: int(createRandomUInt32()),
				Name:     createRandomString(32),
			})

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
