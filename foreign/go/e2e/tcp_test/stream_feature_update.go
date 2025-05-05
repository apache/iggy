package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("UPDATE STREAM:", func() {
	prefix := "UpdateStream"
	When("User is logged in", func() {
		Context("and tries to update existing stream with a valid name", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			newName := createRandomString(128)

			err := client.UpdateStream(iggcon.UpdateStreamRequest{
				StreamId: iggcon.NewIdentifier(streamId),
				Name:     newName,
			})
			itShouldNotReturnError(err)
			itShouldSuccessfullyUpdateStream(streamId, newName, client)
		})

		Context("and tries to update stream with duplicate stream name", func() {
			client := createAuthorizedConnection()
			stream1Id, stream1Name := successfullyCreateStream(prefix, client)
			stream2Id, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(stream1Id, client)
			defer deleteStreamAfterTests(stream2Id, client)

			err := client.UpdateStream(iggcon.UpdateStreamRequest{
				StreamId: iggcon.NewIdentifier(stream2Id),
				Name:     stream1Name,
			})

			itShouldReturnSpecificError(err, "stream_name_already_exists")
		})

		Context("and tries to update non-existing stream", func() {
			client := createAuthorizedConnection()
			err := client.UpdateStream(iggcon.UpdateStreamRequest{
				StreamId: iggcon.NewIdentifier(int(createRandomUInt32())),
				Name:     createRandomString(128),
			})

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})

		Context("and tries to update existing stream with a name that's over 255 characters", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, createAuthorizedConnection())

			err := client.UpdateStream(iggcon.UpdateStreamRequest{
				StreamId: iggcon.NewIdentifier(streamId),
				Name:     createRandomString(256),
			})

			itShouldReturnSpecificError(err, "stream_name_too_long")
		})
	})

	When("User is not logged in", func() {
		Context("and tries to update stream", func() {
			client := createConnection()
			err := client.UpdateStream(iggcon.UpdateStreamRequest{
				StreamId: iggcon.NewIdentifier(int(createRandomUInt32())),
				Name:     createRandomString(128),
			})

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
