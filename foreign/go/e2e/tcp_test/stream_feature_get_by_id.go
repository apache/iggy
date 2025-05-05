package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("GET STREAM BY ID:", func() {
	prefix := "GetStreamById"
	When("User is logged in", func() {
		Context("and tries to get existing stream", func() {
			client := createAuthorizedConnection()
			streamId, name := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			stream, err := client.GetStreamById(iggcon.GetStreamRequest{StreamID: iggcon.NewIdentifier(streamId)})

			itShouldNotReturnError(err)
			itShouldReturnSpecificStream(streamId, name, *stream)
		})

		Context("and tries to get non-existing stream", func() {
			client := createAuthorizedConnection()
			streamId := int(createRandomUInt32())

			_, err := client.GetStreamById(iggcon.GetStreamRequest{StreamID: iggcon.NewIdentifier(streamId)})

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})
	})

	// ! TODO: review if needed to implement into sdk
	// When("User is not logged in", func() {
	// 	Context("and tries to get stream by id", func() {
	// 		client := createConnection()
	// 		_, err := client.GetStreamById(iggcon.GetStreamRequest{StreamID: iggcon.NewIdentifier(int(createRandomUInt32()))})

	// 		itShouldReturnUnauthenticatedError(err)
	// 	})
	// })
})
