package tcp_test

import (
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("GET ALL STREAMS:", func() {
	prefix := "GetAllStreams"
	When("User is logged in", func() {
		Context("and tries to get all streams", func() {
			client := createAuthorizedConnection()
			streamId, name := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			streams, err := client.GetStreams()

			itShouldNotReturnError(err)
			itShouldContainSpecificStream(streamId, name, streams)
		})
	})

	When("User is not logged in", func() {
		Context("and tries to get all streams", func() {
			client := createConnection()
			_, err := client.GetStreams()

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
