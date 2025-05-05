package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("DELETE PAT:", func() {
	When("User is logged in", func() {
		Context("tries to delete PAT with correct data", func() {
			client := createAuthorizedConnection()
			name := createRandomString(16)
			token := successfullyCreateAccessToken(name, client)

			err := client.DeleteAccessToken(iggcon.DeleteAccessTokenRequest{
				Name: name,
			})

			itShouldNotReturnError(err)
			itShouldSuccessfullyDeleteAccessToken(token, client)
		})
	})

	When("User is not logged in", func() {
		Context("and tries to delete PAT", func() {
			client := createConnection()
			err := client.DeleteAccessToken(iggcon.DeleteAccessTokenRequest{
				Name: createRandomString(16),
			})
			itShouldReturnUnauthenticatedError(err)
		})
	})
})
