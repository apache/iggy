package tcp_test

import (
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("GET PAT:", func() {
	When("User is logged in", func() {
		Context("tries to get all PATs", func() {
			client := createAuthorizedConnection()
			name := createRandomString(16)
			successfullyCreateAccessToken(name, client)

			tokens, err := client.GetAccessTokens()

			itShouldNotReturnError(err)
			itShouldContainSpecificAccessToken(name, tokens)
		})
	})

	When("User is not logged in", func() {
		Context("and tries to all get PAT's", func() {
			client := createConnection()
			_, err := client.GetAccessTokens()
			itShouldReturnUnauthenticatedError(err)
		})
	})
})
