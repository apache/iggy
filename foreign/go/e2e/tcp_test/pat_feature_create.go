package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("CREATE PAT:", func() {
	When("User is logged in", func() {
		Context("tries to create PAT with correct data", func() {
			client := createAuthorizedConnection()
			request := iggcon.CreateAccessTokenRequest{
				Name:   createRandomString(16),
				Expiry: 0,
			}

			response, err := client.CreateAccessToken(request)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateAccessToken(request.Name, client)
			itShouldBePossibleToLogInWithAccessToken(response.Token)
		})
	})

	When("User is not logged in", func() {
		Context("and tries to create PAT", func() {
			client := createConnection()
			request := iggcon.CreateAccessTokenRequest{
				Name:   createRandomString(16),
				Expiry: 0,
			}

			_, err := client.CreateAccessToken(request)
			itShouldReturnUnauthenticatedError(err)
		})
	})
})
