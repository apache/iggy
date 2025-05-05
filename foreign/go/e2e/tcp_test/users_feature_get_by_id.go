package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("GET USER:", func() {
	When("User is logged in", func() {
		Context("tries to get existing user", func() {
			client := createAuthorizedConnection()
			name := createRandomString(16)
			userId := successfullyCreateUser(name, client)
			defer deleteUserAfterTests(int(userId), client)

			user, err := client.GetUser(iggcon.NewIdentifier(int(userId)))

			itShouldNotReturnError(err)
			itShouldReturnSpecificUser(name, *user)
		})
	})

	// ! TODO: review if needed to implement into sdk
	// When("User is not logged in", func() {
	//		Context("and tries to get user", func() {
	//			client := createConnection()
	//			_, err := client.GetUser(iggcon.NewIdentifier(int(createRandomUInt32())))
	//			itShouldReturnUnauthenticatedError(err)
	//		})
	//	})
})
