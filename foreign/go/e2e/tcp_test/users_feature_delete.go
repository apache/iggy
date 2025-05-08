package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("DELETE USER:", func() {
	When("User is logged in", func() {
		Context("tries to delete user with correct data", func() {
			client := createAuthorizedConnection()
			userId := successfullyCreateUser(createRandomString(16), client)

			err := client.DeleteUser(iggcon.NewIdentifier(int(userId)))

			itShouldNotReturnError(err)
			itShouldSuccessfullyDeleteUser(int(userId), client)
		})
	})

	When("User is not logged in", func() {
		Context("and tries to delete user", func() {
			client := createConnection()
			err := client.DeleteUser(iggcon.NewIdentifier(int(createRandomUInt32())))
			itShouldReturnUnauthenticatedError(err)
		})
	})
})
