package tcp_test

import (
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("GET USER:", func() {
	When("User is logged in", func() {
		Context("tries to get all users", func() {
			client := createAuthorizedConnection()
			name := createRandomString(16)
			userId := successfullyCreateUser(name, client)
			defer deleteUserAfterTests(int(userId), client)

			users, err := client.GetUsers()

			itShouldNotReturnError(err)
			itShouldContainSpecificUser(name, users)
		})
	})

	When("User is not logged in", func() {
		Context("and tries to all get users", func() {
			client := createConnection()
			_, err := client.GetUsers()
			itShouldReturnUnauthenticatedError(err)
		})
	})
})
