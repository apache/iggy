package tcp_test

import (
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("LOGOUT FEATURE:", func() {
	When("User is logged in", func() {
		Context("and tries to log out", func() {
			client := createAuthorizedConnection()
			err := client.LogOut()

			itShouldNotReturnError(err)
		})
	})

	When("User is not logged in", func() {
		Context("and tries to log out", func() {
			client := createConnection()
			err := client.LogOut()

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
