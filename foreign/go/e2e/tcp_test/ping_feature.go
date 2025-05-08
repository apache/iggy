package tcp_test

import (
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("PING FEATURE:", func() {
	When("User is logged in", func() {
		Context("and tries to ping server", func() {
			client := createAuthorizedConnection()
			err := client.Ping()

			itShouldNotReturnError(err)
		})
	})

	When("User is not logged in", func() {
		Context("and tries to ping server", func() {
			client := createConnection()
			err := client.Ping()

			itShouldNotReturnError(err)
		})
	})
})
