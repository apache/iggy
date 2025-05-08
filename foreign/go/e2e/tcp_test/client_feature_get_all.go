package tcp_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("GET ALL CLIENT FEATURE:", func() {
	When("user is logged in", func() {
		Context("and tries to log with correct data", func() {
			client := createAuthorizedConnection()
			clients, err := client.GetClients()

			itShouldNotReturnError(err)
			It("should return stats", func() {
				Expect(clients).ToNot(BeNil())
			})

			It("should return at least one client", func() {
				Expect(len(clients)).ToNot(BeZero())
			})
		})
	})

	When("user is not logged in", func() {
		Context("and tries get all clients", func() {
			client := createConnection()
			clients, err := client.GetClients()

			itShouldReturnUnauthenticatedError(err)
			It("should not return clients", func() {
				Expect(clients).To(BeNil())
			})
		})
	})
})
