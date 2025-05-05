package tcp_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("STAT FEATURE:", func() {
	When("user is logged in", func() {
		Context("and tries to log with correct data", func() {
			client := createAuthorizedConnection()
			stats, err := client.GetStats()

			itShouldNotReturnError(err)
			It("should return stats", func() {
				Expect(stats).ToNot(BeNil())
			})
		})
	})

	// When("user is not logged in", func() {
	// 	Context("and tries get iggy statistics", func() {
	// 		client := createConnection()
	// 		stats, err := client.GetStats()

	// 		itShouldReturnUnauthenticatedError(err)
	// 		It("should not return stats", func() {
	// 			Expect(stats).To(BeNil())
	// 		})
	// 	})
	// })
})
