package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("UPDATE USER:", func() {
	When("User is logged in", func() {
		Context("tries to update user existing user", func() {
			client := createAuthorizedConnection()
			userId := successfullyCreateUser(createRandomString(16), client)
			defer deleteUserAfterTests(userId, client)
			request := iggcon.UpdateUserRequest{
				UserID:   iggcon.NewIdentifier(int(userId)),
				Username: createRandomString(16),
			}

			err := client.UpdateUser(request)

			itShouldNotReturnError(err)
			itShouldSuccessfullyUpdateUser(userId, request.Username, client)
		})
	})

	When("User is not logged in", func() {
		Context("and tries to update user", func() {
			client := createConnection()
			request := iggcon.UpdateUserRequest{
				UserID:   iggcon.NewIdentifier(int(createRandomUInt32())),
				Username: createRandomString(16),
			}

			err := client.UpdateUser(request)
			itShouldReturnUnauthenticatedError(err)
		})
	})
})
