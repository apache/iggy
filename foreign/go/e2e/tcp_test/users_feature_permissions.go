package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("UPDATE USER PERMISSIONS:", func() {
	When("User is logged in", func() {
		Context("tries to update permissions of existing user", func() {
			client := createAuthorizedConnection()
			userId := successfullyCreateUser(createRandomString(16), client)
			defer deleteUserAfterTests(userId, client)
			request := iggcon.UpdateUserPermissionsRequest{
				UserID: iggcon.NewIdentifier(int(userId)),
				Permissions: &iggcon.Permissions{
					Global: iggcon.GlobalPermissions{
						ManageServers: false,
						ReadServers:   false,
						ManageUsers:   false,
						ReadUsers:     false,
						ManageStreams: false,
						ReadStreams:   false,
						ManageTopics:  false,
						ReadTopics:    false,
						PollMessages:  false,
						SendMessages:  false,
					},
				},
			}

			err := client.UpdateUserPermissions(request)

			itShouldNotReturnError(err)
			itShouldSuccessfullyUpdateUserPermissions(userId, client)
		})
	})

	When("User is not logged in", func() {
		Context("and tries to change user permissions", func() {
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
