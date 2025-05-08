package tcp_test

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("CHANGE PASSWORD:", func() {
	When("User is logged in", func() {
		Context("tries to change password of existing user", func() {
			client := createAuthorizedConnection()
			createRequest := iggcon.CreateUserRequest{
				Username: createRandomStringWithPrefix("ch_p_", 16),
				Password: "oldPassword",
				Status:   iggcon.Active,
				Permissions: &iggcon.Permissions{
					Global: iggcon.GlobalPermissions{
						ManageServers: true,
						ReadServers:   true,
						ManageUsers:   true,
						ReadUsers:     true,
						ManageStreams: true,
						ReadStreams:   true,
						ManageTopics:  true,
						ReadTopics:    true,
						PollMessages:  true,
						SendMessages:  true,
					},
				},
			}

			err := client.CreateUser(createRequest)
			defer deleteUserAfterTests(createRequest.Username, client)
			request := iggcon.ChangePasswordRequest{
				UserID:          iggcon.NewIdentifier(createRequest.Username),
				CurrentPassword: createRequest.Password,
				NewPassword:     "newPassword",
			}

			err = client.ChangePassword(request)

			itShouldNotReturnError(err)
			//itShouldBePossibleToLogInWithCredentials(createRequest.Username, request.NewPassword)
		})
	})

	When("User is not logged in", func() {
		Context("and tries to change password", func() {
			client := createConnection()
			request := iggcon.UpdateUserPermissionsRequest{
				UserID: iggcon.NewIdentifier(int(createRandomUInt32())),
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
			itShouldReturnUnauthenticatedError(err)
		})
	})
})
