// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Enums;

namespace Apache.Iggy.IggyClient;

/// <summary>
///     Defines methods for managing users and authentication in an Iggy client.
///     Users can have different permissions and status levels within the system.
/// </summary>
public interface IIggyUsers
{
    /// <summary>
    ///     Retrieves detailed information about a specific user by their identifier.
    /// </summary>
    /// <param name="userId">The identifier of the user to retrieve (numeric ID or username).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation and returns the user information, or null if not found.</returns>
    Task<UserResponse?> GetUser(Identifier userId, CancellationToken token = default);

    /// <summary>
    ///     Retrieves information about all users in the system.
    /// </summary>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation and returns a read-only collection of user information.</returns>
    Task<IReadOnlyList<UserResponse>> GetUsers(CancellationToken token = default);

    /// <summary>
    ///     Creates a new user with the specified credentials and permissions.
    /// </summary>
    /// <param name="userName">The unique username for the new user.</param>
    /// <param name="password">The password for the user.</param>
    /// <param name="status">The initial status of the user (active or inactive).</param>
    /// <param name="permissions">The permissions granted to the user (optional).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>
    ///     A task that represents the asynchronous operation and returns the created user information, or null if
    ///     creation failed.
    /// </returns>
    Task<UserResponse?> CreateUser(string userName, string password, UserStatus status, Permissions? permissions = null,
        CancellationToken token = default);

    /// <summary>
    ///     Deletes an existing user from the system.
    /// </summary>
    /// <param name="userId">The identifier of the user to delete (numeric ID or username).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task DeleteUser(Identifier userId, CancellationToken token = default);

    /// <summary>
    ///     Updates user properties such as username and status.
    /// </summary>
    /// <param name="userId">The identifier of the user to update (numeric ID or username).</param>
    /// <param name="userName">The new username (optional).</param>
    /// <param name="status">The new status (optional).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task UpdateUser(Identifier userId, string? userName = null, UserStatus? status = null,
        CancellationToken token = default);

    /// <summary>
    ///     Updates the permissions for a specific user.
    /// </summary>
    /// <param name="userId">The identifier of the user whose permissions will be updated (numeric ID or username).</param>
    /// <param name="permissions">The new permissions to assign (null will remove all permissions).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task UpdatePermissions(Identifier userId, Permissions? permissions = null, CancellationToken token = default);

    /// <summary>
    ///     Changes the password for a user.
    /// </summary>
    /// <param name="userId">The identifier of the user whose password will be changed (numeric ID or username).</param>
    /// <param name="currentPassword">The current password of the user.</param>
    /// <param name="newPassword">The new password for the user.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ChangePassword(Identifier userId, string currentPassword, string newPassword,
        CancellationToken token = default);

    /// <summary>
    ///     Authenticates a user with username and password, establishing a session.
    /// </summary>
    /// <remarks>
    ///     Upon successful authentication, the client receives a session token that can be used for subsequent requests.
    /// </remarks>
    /// <param name="userName">The username of the user logging in.</param>
    /// <param name="password">The password of the user.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>
    ///     A task that represents the asynchronous operation and returns authentication response with session
    ///     information, or null if login failed.
    /// </returns>
    Task<AuthResponse?> LoginUser(string userName, string password, CancellationToken token = default);

    /// <summary>
    ///     Logs out the current user, invalidating the current session.
    /// </summary>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task LogoutUser(CancellationToken token = default);
}
