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

namespace Apache.Iggy.IggyClient;

/// <summary>
///     Defines methods for managing personal access tokens (PATs) in an Iggy client.
///     Personal access tokens provide an alternative to username/password authentication for API access.
/// </summary>
public interface IIggyPersonalAccessToken
{
    /// <summary>
    ///     Retrieves information about all personal access tokens for the current user.
    /// </summary>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>
    ///     A task that represents the asynchronous operation and returns a read-only collection of personal access token
    ///     information.
    /// </returns>
    Task<IReadOnlyList<PersonalAccessTokenResponse>> GetPersonalAccessTokensAsync(CancellationToken token = default);

    /// <summary>
    ///     Creates a new personal access token for the current user.
    /// </summary>
    /// <param name="name">The name to identify this token.</param>
    /// <param name="expiry">The expiration time in milliseconds from now (optional, null means no expiration).</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>
    ///     A task that represents the asynchronous operation and returns the created personal access token with its
    ///     secret value, or null if creation failed.
    /// </returns>
    Task<RawPersonalAccessToken?> CreatePersonalAccessTokenAsync(string name, ulong? expiry = null,
        CancellationToken token = default);

    /// <summary>
    ///     Deletes a personal access token by name.
    /// </summary>
    /// <param name="name">The name of the token to delete.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task DeletePersonalAccessTokenAsync(string name, CancellationToken token = default);

    /// <summary>
    ///     Authenticates with a personal access token.
    /// </summary>
    /// <param name="token">The personal access token secret value for authentication.</param>
    /// <param name="ct">The cancellation token to cancel the operation.</param>
    /// <returns>
    ///     A task that represents the asynchronous operation and returns authentication response with session
    ///     information, or null if login failed.
    /// </returns>
    Task<AuthResponse?> LoginWithPersonalAccessToken(string token, CancellationToken ct = default);
}
