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

namespace Apache.Iggy.Configuration;

/// <summary>
///     Represents the settings required for automatic login functionality in the Iggy client.
/// </summary>
public class AutoLoginSettings
{
    /// <summary>
    ///     Enable automatic login on connection establishment.
    /// </summary>
    public bool Enabled { get; init; }

    /// <summary>
    ///     Specifies the username for auto-login configuration.
    /// </summary>
    public string Username { get; init; } = string.Empty;

    /// <summary>
    ///     Specifies the password for auto-login authentication
    /// </summary>
    public string Password { get; init; } = string.Empty;

    /// <summary>
    ///     Personal access token to sign in with instead of a username and password. Takes precedence over them
    ///     when set.
    /// </summary>
    public string PersonalAccessToken { get; init; } = string.Empty;

    /// <summary>
    ///     Auto login with a username and password.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when the username is empty.</exception>
    public static AutoLoginSettings For(string username, string password)
    {
        ArgumentException.ThrowIfNullOrEmpty(username);

        return new AutoLoginSettings
        {
            Enabled = true,
            Username = username,
            Password = password
        };
    }

    /// <summary>
    ///     Auto login with a personal access token.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when the token is empty.</exception>
    public static AutoLoginSettings ForPersonalAccessToken(string token)
    {
        ArgumentException.ThrowIfNullOrEmpty(token);

        return new AutoLoginSettings
        {
            Enabled = true,
            PersonalAccessToken = token
        };
    }
}
