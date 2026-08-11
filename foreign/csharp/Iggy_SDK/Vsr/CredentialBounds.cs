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

using System.Text;

namespace Apache.Iggy.Vsr;

/// <summary>
///     The credential bounds every server enforces, checked before encoding so an oversized credential is
///     reported as the typed status code instead of desyncing the u8 length prefix on the wire. Mirrors
///     <c>core/common/src/http/users/defaults.rs</c>.
/// </summary>
internal static class CredentialBounds
{
    internal const int MIN_USERNAME_LENGTH = 3;
    internal const int MAX_USERNAME_LENGTH = 50;
    internal const int MIN_PASSWORD_LENGTH = 3;
    internal const int MAX_PASSWORD_LENGTH = 100;

    internal const int MIN_TOKEN_LENGTH = 1;
    internal const int MAX_TOKEN_LENGTH = 255;

    internal static void ValidateUsername(string username)
    {
        var length = Encoding.UTF8.GetByteCount(username);
        if (length is < MIN_USERNAME_LENGTH or > MAX_USERNAME_LENGTH)
        {
            throw VsrError.Exception(VsrError.INVALID_USERNAME,
                $"Username must be {MIN_USERNAME_LENGTH}-{MAX_USERNAME_LENGTH} bytes, got {length}.");
        }
    }

    internal static void ValidatePassword(string password)
    {
        var length = Encoding.UTF8.GetByteCount(password);
        if (length is < MIN_PASSWORD_LENGTH or > MAX_PASSWORD_LENGTH)
        {
            throw VsrError.Exception(VsrError.INVALID_PASSWORD,
                $"Password must be {MIN_PASSWORD_LENGTH}-{MAX_PASSWORD_LENGTH} bytes, got {length}.");
        }
    }

    internal static void ValidateToken(string token)
    {
        var length = Encoding.UTF8.GetByteCount(token);
        if (length is < MIN_TOKEN_LENGTH or > MAX_TOKEN_LENGTH)
        {
            throw VsrError.Exception(VsrError.INVALID_PERSONAL_ACCESS_TOKEN,
                $"Personal access token must be {MIN_TOKEN_LENGTH}-{MAX_TOKEN_LENGTH} bytes, got {length}.");
        }
    }
}
