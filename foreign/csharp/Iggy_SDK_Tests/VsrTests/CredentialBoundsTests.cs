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

using Apache.Iggy.Exceptions;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.VsrTests;

/// <summary>
///     Mirrors <c>validate_username</c> / <c>validate_password</c> in
///     <c>core/common/src/traits/binary_impls/mod.rs</c>.
/// </summary>
public sealed class CredentialBoundsTests
{
    [Fact]
    public void ValidateUsername_AcceptsTheServerBounds()
    {
        CredentialBounds.ValidateUsername(new string('a', CredentialBounds.MIN_USERNAME_LENGTH));
        CredentialBounds.ValidateUsername(new string('a', CredentialBounds.MAX_USERNAME_LENGTH));
    }

    [Fact]
    public void ValidateUsername_RejectsOutOfBounds()
    {
        AssertRejects(VsrError.INVALID_USERNAME,
            () => CredentialBounds.ValidateUsername(new string('a', CredentialBounds.MIN_USERNAME_LENGTH - 1)));
        AssertRejects(VsrError.INVALID_USERNAME,
            () => CredentialBounds.ValidateUsername(new string('a', CredentialBounds.MAX_USERNAME_LENGTH + 1)));
    }

    [Fact]
    public void ValidatePassword_AcceptsTheServerBounds()
    {
        CredentialBounds.ValidatePassword(new string('a', CredentialBounds.MIN_PASSWORD_LENGTH));
        CredentialBounds.ValidatePassword(new string('a', CredentialBounds.MAX_PASSWORD_LENGTH));
    }

    [Fact]
    public void ValidatePassword_RejectsOutOfBounds()
    {
        AssertRejects(VsrError.INVALID_PASSWORD,
            () => CredentialBounds.ValidatePassword(new string('a', CredentialBounds.MIN_PASSWORD_LENGTH - 1)));
        AssertRejects(VsrError.INVALID_PASSWORD,
            () => CredentialBounds.ValidatePassword(new string('a', CredentialBounds.MAX_PASSWORD_LENGTH + 1)));
    }

    [Fact]
    public void ValidatePassword_CountsUtf8BytesNotChars()
    {
        // 51 two-byte code points encode to 102 bytes, over the server's limit despite fitting in chars.
        AssertRejects(VsrError.INVALID_PASSWORD, () => CredentialBounds.ValidatePassword(new string('ż', 51)));
    }

    private static void AssertRejects(int statusCode, Action action)
    {
        Assert.Equal(statusCode, Assert.Throws<IggyInvalidStatusCodeException>(action).StatusCode);
    }
}
