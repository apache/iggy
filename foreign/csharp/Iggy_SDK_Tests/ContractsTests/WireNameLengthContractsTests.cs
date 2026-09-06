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

using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Partitioning = Apache.Iggy.Kinds.Partitioning;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.ContractsTests;

public sealed class WireNameLengthContractsTests
{
    // 129 characters, but 258 UTF-8 bytes: a character count would let this through.
    private static readonly string OverLimit = new('ż', 129);

    // 128 characters, exactly 255 UTF-8 bytes: the widest name the one-byte prefix can carry.
    private static readonly string AtLimit = new string('ż', 127) + "a";

    private static readonly Identifier Id = Identifier.Numeric(1);

    public static TheoryData<string, string, Func<byte[]>> EmptyCases => new()
    {
        { nameof(TcpContracts.CreateStream), "name", () => TcpContracts.CreateStream("") },
        { nameof(TcpContracts.UpdateStream), "name", () => TcpContracts.UpdateStream(Id, "") },
        { nameof(TcpContracts.CreateGroup), "name", () => TcpContracts.CreateGroup(Id, Id, "") },
        { nameof(TcpContracts.CreatePersonalAccessToken), "name", () => TcpContracts.CreatePersonalAccessToken("", null) },
        { nameof(TcpContracts.DeletePersonalRequestToken), "name", () => TcpContracts.DeletePersonalRequestToken("") },
        { nameof(TcpContracts.LoginWithPersonalAccessToken), "token", () => TcpContracts.LoginWithPersonalAccessToken("") },
        { nameof(TcpContracts.LoginUser) + "/userName", "userName", () => TcpContracts.LoginUser("", "pass", null, null) },
        { nameof(TcpContracts.LoginUser) + "/password", "password", () => TcpContracts.LoginUser("user", "", null, null) }
    };

    public static TheoryData<string, string, Func<byte[]>> OverLimitCases => new()
    {
        { nameof(TcpContracts.CreateStream), "name", () => TcpContracts.CreateStream(OverLimit) },
        { nameof(TcpContracts.UpdateStream), "name", () => TcpContracts.UpdateStream(Id, OverLimit) },
        { nameof(TcpContracts.CreateGroup), "name", () => TcpContracts.CreateGroup(Id, Id, OverLimit) },
        { nameof(TcpContracts.CreatePersonalAccessToken), "name", () => TcpContracts.CreatePersonalAccessToken(OverLimit, null) },
        { nameof(TcpContracts.DeletePersonalRequestToken), "name", () => TcpContracts.DeletePersonalRequestToken(OverLimit) },
        { nameof(TcpContracts.LoginWithPersonalAccessToken), "token", () => TcpContracts.LoginWithPersonalAccessToken(OverLimit) },
        { nameof(TcpContracts.LoginUser) + "/userName", "userName", () => TcpContracts.LoginUser(OverLimit, "pass", null, null) },
        { nameof(TcpContracts.LoginUser) + "/password", "password", () => TcpContracts.LoginUser("user", OverLimit, null, null) }
    };

    // Each frame carries the name right after a fixed-size prefix; the offset says where its length byte sits.
    public static TheoryData<string, int, Func<byte[]>> AtLimitCases => new()
    {
        { nameof(TcpContracts.CreateStream), 0, () => TcpContracts.CreateStream(AtLimit) },
        { nameof(TcpContracts.UpdateStream), 2 + Id.Length, () => TcpContracts.UpdateStream(Id, AtLimit) },
        { nameof(TcpContracts.CreateGroup), 2 * (2 + Id.Length), () => TcpContracts.CreateGroup(Id, Id, AtLimit) },
        { nameof(TcpContracts.CreatePersonalAccessToken), 0, () => TcpContracts.CreatePersonalAccessToken(AtLimit, null) },
        { nameof(TcpContracts.DeletePersonalRequestToken), 0, () => TcpContracts.DeletePersonalRequestToken(AtLimit) },
        { nameof(TcpContracts.LoginWithPersonalAccessToken), 0, () => TcpContracts.LoginWithPersonalAccessToken(AtLimit) },
        { nameof(TcpContracts.LoginUser) + "/userName", 0, () => TcpContracts.LoginUser(AtLimit, "pass", null, null) },
        { nameof(TcpContracts.LoginUser) + "/password", 1 + 4, () => TcpContracts.LoginUser("user", AtLimit, null, null) }
    };

    // User management shares the server's credential bounds with the login path, not the 1-255 wire rule.
    public static TheoryData<string, int, Func<byte[]>> CredentialCases => new()
    {
        { nameof(TcpContracts.CreateUser) + "/empty userName", VsrError.INVALID_USERNAME, () => TcpContracts.CreateUser("", "pass", UserStatus.Active) },
        { nameof(TcpContracts.CreateUser) + "/short userName", VsrError.INVALID_USERNAME, () => TcpContracts.CreateUser("ab", "pass", UserStatus.Active) },
        { nameof(TcpContracts.CreateUser) + "/long userName", VsrError.INVALID_USERNAME, () => TcpContracts.CreateUser(new string('a', 51), "pass", UserStatus.Active) },
        { nameof(TcpContracts.CreateUser) + "/empty password", VsrError.INVALID_PASSWORD, () => TcpContracts.CreateUser("user", "", UserStatus.Active) },
        { nameof(TcpContracts.CreateUser) + "/long password", VsrError.INVALID_PASSWORD, () => TcpContracts.CreateUser("user", new string('a', 101), UserStatus.Active) },
        { nameof(TcpContracts.UpdateUser) + "/empty userName", VsrError.INVALID_USERNAME, () => TcpContracts.UpdateUser(Id, "", null) },
        { nameof(TcpContracts.UpdateUser) + "/long userName", VsrError.INVALID_USERNAME, () => TcpContracts.UpdateUser(Id, new string('a', 51), null) },
        { nameof(TcpContracts.ChangePassword) + "/empty current", VsrError.INVALID_PASSWORD, () => TcpContracts.ChangePassword(Id, "", "new") },
        { nameof(TcpContracts.ChangePassword) + "/long current", VsrError.INVALID_PASSWORD, () => TcpContracts.ChangePassword(Id, new string('a', 101), "new") },
        { nameof(TcpContracts.ChangePassword) + "/empty new", VsrError.INVALID_PASSWORD, () => TcpContracts.ChangePassword(Id, "old", "") },
        { nameof(TcpContracts.ChangePassword) + "/long new", VsrError.INVALID_PASSWORD, () => TcpContracts.ChangePassword(Id, "old", new string('a', 101)) }
    };

    [Theory]
    [MemberData(nameof(OverLimitCases))]
    public void Contract_WithAStringOverTheWireLimitInBytes_Throws(string contract, string parameterName,
        Func<byte[]> serialize)
    {
        Assert.NotEmpty(contract);
        var exception = Assert.Throws<ArgumentException>(serialize);
        Assert.Equal(parameterName, exception.ParamName);
    }

    [Theory]
    [MemberData(nameof(EmptyCases))]
    public void Contract_WithAnEmptyString_Throws(string contract, string parameterName, Func<byte[]> serialize)
    {
        Assert.NotEmpty(contract);
        var exception = Assert.Throws<ArgumentException>(serialize);
        Assert.Equal(parameterName, exception.ParamName);
    }

    [Theory]
    [MemberData(nameof(AtLimitCases))]
    public void Contract_WithAStringOfExactly255Bytes_PrefixesTheFullLength(string contract, int prefixOffset,
        Func<byte[]> serialize)
    {
        Assert.NotEmpty(contract);
        var bytes = serialize();

        Assert.Equal(255, bytes[prefixOffset]);
        Assert.Equal((byte)'a', bytes[prefixOffset + 255]);
    }

    [Theory]
    [MemberData(nameof(CredentialCases))]
    public void UserContract_WithACredentialOutsideTheServerBounds_ThrowsTheTypedStatus(string contract,
        int statusCode, Func<byte[]> serialize)
    {
        Assert.NotEmpty(contract);
        var exception = Assert.Throws<IggyInvalidStatusCodeException>(serialize);
        Assert.Equal(statusCode, exception.StatusCode);
        Assert.False(exception.FromServer);
    }

    [Fact]
    public void CreateUser_WithCredentialsAtTheServerBounds_Serializes()
    {
        var bytes = TcpContracts.CreateUser(new string('u', 50), new string('p', 100), UserStatus.Active);

        Assert.Equal(50, bytes[0]);
        Assert.Equal(100, bytes[1 + 50]);
    }

    [Fact]
    public void LoginUser_WithEmptyVersionAndContext_Serializes()
    {
        var bytes = TcpContracts.LoginUser("user", "pass", "", "");

        Assert.Equal(1 + 4 + 1 + 4 + 4 + 4, bytes.Length);
    }

    [Fact]
    public void UpdateUser_WithStatusOnly_SerializesExactlyOneNameFlagAndStatusPair()
    {
        var bytes = TcpContracts.UpdateUser(Id, null, UserStatus.Inactive);

        Assert.Equal(new byte[] { 1, 4, 1, 0, 0, 0, 0, 1, (byte)UserStatus.Inactive }, bytes);
    }

    [Fact]
    public void UpdateUser_WithNameOnly_SerializesExactlyOneNameAndStatusFlag()
    {
        var bytes = TcpContracts.UpdateUser(Id, "abc", null);

        Assert.Equal(new byte[] { 1, 4, 1, 0, 0, 0, 1, 3, (byte)'a', (byte)'b', (byte)'c', 0 }, bytes);
    }

    [Fact]
    public void CreateStream_WithANonAsciiName_PrefixesTheUtf8ByteCount()
    {
        var bytes = TcpContracts.CreateStream("café");

        Assert.Equal(5, bytes[0]);
        Assert.Equal(6, bytes.Length);
    }

    [Fact]
    public void GetUser_WithANonAsciiStringIdentifier_SerializesTheUtf8Bytes()
    {
        var bytes = TcpContracts.GetUser(Identifier.String("café"));

        Assert.Equal(new byte[] { 2, 5, (byte)'c', (byte)'a', (byte)'f', 0xC3, 0xA9 }, bytes);
    }

    [Fact]
    public void UpdateStream_WithANonAsciiStringIdentifier_PlacesTheNameAfterTheUtf8Bytes()
    {
        var bytes = TcpContracts.UpdateStream(Identifier.String("café"), "topic");

        Assert.Equal(
            new byte[]
            {
                2, 5, (byte)'c', (byte)'a', (byte)'f', 0xC3, 0xA9,
                5, (byte)'t', (byte)'o', (byte)'p', (byte)'i', (byte)'c'
            }, bytes);
    }

    [Fact]
    public void Identifier_BuiltWithAnObjectInitializerOver255Bytes_Throws()
    {
        var exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new Identifier { Kind = IdKind.String, Value = new byte[300] });
        Assert.Equal("Value", exception.ParamName);
    }

    [Fact]
    public void Partitioning_BuiltWithAnObjectInitializerOver255Bytes_Throws()
    {
        var exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new Partitioning { Kind = Enums.Partitioning.MessageKey, Value = new byte[300] });
        Assert.Equal("Value", exception.ParamName);
    }

    [Fact]
    public void Identifier_BuiltWithAnObjectInitializerOf255Bytes_KeepsTheFullLength()
    {
        var identifier = new Identifier { Kind = IdKind.String, Value = new byte[255] };

        Assert.Equal(255, identifier.Length);
    }
}
