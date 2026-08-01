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

using System.Buffers.Binary;
using System.Text;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.VsrTests;

public sealed class LoginRegisterTests
{
    private static int AssertVersionInfo(byte[] body)
    {
        Assert.Equal(LoginRegister.PROTOCOL_VERSION, BinaryPrimitives.ReadUInt32LittleEndian(body));
        var position = 4;
        position += AssertName(body, position, LoginRegister.SDK_NAME);
        position += AssertName(body, position, SdkVersion.Value);

        return position;
    }

    private static int AssertName(byte[] body, int position, string expected)
    {
        var length = body[position];
        Assert.Equal(Encoding.UTF8.GetByteCount(expected), length);
        Assert.Equal(expected, Encoding.UTF8.GetString(body, position + 1, length));

        return 1 + length;
    }

    [Fact]
    public void ProtocolVersion_PacksTenBitsPerComponent()
    {
        Assert.Equal((0u << 20) | (10u << 10) | 3u, LoginRegister.PROTOCOL_VERSION);
    }

    [Fact]
    public void Serialize_WritesVersionInfoThenCredentialsThenContext()
    {
        var body = LoginRegister.Serialize("admin", "secret");

        var position = AssertVersionInfo(body);
        position += AssertName(body, position, "admin");
        position += AssertName(body, position, "secret");

        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(body.AsSpan(position)));
        Assert.Equal(position + 4, body.Length);
    }

    [Fact]
    public void Serialize_AppendsTheClientContextWithAUInt32Length()
    {
        var body = LoginRegister.Serialize("admin", "secret", "ctx");

        var position = AssertVersionInfo(body);
        position += AssertName(body, position, "admin");
        position += AssertName(body, position, "secret");

        Assert.Equal(3u, BinaryPrimitives.ReadUInt32LittleEndian(body.AsSpan(position)));
        Assert.Equal("ctx", Encoding.UTF8.GetString(body, position + 4, 3));
        Assert.Equal(position + 7, body.Length);
    }

    [Fact]
    public void SerializeWithPersonalAccessToken_PutsTheTokenInTheCredentialSlot()
    {
        var body = LoginRegister.SerializeWithPersonalAccessToken("token");

        var position = AssertVersionInfo(body);
        position += AssertName(body, position, "token");

        Assert.Equal(0u, BinaryPrimitives.ReadUInt32LittleEndian(body.AsSpan(position)));
        Assert.Equal(position + 4, body.Length);
    }

    [Fact]
    public void Serialize_RejectsAnEmptyCredential()
    {
        var error = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            LoginRegister.Serialize("admin", string.Empty));

        Assert.Equal(VsrError.INVALID_PASSWORD, error.StatusCode);
    }

    [Fact]
    public void Serialize_RejectsACredentialAboveTheLengthPrefix()
    {
        var error = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            LoginRegister.Serialize("admin", new string('x', 256)));

        Assert.Equal(VsrError.INVALID_PASSWORD, error.StatusCode);
    }

    /// <summary>
    ///     The u8 length prefix is also guarded inside the writer, for the fields no credential check covers.
    /// </summary>
    [Fact]
    public void SerializeWithPersonalAccessToken_RejectsATokenAboveTheLengthPrefix()
    {
        var error = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            LoginRegister.SerializeWithPersonalAccessToken(new string('x', 256)));

        Assert.Equal(VsrError.INVALID_PERSONAL_ACCESS_TOKEN, error.StatusCode);
    }

    [Fact]
    public void SerializeWithPersonalAccessToken_RejectsAnEmptyToken()
    {
        var error = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            LoginRegister.SerializeWithPersonalAccessToken(string.Empty));

        Assert.Equal(VsrError.INVALID_PERSONAL_ACCESS_TOKEN, error.StatusCode);
    }

    [Fact]
    public void Deserialize_ReadsTheRegisterReply()
    {
        var body = VsrTestPayloads.Concat(VsrTestPayloads.UInt32(42), new byte[8], VsrTestPayloads.UInt32(10243),
            [5], "0.8.0"u8.ToArray());
        BinaryPrimitives.WriteUInt64LittleEndian(body.AsSpan(4), 100);

        var response = LoginRegister.Deserialize(body);

        Assert.Equal(42u, response.UserId);
        Assert.Equal(100UL, response.Session);
        Assert.Equal(10243u, response.ServerProtocolVersion);
        Assert.Equal("0.8.0", response.ServerVersion);
    }

    [Fact]
    public void Deserialize_TruncatedReplyIsInvalidFormat()
    {
        var body = VsrTestPayloads.Concat(VsrTestPayloads.UInt32(42), new byte[8], VsrTestPayloads.UInt32(10243),
            [5], "0.8.0"u8.ToArray());

        for (var length = 0; length < body.Length; length++)
        {
            var truncated = body[..length];
            var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
                LoginRegister.Deserialize(truncated));

            Assert.Equal(VsrError.INVALID_FORMAT, exception.StatusCode);
        }
    }

    [Fact]
    public void Deserialize_EmptyReplyIsTheTerminalRegisterRejection()
    {
        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            LoginRegister.Deserialize(ReadOnlySpan<byte>.Empty));

        Assert.Equal(VsrError.INVALID_FORMAT, exception.StatusCode);
        Assert.Contains("rejected the login", exception.Message);
    }
}
