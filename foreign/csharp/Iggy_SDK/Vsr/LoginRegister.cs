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
using Apache.Iggy.Utils;

namespace Apache.Iggy.Vsr;

/// <summary>
///     The register handshake bodies. VSR replaces the legacy login codes with
///     <c>LOGIN_REGISTER</c> / <c>LOGIN_REGISTER_WITH_PAT</c>, whose bodies lead with the client version
///     info the server gates on before it touches credentials.
/// </summary>
internal static class LoginRegister
{
    internal const string SDK_NAME = "csharp-sdk";

    /// <summary>Semver of the <c>iggy_binary_protocol</c> crate this SDK is built against.</summary>
    internal const int PROTOCOL_VERSION_MAJOR = 0;

    internal const int PROTOCOL_VERSION_MINOR = 11;
    internal const int PROTOCOL_VERSION_PATCH = 0;

    /// <summary>Packed protocol version: <c>major &lt;&lt; 20 | minor &lt;&lt; 10 | patch</c>, 10 bits each.</summary>
    internal const uint PROTOCOL_VERSION =
        ((uint)PROTOCOL_VERSION_MAJOR << 20) | ((uint)PROTOCOL_VERSION_MINOR << 10) | PROTOCOL_VERSION_PATCH;

    private const int MaxWireNameLength = 255;

    private static int VersionInfoLength => 4 + NameLength(SDK_NAME) + NameLength(SdkVersion.Value);

    internal static byte[] Serialize(string username, string password, string? clientContext = null)
    {
        CredentialBounds.ValidateUsername(username);
        CredentialBounds.ValidatePassword(password);

        var writer = new BodyWriter(VersionInfoLength + NameLength(username) + NameLength(password) + 4 +
                                    ContextLength(clientContext));
        writer.WriteVersionInfo();
        writer.WriteName(username, nameof(username));
        writer.WriteName(password, nameof(password));
        writer.WriteContext(clientContext);

        return writer.Buffer;
    }

    internal static byte[] SerializeWithPersonalAccessToken(string token, string? clientContext = null)
    {
        CredentialBounds.ValidateToken(token);

        var writer = new BodyWriter(VersionInfoLength + NameLength(token) + 4 + ContextLength(clientContext));
        writer.WriteVersionInfo();
        writer.WriteName(token, nameof(token));
        writer.WriteContext(clientContext);

        return writer.Buffer;
    }

    /// <summary>
    ///     <c>[user_id u32][session u64][server_protocol_version u32][server_version len u8 + bytes]</c>.
    /// </summary>
    internal static LoginRegisterResponse Deserialize(ReadOnlySpan<byte> body)
    {
        if (body.IsEmpty)
        {
            // The server fast-fails a terminal register failure (invalid credentials, invalid token,
            // inactive user) with an empty reply instead of a typed error frame, and the reason is not
            // recoverable from the wire. Same INVALID_FORMAT surface as the Rust SDK.
            throw VsrError.Exception(VsrError.INVALID_FORMAT,
                "Server rejected the login. The register reply is empty, which the server sends for invalid " +
                "credentials, an invalid personal access token, or an inactive user.");
        }

        if (body.Length < 17)
        {
            throw VsrError.Exception(VsrError.INVALID_FORMAT, "Register reply is truncated.");
        }

        var serverVersionLength = body[16];
        if (serverVersionLength == 0 || body.Length < 17 + serverVersionLength)
        {
            throw VsrError.Exception(VsrError.INVALID_FORMAT, "Register reply carries a malformed server version.");
        }

        return new LoginRegisterResponse(BinaryPrimitives.ReadUInt32LittleEndian(body[..4]),
            BinaryPrimitives.ReadUInt64LittleEndian(body[4..12]),
            BinaryPrimitives.ReadUInt32LittleEndian(body[12..16]),
            Encoding.UTF8.GetString(body.Slice(17, serverVersionLength)));
    }

    private static int NameLength(string value)
    {
        return 1 + Encoding.UTF8.GetByteCount(value);
    }

    private static int ContextLength(string? clientContext)
    {
        return clientContext is null ? 0 : Encoding.UTF8.GetByteCount(clientContext);
    }

    private struct BodyWriter
    {
        private int _position;

        internal BodyWriter(int length)
        {
            Buffer = new byte[length];
            _position = 0;
        }

        internal byte[] Buffer { get; }

        internal void WriteVersionInfo()
        {
            BinaryPrimitives.WriteUInt32LittleEndian(Buffer.AsSpan(_position, 4), PROTOCOL_VERSION);
            _position += 4;
            WriteName(SDK_NAME, nameof(SDK_NAME));
            WriteName(SdkVersion.Value, nameof(SdkVersion));
        }

        internal void WriteName(string value, string name)
        {
            var length = Encoding.UTF8.GetByteCount(value);
            if (length is 0 or > MaxWireNameLength)
            {
                throw new ArgumentException($"{name} must be 1-{MaxWireNameLength} UTF-8 bytes, got {length}.", name);
            }

            Buffer[_position] = (byte)length;
            _position += 1;
            Encoding.UTF8.GetBytes(value, Buffer.AsSpan(_position, length));
            _position += length;
        }

        internal void WriteContext(string? clientContext)
        {
            var length = ContextLength(clientContext);
            BinaryPrimitives.WriteUInt32LittleEndian(Buffer.AsSpan(_position, 4), (uint)length);
            _position += 4;
            if (clientContext is not null && length > 0)
            {
                Encoding.UTF8.GetBytes(clientContext, Buffer.AsSpan(_position, length));
                _position += length;
            }
        }
    }
}

internal readonly record struct LoginRegisterResponse(
    uint UserId,
    ulong Session,
    uint ServerProtocolVersion,
    string ServerVersion);
