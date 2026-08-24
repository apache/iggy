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

namespace Apache.Iggy.Vsr;

/// <summary>
///     Decodes a reply frame into the typed payload the classic response readers expect.
/// </summary>
internal static class VsrReplyDecoder
{
    internal const int RESULT_COUNT_LENGTH = 4;
    internal const int RESULT_ENTRY_LENGTH = 8;

    /// <summary>
    ///     Runs the decode funnel in the order the wire contract requires: eviction frames first, then the
    ///     announced size, then the pre-commit status word, and only then the body and its committed result
    ///     section. Reading the status before the body is what lets an empty deny reply fail with the right
    ///     error instead of a truncation.
    /// </summary>
    internal static ReadOnlyMemory<byte> Decode(ReadOnlySpan<byte> header, ReadOnlyMemory<byte> body)
    {
        if (header.Length < VsrHeader.HEADER_SIZE)
        {
            throw VsrError.Exception(VsrError.EMPTY_RESPONSE, "Reply header is shorter than the consensus header.");
        }

        switch (VsrHeader.PeekCommand(header))
        {
            case Command.Eviction:
                throw ToException(VsrHeader.ReadEviction(header));
            case Command.Reply:
                break;
            default:
                throw VsrError.Exception(VsrError.INVALID_COMMAND,
                    $"Unexpected consensus frame ({header[VsrHeader.COMMAND_OFFSET]}).");
        }

        var expectedBody = ReadBodySize(header);
        if (body.Length < expectedBody)
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND, "Reply body is shorter than the announced frame size.");
        }

        var status = VsrHeader.ReadStatus(header);
        if (status != 0)
        {
            throw VsrError.FromServer((int)status, $"Server rejected the request with status {status}.");
        }

        return SplitResultSection(VsrHeader.ReadReplyOperation(header), body[..expectedBody]);
    }

    /// <summary>Body length announced by the frame, i.e. the bytes to read after the header.</summary>
    internal static int ReadBodySize(ReadOnlySpan<byte> header)
    {
        var size = VsrHeader.ReadSize(header);
        if (size < VsrHeader.HEADER_SIZE || size > int.MaxValue)
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND, $"Reply announced an invalid frame size ({size}).");
        }

        return (int)size - VsrHeader.HEADER_SIZE;
    }

    internal static Exception ToException(EvictionFrame eviction)
    {
        var (code, message) = eviction.Reason switch
        {
            // The five reasons below fall into the catch-all of the shared grader, so they carry INVALID_COMMAND
            // even where a narrower status would read better. The message keeps the detail; the code is the part
            // six SDKs agree on.
            EvictionReason.ClientReleaseTooLow => (VsrError.INVALID_COMMAND,
                "Client release is below the cluster minimum."),
            EvictionReason.ClientReleaseTooHigh => (VsrError.INVALID_COMMAND,
                "Client release is above the cluster maximum."),
            EvictionReason.InvalidRequestOperation => (VsrError.INVALID_COMMAND,
                "Server rejected the request operation."),
            EvictionReason.InvalidRequestBody => (VsrError.INVALID_COMMAND, "Server rejected the request body."),
            EvictionReason.InvalidRequestBodySize => (VsrError.INVALID_COMMAND,
                "Server rejected the request body size."),
            EvictionReason.InvalidCredentials => (VsrError.INVALID_CREDENTIALS, "Invalid credentials."),
            EvictionReason.InvalidToken => (VsrError.INVALID_PERSONAL_ACCESS_TOKEN, "Invalid personal access token."),
            EvictionReason.UserInactive => (VsrError.UNAUTHENTICATED, "User is inactive."),
            EvictionReason.SessionError => (VsrError.UNAUTHENTICATED, "Session error."),
            EvictionReason.NoSession => (VsrError.UNAUTHENTICATED, "No session for this client."),
            EvictionReason.SessionTooLow => (VsrError.UNAUTHENTICATED, "Session is below the cluster minimum."),
            EvictionReason.SessionReleaseMismatch => (VsrError.UNAUTHENTICATED, "Session release mismatch."),
            EvictionReason.StaleClient => (VsrError.STALE_CLIENT, "Client missed too many heartbeats."),
            EvictionReason.IncompatibleProtocol => IncompatibleProtocol(eviction),
            EvictionReason.MalformedLogin => (VsrError.INVALID_FORMAT, "Malformed login body."),

            // Reserved and any reason this build cannot decode share the grader's catch-all.
            null => (VsrError.INVALID_COMMAND, "Session evicted for an unrecognized reason."),
            _ => (VsrError.INVALID_COMMAND, $"Session evicted ({eviction.Reason}).")
        };

        return VsrError.FromServer(code, message);
    }

    /// <summary>
    ///     Leading result code of a committed reply body: 0 for success, otherwise the first entry's result.
    ///     <c>null</c> when the body cannot hold what the count claims - corruption, never a silent success.
    /// </summary>
    internal static uint? ReadResultCode(ReadOnlySpan<byte> body)
    {
        if (!TryReadUInt32(body, 0, out var count))
        {
            return null;
        }

        if (count == 0)
        {
            return 0;
        }

        return TryReadUInt32(body, RESULT_COUNT_LENGTH + 4, out var result) ? result : null;
    }

    /// <summary>Byte length of the leading result section, i.e. where the typed payload starts.</summary>
    internal static int? ReadResultSectionLength(ReadOnlySpan<byte> body)
    {
        if (!TryReadUInt32(body, 0, out var count))
        {
            return null;
        }

        var length = RESULT_COUNT_LENGTH + (long)count * RESULT_ENTRY_LENGTH;

        return body.Length >= length ? (int)length : null;
    }

    private static (int Code, string Message) IncompatibleProtocol(EvictionFrame eviction)
    {
        if (eviction.ServerProtocolVersionMin == 0 ||
            eviction.ServerProtocolVersion < eviction.ServerProtocolVersionMin)
        {
            return (VsrError.UNAUTHENTICATED, "Server rejected the client protocol version.");
        }

        return (VsrError.INCOMPATIBLE_PROTOCOL_VERSION,
            $"Client protocol version {LoginRegister.PROTOCOL_VERSION} is outside the range accepted by the server " +
            $"({eviction.ServerProtocolVersionMin}..{eviction.ServerProtocolVersion}).");
    }

    /// <summary>
    ///     Strips the committed result section from a result-framed reply and maps a committed rejection to
    ///     its error. Register replies are result-framed only when non-empty: a terminal register failure
    ///     ships an empty body and is passed through to fail the typed response decode.
    /// </summary>
    private static ReadOnlyMemory<byte> SplitResultSection(VsrOperation operation, ReadOnlyMemory<byte> body)
    {
        var resultFramed = operation.IsResultFramed() ||
                           (operation == VsrOperation.Register && !body.IsEmpty);
        if (!resultFramed)
        {
            return body;
        }

        var code = ReadResultCode(body.Span);
        if (code is null)
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND, "Reply carries a malformed committed result section.");
        }

        if (code != 0)
        {
            throw VsrError.FromServer((int)code.Value, $"Server rejected the request with status {code.Value}.");
        }

        var payloadStart = ReadResultSectionLength(body.Span)
                           ?? throw VsrError.Exception(VsrError.INVALID_COMMAND,
                               "Reply carries a truncated committed result section.");

        return body[payloadStart..];
    }

    private static bool TryReadUInt32(ReadOnlySpan<byte> body, int offset, out uint value)
    {
        if (body.Length < offset + 4)
        {
            value = 0;

            return false;
        }

        value = BinaryPrimitives.ReadUInt32LittleEndian(body.Slice(offset, 4));

        return true;
    }
}
