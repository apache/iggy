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
///     The 256-byte consensus header, read and written by wire offset. Offsets mirror
///     <c>core/binary_protocol/src/consensus/header.rs</c>. Checksums stay zero: the server does not verify
///     them for client frames.
/// </summary>
internal static class VsrHeader
{
    internal const int HEADER_SIZE = 256;

    internal const int SIZE_OFFSET = 48;
    internal const int COMMAND_OFFSET = 60;

    // The client wire carries no routing group: the server derives it (plane from
    // the operation, partition target from the payload) and stamps it into its own
    // internal header. Every field past the operation therefore sits eight bytes
    // earlier than it did while the client header still carried one.
    internal const int REQUEST_CLIENT_OFFSET = 128;
    internal const int REQUEST_TIMESTAMP_OFFSET = 160;
    internal const int REQUEST_ID_OFFSET = 168;
    internal const int REQUEST_OPERATION_OFFSET = 176;
    internal const int REQUEST_SESSION_OFFSET = 184;
    internal const int REQUEST_RESERVED_OFFSET = 196;

    internal const int REPLY_OPERATION_OFFSET = 208;
    internal const int REPLY_STATUS_OFFSET = 216;

    internal const int EVICTION_CLIENT_OFFSET = 128;
    internal const int EVICTION_PROTOCOL_VERSION_OFFSET = 144;
    internal const int EVICTION_PROTOCOL_VERSION_MIN_OFFSET = 148;
    internal const int EVICTION_REASON_OFFSET = 255;

    /// <summary>
    ///     Encodes the request header for a classic command code and its body. Returns the total frame size
    ///     (header plus body).
    /// </summary>
    /// <remarks>
    ///     Everything that can fail runs before a request id is consumed. The primary accepts any id above the
    ///     watermark, so a gap costs nothing, but a consumed id can never be handed back: re-encoding it for a
    ///     different request would let the client table answer that request from the first one's cached reply.
    /// </remarks>
    internal static int EncodeRequestHeader(Span<byte> header, ConsensusSession session, int code,
        ReadOnlySpan<byte> payload)
    {
        if (header.Length < HEADER_SIZE)
        {
            throw new ArgumentException($"Header buffer must be at least {HEADER_SIZE} bytes.", nameof(header));
        }

        header = header[..HEADER_SIZE];
        header.Clear();

        var operation = VsrOperations.ForCode(code);

        if (payload.Length > int.MaxValue - HEADER_SIZE)
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND, "Request body exceeds the maximum frame size.");
        }

        var frame = session.Resolve(operation);
        var totalSize = HEADER_SIZE + payload.Length;

        BinaryPrimitives.WriteUInt32LittleEndian(header[SIZE_OFFSET..], (uint)totalSize);
        header[COMMAND_OFFSET] = (byte)Command2.Request;
        WriteUInt128(header[REQUEST_CLIENT_OFFSET..], frame.ClientId);
        BinaryPrimitives.WriteUInt64LittleEndian(header[REQUEST_TIMESTAMP_OFFSET..], 0);
        BinaryPrimitives.WriteUInt64LittleEndian(header[REQUEST_ID_OFFSET..], frame.RequestId);
        header[REQUEST_OPERATION_OFFSET] = (byte)operation;
        BinaryPrimitives.WriteUInt64LittleEndian(header[REQUEST_SESSION_OFFSET..], frame.SessionId);

        if (operation == VsrOperation.NonReplicated)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(header[REQUEST_RESERVED_OFFSET..], (uint)code);
        }

        return totalSize;
    }

    internal static Command2 PeekCommand(ReadOnlySpan<byte> header)
    {
        return header[COMMAND_OFFSET] switch
        {
            (byte)Command2.Reply => Command2.Reply,
            (byte)Command2.Eviction => Command2.Eviction,
            _ => Command2.Reserved
        };
    }

    /// <summary>Total frame size (header plus body) the peer announced.</summary>
    internal static uint ReadSize(ReadOnlySpan<byte> header)
    {
        return BinaryPrimitives.ReadUInt32LittleEndian(header[SIZE_OFFSET..]);
    }

    /// <summary>
    ///     Pre-commit deny channel. Nonzero means refused before commit with an empty body; a committed
    ///     rejection stamps 0 here and rides the result section instead.
    /// </summary>
    internal static uint ReadStatus(ReadOnlySpan<byte> header)
    {
        return BinaryPrimitives.ReadUInt32LittleEndian(header[REPLY_STATUS_OFFSET..]);
    }

    internal static VsrOperation ReadReplyOperation(ReadOnlySpan<byte> header)
    {
        var operation = header[REPLY_OPERATION_OFFSET];
        if (!VsrOperations.IsKnown(operation))
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND, $"Reply carries an unknown operation ({operation}).");
        }

        return (VsrOperation)operation;
    }

    internal static EvictionFrame ReadEviction(ReadOnlySpan<byte> header)
    {
        var reason = header[EVICTION_REASON_OFFSET];

        return new EvictionFrame(
            reason is > 0 and <= (byte)EvictionReason.MalformedLogin ? (EvictionReason)reason : null,
            BinaryPrimitives.ReadUInt32LittleEndian(header[EVICTION_PROTOCOL_VERSION_OFFSET..]),
            BinaryPrimitives.ReadUInt32LittleEndian(header[EVICTION_PROTOCOL_VERSION_MIN_OFFSET..]));
    }

    private static void WriteUInt128(Span<byte> destination, UInt128 value)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(destination, (ulong)value);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[8..], (ulong)(value >> 64));
    }
}

/// <summary>Session-terminal eviction frame. Version fields are zero unless the reason is a protocol mismatch.</summary>
internal readonly record struct EvictionFrame(
    EvictionReason? Reason,
    uint ServerProtocolVersion,
    uint ServerProtocolVersionMin);
