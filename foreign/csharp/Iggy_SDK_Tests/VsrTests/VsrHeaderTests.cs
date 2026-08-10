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
using Apache.Iggy.Exceptions;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.VsrTests;

public sealed class VsrHeaderTests
{
    private static ConsensusSession BoundSession(ulong session = 5)
    {
        var consensusSession = new ConsensusSession(0x0102_0304_0506_0708);
        consensusSession.Resolve(VsrOperation.Register);
        consensusSession.Bind(session);

        return consensusSession;
    }

    private static byte[] Encode(ConsensusSession session, int code, int bodyLength, out int totalSize,
        ulong ns = 0)
    {
        var header = new byte[VsrHeader.HEADER_SIZE];
        totalSize = VsrHeader.EncodeRequestHeader(header, session, code, ns, bodyLength);

        return header;
    }

    private static ulong ReadUInt64(byte[] header, int offset)
    {
        return BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(offset));
    }

    private static uint ReadUInt32(byte[] header, int offset)
    {
        return BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(offset));
    }

    /// <summary>
    ///     The caller resolves the routing namespace from its typed identifiers, but only partition-plane
    ///     operations carry it: a stray namespace on a metadata or non-replicated request would route it off
    ///     the metadata consensus group.
    /// </summary>
    [Fact]
    public void Encode_NonPartitionOpsIgnoreTheCallerNamespace()
    {
        var session = BoundSession();

        var metadataHeader = Encode(session, CommandCodes.CREATE_STREAM_CODE, 1, out _, VsrNamespace.Pack(1, 2, 3));
        var nonReplicatedHeader = Encode(session, CommandCodes.PING_CODE, 0, out _, VsrNamespace.Pack(1, 2, 3));

        Assert.Equal(0UL, ReadUInt64(metadataHeader, VsrHeader.REQUEST_NAMESPACE_OFFSET));
        Assert.Equal(0UL, ReadUInt64(nonReplicatedHeader, VsrHeader.REQUEST_NAMESPACE_OFFSET));
    }

    [Fact]
    public void Encode_RegisterUsesZeroRequestAndSessionOnMetadataNamespace()
    {
        var session = new ConsensusSession(7);
        var payload = LoginRegister.Serialize("admin", "secret");

        var header = Encode(session, CommandCodes.LOGIN_REGISTER_CODE, payload.Length, out var totalSize);

        Assert.Equal(VsrHeader.HEADER_SIZE + payload.Length, totalSize);
        Assert.Equal((uint)totalSize, ReadUInt32(header, VsrHeader.SIZE_OFFSET));
        Assert.Equal((byte)Command2.Request, header[VsrHeader.COMMAND_OFFSET]);
        Assert.Equal((byte)VsrOperation.Register, header[VsrHeader.REQUEST_OPERATION_OFFSET]);
        Assert.Equal(0UL, ReadUInt64(header, VsrHeader.REQUEST_ID_OFFSET));
        Assert.Equal(0UL, ReadUInt64(header, VsrHeader.REQUEST_SESSION_OFFSET));
        Assert.Equal(0UL, ReadUInt64(header, VsrHeader.REQUEST_TIMESTAMP_OFFSET));
        Assert.Equal(VsrNamespace.METADATA_CONSENSUS_NAMESPACE, ReadUInt64(header, VsrHeader.REQUEST_NAMESPACE_OFFSET));
        Assert.Equal(7UL, ReadUInt64(header, VsrHeader.REQUEST_CLIENT_OFFSET));
        Assert.Equal(0UL, ReadUInt64(header, VsrHeader.REQUEST_CLIENT_OFFSET + 8));
    }

    [Fact]
    public void Encode_WritesClientIdAsTwoLittleEndianHalvesLowFirst()
    {
        var session = new ConsensusSession(new UInt128(0xAABB_CCDD_EEFF_0011, 0x1122_3344_5566_7788));

        var header = Encode(session, CommandCodes.PING_CODE, 0, out _);

        Assert.Equal(0x1122_3344_5566_7788UL, ReadUInt64(header, VsrHeader.REQUEST_CLIENT_OFFSET));
        Assert.Equal(0xAABB_CCDD_EEFF_0011UL, ReadUInt64(header, VsrHeader.REQUEST_CLIENT_OFFSET + 8));
    }

    [Fact]
    public void Encode_NonReplicatedDoesNotAdvanceCounterAndCarriesCodeInReserved()
    {
        var session = BoundSession();

        var header = Encode(session, CommandCodes.PING_CODE, 0, out _);

        Assert.Equal((byte)VsrOperation.NonReplicated, header[VsrHeader.REQUEST_OPERATION_OFFSET]);
        Assert.Equal(1UL, ReadUInt64(header, VsrHeader.REQUEST_ID_OFFSET));
        Assert.Equal(1UL, session.RequestCounter);
        Assert.Equal(5UL, ReadUInt64(header, VsrHeader.REQUEST_SESSION_OFFSET));
        Assert.Equal(0UL, ReadUInt64(header, VsrHeader.REQUEST_NAMESPACE_OFFSET));
        Assert.Equal((uint)CommandCodes.PING_CODE, ReadUInt32(header, VsrHeader.REQUEST_RESERVED_OFFSET));
    }

    [Fact]
    public void Encode_UnknownCodeRidesNonReplicated()
    {
        var session = BoundSession();

        var header = Encode(session, 9999, 0, out _);

        Assert.Equal((byte)VsrOperation.NonReplicated, header[VsrHeader.REQUEST_OPERATION_OFFSET]);
        Assert.Equal(9999u, ReadUInt32(header, VsrHeader.REQUEST_RESERVED_OFFSET));
    }

    [Fact]
    public void Encode_NonReplicatedWithoutSessionSendsSessionZero()
    {
        var session = new ConsensusSession(1);

        var header = Encode(session, CommandCodes.PING_CODE, 0, out _);

        Assert.Equal(0UL, ReadUInt64(header, VsrHeader.REQUEST_SESSION_OFFSET));
    }

    [Fact]
    public void Encode_MetadataAdvancesTheCounter()
    {
        var session = BoundSession();

        var header = Encode(session, CommandCodes.CREATE_STREAM_CODE, 3, out _);

        Assert.Equal((byte)VsrOperation.CreateStream, header[VsrHeader.REQUEST_OPERATION_OFFSET]);
        Assert.Equal(1UL, ReadUInt64(header, VsrHeader.REQUEST_ID_OFFSET));
        Assert.Equal(2UL, session.RequestCounter);
        Assert.Equal(0UL, ReadUInt64(header, VsrHeader.REQUEST_NAMESPACE_OFFSET));
        Assert.Equal(0u, ReadUInt32(header, VsrHeader.REQUEST_RESERVED_OFFSET));
    }

    [Fact]
    public void Encode_LogoutAdvancesTheCounterOnMetadataNamespace()
    {
        var session = BoundSession();

        var header = Encode(session, CommandCodes.LOGOUT_USER_CODE, 0, out _);

        Assert.Equal((byte)VsrOperation.Logout, header[VsrHeader.REQUEST_OPERATION_OFFSET]);
        Assert.Equal(1UL, ReadUInt64(header, VsrHeader.REQUEST_ID_OFFSET));
        Assert.Equal(2UL, session.RequestCounter);
        Assert.Equal(VsrNamespace.METADATA_CONSENSUS_NAMESPACE, ReadUInt64(header, VsrHeader.REQUEST_NAMESPACE_OFFSET));
    }

    [Fact]
    public void Encode_PartitionOpDoesNotAdvanceTheCounter()
    {
        var session = BoundSession();

        var header = Encode(session, CommandCodes.SEND_MESSAGES_CODE, 16, out _, VsrNamespace.Pack(2, 3, 4));

        Assert.Equal((byte)VsrOperation.SendMessages, header[VsrHeader.REQUEST_OPERATION_OFFSET]);
        Assert.Equal(1UL, ReadUInt64(header, VsrHeader.REQUEST_ID_OFFSET));
        Assert.Equal(1UL, session.RequestCounter);
        Assert.Equal(VsrNamespace.Pack(2, 3, 4), ReadUInt64(header, VsrHeader.REQUEST_NAMESPACE_OFFSET));
    }

    [Fact]
    public void Encode_ReplicatedOpWithoutSessionIsUnauthenticated()
    {
        var session = new ConsensusSession(1);

        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            Encode(session, CommandCodes.CREATE_STREAM_CODE, 1, out _));

        Assert.Equal(VsrError.UNAUTHENTICATED, exception.StatusCode);
        Assert.Equal(1UL, session.RequestCounter);
    }

    [Fact]
    public void Encode_ClearsStaleBytesFromAReusedBuffer()
    {
        var header = new byte[VsrHeader.HEADER_SIZE];
        Array.Fill(header, (byte)0xFF);
        var session = BoundSession();

        VsrHeader.EncodeRequestHeader(header, session, CommandCodes.CREATE_STREAM_CODE, 0, 1);

        Assert.Equal(0u, ReadUInt32(header, VsrHeader.REQUEST_RESERVED_OFFSET));
        Assert.All(header[..VsrHeader.SIZE_OFFSET], stale => Assert.Equal(0, stale));
    }

    [Fact]
    public void Encode_RejectsAShortBuffer()
    {
        var session = BoundSession();

        Assert.Throws<ArgumentException>(() =>
            VsrHeader.EncodeRequestHeader(new byte[VsrHeader.HEADER_SIZE - 1], session, CommandCodes.PING_CODE, 0,
                0));
    }

    [Fact]
    public void PeekCommand_MapsOnlyTheClientVisibleFrames()
    {
        var header = new byte[VsrHeader.HEADER_SIZE];

        header[VsrHeader.COMMAND_OFFSET] = (byte)Command2.Reply;
        Assert.Equal(Command2.Reply, VsrHeader.PeekCommand(header));

        header[VsrHeader.COMMAND_OFFSET] = (byte)Command2.Eviction;
        Assert.Equal(Command2.Eviction, VsrHeader.PeekCommand(header));

        header[VsrHeader.COMMAND_OFFSET] = 6;
        Assert.Equal(Command2.Reserved, VsrHeader.PeekCommand(header));
    }

    [Fact]
    public void ReadReplyOperation_RejectsAnUnknownDiscriminant()
    {
        var header = new byte[VsrHeader.HEADER_SIZE];
        header[VsrHeader.REPLY_OPERATION_OFFSET] = 200;

        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() => VsrHeader.ReadReplyOperation(header));

        Assert.Equal(VsrError.INVALID_COMMAND, exception.StatusCode);
    }

    [Fact]
    public void ReadEviction_ReadsReasonAndProtocolWindow()
    {
        var header = new byte[VsrHeader.HEADER_SIZE];
        header[VsrHeader.EVICTION_REASON_OFFSET] = (byte)EvictionReason.IncompatibleProtocol;
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(VsrHeader.EVICTION_PROTOCOL_VERSION_OFFSET), 10243);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(VsrHeader.EVICTION_PROTOCOL_VERSION_MIN_OFFSET), 10240);

        var eviction = VsrHeader.ReadEviction(header);

        Assert.Equal(EvictionReason.IncompatibleProtocol, eviction.Reason);
        Assert.Equal(10243u, eviction.ServerProtocolVersion);
        Assert.Equal(10240u, eviction.ServerProtocolVersionMin);
    }

    /// <summary>
    ///     An undecodable reason stays null rather than collapsing onto a named one, and grades through the
    ///     shared grader's catch-all like every reason the Rust mapping does not name.
    /// </summary>
    [Fact]
    public void ReadEviction_UnknownReasonDecodesAsUndecodable()
    {
        var header = new byte[VsrHeader.HEADER_SIZE];
        header[VsrHeader.EVICTION_REASON_OFFSET] = 200;

        Assert.Null(VsrHeader.ReadEviction(header).Reason);
        Assert.Equal(VsrError.INVALID_COMMAND,
            Assert.IsType<IggyInvalidStatusCodeException>(VsrReplyDecoder.ToException(VsrHeader.ReadEviction(header)))
                .StatusCode);
    }

    /// <summary>
    ///     Reason 0 is the Reserved sentinel the server rejects on the wire, so it decodes as an unrecognized
    ///     reason rather than as one. Rust grades it through the same catch-all.
    /// </summary>
    [Fact]
    public void ReadEviction_ReservedReasonDecodesAsUnknown()
    {
        var header = new byte[VsrHeader.HEADER_SIZE];
        header[VsrHeader.EVICTION_REASON_OFFSET] = (byte)EvictionReason.Reserved;

        Assert.Null(VsrHeader.ReadEviction(header).Reason);
        Assert.Equal(VsrError.INVALID_COMMAND,
            Assert.IsType<IggyInvalidStatusCodeException>(VsrReplyDecoder.ToException(VsrHeader.ReadEviction(header)))
                .StatusCode);
    }
}
