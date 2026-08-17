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
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.VsrTests;

public sealed class VsrReplyDecoderTests
{
    private static byte[] ReplyHeader(VsrOperation operation, int bodyLength, uint status = 0)
    {
        var header = new byte[VsrHeader.HEADER_SIZE];
        header[VsrHeader.COMMAND_OFFSET] = (byte)Command.Reply;
        header[VsrHeader.REPLY_OPERATION_OFFSET] = (byte)operation;
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(VsrHeader.SIZE_OFFSET),
            (uint)(VsrHeader.HEADER_SIZE + bodyLength));
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(VsrHeader.REPLY_STATUS_OFFSET), status);

        return header;
    }

    private static byte[] EvictionHeader(EvictionReason reason, uint version = 0, uint versionMin = 0)
    {
        var header = new byte[VsrHeader.HEADER_SIZE];
        header[VsrHeader.COMMAND_OFFSET] = (byte)Command.Eviction;
        header[VsrHeader.EVICTION_REASON_OFFSET] = (byte)reason;
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(VsrHeader.EVICTION_PROTOCOL_VERSION_OFFSET), version);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(VsrHeader.EVICTION_PROTOCOL_VERSION_MIN_OFFSET),
            versionMin);

        return header;
    }

    private static byte[] SuccessBody(params byte[] payload)
    {
        return VsrTestPayloads.Concat(VsrTestPayloads.UInt32(0), payload);
    }

    private static byte[] RejectionBody(uint code)
    {
        return VsrTestPayloads.Concat(VsrTestPayloads.UInt32(1), VsrTestPayloads.UInt32(0),
            VsrTestPayloads.UInt32((int)code));
    }

    [Fact]
    public void Decode_StripsTheResultSectionFromACommittedMetadataReply()
    {
        var body = SuccessBody(1, 2, 3);

        ReadOnlyMemory<byte> payload
            = VsrReplyDecoder.Decode(ReplyHeader(VsrOperation.CreateStream, body.Length), body);

        Assert.Equal([1, 2, 3], payload.ToArray());
    }

    [Fact]
    public void Decode_CommittedRejectionThrowsTheTypedError()
    {
        var body = RejectionBody(1009);

        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrReplyDecoder.Decode(ReplyHeader(VsrOperation.CreateStream, body.Length), body));

        Assert.Equal(1009, exception.StatusCode);
        Assert.True(exception.FromServer);
    }

    /// <summary>
    ///     Retry and failover key off the transient status codes, and a locally raised code says nothing about
    ///     what the cluster did, so the two origins have to stay distinguishable.
    /// </summary>
    [Fact]
    public void ServerVerdictsAreDistinguishableFromClientSideFailures()
    {
        var header = ReplyHeader(VsrOperation.CreateStream, 0, VsrError.TRANSIENT_NOT_ACCEPTED);

        var fromWire = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrReplyDecoder.Decode(header, ReadOnlyMemory<byte>.Empty));

        Assert.True(fromWire.FromServer);
        Assert.False(VsrError.Exception(VsrError.TRANSIENT_NOT_ACCEPTED, "raised locally").FromServer);
    }

    [Fact]
    public void Decode_TruncatedResultSectionIsInvalidCommandNeverSuccess()
    {
        var body = VsrTestPayloads.UInt32(1);

        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrReplyDecoder.Decode(ReplyHeader(VsrOperation.CreateStream, body.Length), body));

        Assert.Equal(VsrError.INVALID_COMMAND, exception.StatusCode);
    }

    [Fact]
    public void Decode_NonZeroStatusIsReadBeforeTheBody()
    {
        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrReplyDecoder.Decode(ReplyHeader(VsrOperation.CreateStream, 0, 1009), ReadOnlyMemory<byte>.Empty));

        Assert.Equal(1009, exception.StatusCode);
    }

    [Fact]
    public void Decode_NonResultFramedReplyPassesTheBodyThrough()
    {
        byte[] body = [1, 2, 3, 4, 5];

        ReadOnlyMemory<byte> payload
            = VsrReplyDecoder.Decode(ReplyHeader(VsrOperation.NonReplicated, body.Length), body);

        Assert.Equal(body, payload.ToArray());
    }

    [Fact]
    public void Decode_PartitionSendReplyPassesTheBodyThrough()
    {
        byte[] body = [7, 7];

        ReadOnlyMemory<byte> payload
            = VsrReplyDecoder.Decode(ReplyHeader(VsrOperation.SendMessages, body.Length), body);

        Assert.Equal(body, payload.ToArray());
    }

    [Fact]
    public void Decode_ConsumerOffsetReplyIsResultFramed()
    {
        var body = SuccessBody();

        ReadOnlyMemory<byte> payload
            = VsrReplyDecoder.Decode(ReplyHeader(VsrOperation.StoreConsumerOffset, body.Length), body);

        Assert.True(payload.IsEmpty);
    }

    [Fact]
    public void Decode_NonEmptyRegisterReplyIsResultFramed()
    {
        var body = SuccessBody(9, 9);

        ReadOnlyMemory<byte> payload = VsrReplyDecoder.Decode(ReplyHeader(VsrOperation.Register, body.Length), body);

        Assert.Equal([9, 9], payload.ToArray());
    }

    [Fact]
    public void Decode_EmptyRegisterReplyPassesThroughToFailTheTypedDecode()
    {
        ReadOnlyMemory<byte> payload
            = VsrReplyDecoder.Decode(ReplyHeader(VsrOperation.Register, 0), ReadOnlyMemory<byte>.Empty);

        Assert.True(payload.IsEmpty);
    }

    [Fact]
    public void Decode_ShortBodyIsInvalidCommand()
    {
        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrReplyDecoder.Decode(ReplyHeader(VsrOperation.CreateStream, 8), new byte[4]));

        Assert.Equal(VsrError.INVALID_COMMAND, exception.StatusCode);
    }

    [Fact]
    public void Decode_FrameSmallerThanTheHeaderIsInvalidCommand()
    {
        var header = ReplyHeader(VsrOperation.CreateStream, 0);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(VsrHeader.SIZE_OFFSET), 128);

        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrReplyDecoder.Decode(header, ReadOnlyMemory<byte>.Empty));

        Assert.Equal(VsrError.INVALID_COMMAND, exception.StatusCode);
    }

    [Fact]
    public void Decode_UnexpectedFrameIsInvalidCommand()
    {
        var header = new byte[VsrHeader.HEADER_SIZE];
        header[VsrHeader.COMMAND_OFFSET] = 6;

        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrReplyDecoder.Decode(header, ReadOnlyMemory<byte>.Empty));

        Assert.Equal(VsrError.INVALID_COMMAND, exception.StatusCode);
    }

    [Fact]
    public void Decode_ShortHeaderIsEmptyResponse()
    {
        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrReplyDecoder.Decode(new byte[8], ReadOnlyMemory<byte>.Empty));

        Assert.Equal(VsrError.EMPTY_RESPONSE, exception.StatusCode);
    }

    [Fact]
    public void Decode_ReadsAnEvictionFromAMisalignedBuffer()
    {
        var frame = VsrTestPayloads.Concat([0, 0, 0], EvictionHeader(EvictionReason.InvalidCredentials));

        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrReplyDecoder.Decode(frame.AsSpan(3), ReadOnlyMemory<byte>.Empty));

        Assert.Equal(VsrError.INVALID_CREDENTIALS, exception.StatusCode);
    }

    [Theory]
    [InlineData((byte)EvictionReason.InvalidCredentials, VsrError.INVALID_CREDENTIALS)]
    [InlineData((byte)EvictionReason.InvalidToken, VsrError.INVALID_PERSONAL_ACCESS_TOKEN)]
    [InlineData((byte)EvictionReason.UserInactive, VsrError.UNAUTHENTICATED)]
    [InlineData((byte)EvictionReason.SessionError, VsrError.UNAUTHENTICATED)]
    [InlineData((byte)EvictionReason.NoSession, VsrError.UNAUTHENTICATED)]
    [InlineData((byte)EvictionReason.SessionTooLow, VsrError.UNAUTHENTICATED)]
    [InlineData((byte)EvictionReason.SessionReleaseMismatch, VsrError.UNAUTHENTICATED)]
    [InlineData((byte)EvictionReason.StaleClient, VsrError.STALE_CLIENT)]
    [InlineData((byte)EvictionReason.MalformedLogin, VsrError.INVALID_FORMAT)]
    [InlineData((byte)EvictionReason.ClientReleaseTooLow, VsrError.INVALID_COMMAND)]
    [InlineData((byte)EvictionReason.ClientReleaseTooHigh, VsrError.INVALID_COMMAND)]
    [InlineData((byte)EvictionReason.InvalidRequestOperation, VsrError.INVALID_COMMAND)]
    [InlineData((byte)EvictionReason.InvalidRequestBody, VsrError.INVALID_COMMAND)]
    [InlineData((byte)EvictionReason.InvalidRequestBodySize, VsrError.INVALID_COMMAND)]

    // Reserved and every reason this build cannot decode land in the shared grader's catch-all.
    [InlineData((byte)EvictionReason.Reserved, VsrError.INVALID_COMMAND)]
    public void Decode_MapsEachEvictionReasonToItsError(byte reason, int expected)
    {
        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrReplyDecoder.Decode(EvictionHeader((EvictionReason)reason), ReadOnlyMemory<byte>.Empty));

        Assert.Equal(expected, exception.StatusCode);
    }

    [Fact]
    public void Decode_IncompatibleProtocolReportsTheAcceptedWindow()
    {
        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrReplyDecoder.Decode(EvictionHeader(EvictionReason.IncompatibleProtocol, 10243, 10240),
                ReadOnlyMemory<byte>.Empty));

        Assert.Equal(VsrError.INCOMPATIBLE_PROTOCOL_VERSION, exception.StatusCode);
        Assert.Contains("10240..10243", exception.Message);
    }

    [Theory]
    [InlineData(10243u, 0u)]
    [InlineData(10240u, 10243u)]
    public void Decode_IncompatibleProtocolWithAnUnusableWindowDegradesToUnauthenticated(uint version, uint min)
    {
        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrReplyDecoder.Decode(EvictionHeader(EvictionReason.IncompatibleProtocol, version, min),
                ReadOnlyMemory<byte>.Empty));

        Assert.Equal(VsrError.UNAUTHENTICATED, exception.StatusCode);
    }

    [Fact]
    public void ReadResultCode_MalformedSectionIsNullNeverZero()
    {
        Assert.Null(VsrReplyDecoder.ReadResultCode([]));
        Assert.Null(VsrReplyDecoder.ReadResultCode(VsrTestPayloads.UInt32(1)));
        Assert.Null(VsrReplyDecoder.ReadResultCode([1, 0, 0, 0, 0, 0, 0, 0]));
        Assert.Equal(0u, VsrReplyDecoder.ReadResultCode(SuccessBody()));
    }

    [Fact]
    public void ReadResultSectionLength_CoversTheCountAndItsEntries()
    {
        Assert.Equal(4, VsrReplyDecoder.ReadResultSectionLength(SuccessBody(1, 2)));
        Assert.Equal(12, VsrReplyDecoder.ReadResultSectionLength(RejectionBody(1009)));
        Assert.Null(VsrReplyDecoder.ReadResultSectionLength(VsrTestPayloads.UInt32(1)));

        // Shorter than the count itself, so there is no section to measure.
        Assert.Null(VsrReplyDecoder.ReadResultSectionLength([1, 2, 3]));
    }
}
