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

public sealed class VsrNamespaceTests
{
    [Fact]
    public void Pack_PlacesEachFieldAtItsShift()
    {
        Assert.Equal((2UL << 32) | (3UL << 20) | 4UL, VsrNamespace.Pack(2, 3, 4));
    }

    [Fact]
    public void MetadataSentinel_SitsAboveThePackedRange()
    {
        var packedMax = VsrNamespace.Pack(VsrNamespace.MAX_STREAMS - 1, VsrNamespace.MAX_TOPICS - 1,
            VsrNamespace.MAX_PARTITIONS - 1);

        Assert.True(VsrNamespace.METADATA_CONSENSUS_NAMESPACE > packedMax);
    }

    [Fact]
    public void ForPartition_PacksNumericIdentifiers()
    {
        Assert.Equal(VsrNamespace.Pack(2, 3, 4),
            VsrNamespace.ForPartition(Identifier.Numeric(2), Identifier.Numeric(3), 4));
    }

    [Fact]
    public void ForPartition_NamedIdentifierResolvesToZero()
    {
        Assert.Equal(0UL, VsrNamespace.ForPartition(Identifier.String("orders"), Identifier.Numeric(3), 4));
        Assert.Equal(0UL, VsrNamespace.ForPartition(Identifier.Numeric(2), Identifier.String("events"), 4));
    }

    [Theory]
    [InlineData((uint)VsrNamespace.MAX_STREAMS, 3u, 4u)]
    [InlineData(2u, (uint)VsrNamespace.MAX_TOPICS, 4u)]
    [InlineData(2u, 3u, (uint)VsrNamespace.MAX_PARTITIONS)]
    public void ForPartition_RejectsIdentifiersOutsideThePackableRange(uint streamId, uint topicId, uint partitionId)
    {
        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrNamespace.ForPartition(Identifier.Numeric(streamId), Identifier.Numeric(topicId), partitionId));

        Assert.Equal(VsrError.INVALID_IDENTIFIER, exception.StatusCode);
    }

    [Theory]
    [InlineData((byte)VsrOperation.Register)]
    [InlineData((byte)VsrOperation.Logout)]
    public void ForOperation_ControlPlaneOpsTargetTheMetadataReplica(byte operation)
    {
        Assert.Equal(VsrNamespace.METADATA_CONSENSUS_NAMESPACE,
            VsrNamespace.ForOperation((VsrOperation)operation, VsrNamespace.Pack(1, 2, 3)));
    }

    [Fact]
    public void ForOperation_MetadataAndNonReplicatedOpsIgnoreThePartitionNamespace()
    {
        Assert.Equal(0UL, VsrNamespace.ForOperation(VsrOperation.CreateStream, VsrNamespace.Pack(1, 2, 3)));
        Assert.Equal(0UL, VsrNamespace.ForOperation(VsrOperation.NonReplicated, VsrNamespace.Pack(1, 2, 3)));
    }

    [Fact]
    public void ForOperation_PartitionOpsCarryTheCallerNamespace()
    {
        Assert.Equal(VsrNamespace.Pack(1, 2, 3),
            VsrNamespace.ForOperation(VsrOperation.SendMessages, VsrNamespace.Pack(1, 2, 3)));
        Assert.Equal(VsrNamespace.Pack(1, 2, 3),
            VsrNamespace.ForOperation(VsrOperation.StoreConsumerOffset, VsrNamespace.Pack(1, 2, 3)));
    }

    [Theory]
    [InlineData((byte)VsrOperation.Register)]
    [InlineData((byte)VsrOperation.Logout)]
    public void ForRequest_ControlPlaneOpsTargetTheMetadataReplica(byte operation)
    {
        Assert.Equal(VsrNamespace.METADATA_CONSENSUS_NAMESPACE,
            VsrNamespace.ForRequest(CommandCodes.LOGOUT_USER_CODE, [], (VsrOperation)operation));
    }

    [Fact]
    public void ForRequest_MetadataAndNonReplicatedOpsUseZero()
    {
        Assert.Equal(0UL, VsrNamespace.ForRequest(CommandCodes.CREATE_STREAM_CODE, [], VsrOperation.CreateStream));
        Assert.Equal(0UL, VsrNamespace.ForRequest(CommandCodes.PING_CODE, [], VsrOperation.NonReplicated));
    }

    [Fact]
    public void ForRequest_SendMessagesPacksThePartitionId()
    {
        var payload = VsrTestPayloads.SendMessagesToPartition(1, 2, 3);

        Assert.Equal(VsrNamespace.Pack(1, 2, 3),
            VsrNamespace.ForRequest(CommandCodes.SEND_MESSAGES_CODE, payload, VsrOperation.SendMessages));
    }

    [Fact]
    public void ForRequest_SendMessagesIgnoresTheMessageBatchAfterTheMetadata()
    {
        var payload = VsrTestPayloads.Concat(VsrTestPayloads.SendMessagesToPartition(1, 2, 3), [0xFF, 0xFF, 0xFF]);

        Assert.Equal(VsrNamespace.Pack(1, 2, 3),
            VsrNamespace.ForRequest(CommandCodes.SEND_MESSAGES_CODE, payload, VsrOperation.SendMessages));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public void ForRequest_SendMessagesRejectsServerSidePartitioning(byte kind)
    {
        var payload = VsrTestPayloads.SendMessages(VsrTestPayloads.NumericIdentifier(1),
            VsrTestPayloads.NumericIdentifier(2), kind, kind == 1 ? [] : "key"u8.ToArray());

        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrNamespace.ForRequest(CommandCodes.SEND_MESSAGES_CODE, payload, VsrOperation.SendMessages));

        Assert.Equal(VsrError.FEATURE_UNAVAILABLE, exception.StatusCode);
    }

    [Fact]
    public void ForRequest_NamedIdentifiersResolveToZero()
    {
        var payload = VsrTestPayloads.SendMessages(VsrTestPayloads.NamedIdentifier("stream"),
            VsrTestPayloads.NumericIdentifier(2), 2, VsrTestPayloads.UInt32(3));

        Assert.Equal(0UL,
            VsrNamespace.ForRequest(CommandCodes.SEND_MESSAGES_CODE, payload, VsrOperation.SendMessages));
    }

    [Theory]
    [InlineData(1, 3)]
    [InlineData(2, 0)]
    [InlineData(7, 4)]
    public void ForRequest_MalformedIdentifierIsInvalidCommandNotANamedIdentifier(byte kind, byte length)
    {
        var streamId = VsrTestPayloads.Concat([kind, length], new byte[length]);
        var payload = VsrTestPayloads.SendMessages(streamId, VsrTestPayloads.NumericIdentifier(2), 2,
            VsrTestPayloads.UInt32(3));

        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrNamespace.ForRequest(CommandCodes.SEND_MESSAGES_CODE, payload, VsrOperation.SendMessages));

        Assert.Equal(VsrError.INVALID_COMMAND, exception.StatusCode);
    }

    [Fact]
    public void ForRequest_UnknownConsumerKindIsInvalidCommand()
    {
        var payload = VsrTestPayloads.ConsumerOffset(VsrTestPayloads.NumericIdentifier(4),
            VsrTestPayloads.NumericIdentifier(5), 6);
        payload[0] = 7;

        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrNamespace.ForRequest(CommandCodes.STORE_CONSUMER_OFFSET_CODE, payload,
                VsrOperation.StoreConsumerOffset));

        Assert.Equal(VsrError.INVALID_COMMAND, exception.StatusCode);
    }

    [Theory]
    [InlineData(CommandCodes.STORE_CONSUMER_OFFSET_CODE, (byte)VsrOperation.StoreConsumerOffset)]
    [InlineData(CommandCodes.DELETE_CONSUMER_OFFSET_CODE, (byte)VsrOperation.DeleteConsumerOffset)]
    [InlineData(CommandCodes.STORE_CONSUMER_OFFSET_2_CODE, (byte)VsrOperation.StoreConsumerOffset2)]
    [InlineData(CommandCodes.DELETE_CONSUMER_OFFSET_2_CODE, (byte)VsrOperation.DeleteConsumerOffset2)]
    public void ForRequest_ConsumerOffsetOpsPackTheirPartition(int code, byte operation)
    {
        var payload = VsrTestPayloads.ConsumerOffset(VsrTestPayloads.NumericIdentifier(4),
            VsrTestPayloads.NumericIdentifier(5), 6);

        Assert.Equal(VsrNamespace.Pack(4, 5, 6), VsrNamespace.ForRequest(code, payload, (VsrOperation)operation));
    }

    [Fact]
    public void ForRequest_ConsumerOffsetWithoutAPartitionIsRejected()
    {
        var payload = VsrTestPayloads.ConsumerOffset(VsrTestPayloads.NumericIdentifier(4),
            VsrTestPayloads.NumericIdentifier(5), null);

        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrNamespace.ForRequest(CommandCodes.STORE_CONSUMER_OFFSET_CODE, payload,
                VsrOperation.StoreConsumerOffset));

        Assert.Equal(VsrError.INVALID_IDENTIFIER, exception.StatusCode);
    }

    /// <summary>
    ///     The partition id is missing, not truncated, so the caller learns the request cannot be routed rather
    ///     than that its payload is malformed - the length guard must not pre-empt that.
    /// </summary>
    [Fact]
    public void ForRequest_ConsumerOffsetWithoutAPartitionIsRejectedEvenWhenTheBodyEndsThere()
    {
        var full = VsrTestPayloads.ConsumerOffset(VsrTestPayloads.NumericIdentifier(4),
            VsrTestPayloads.NumericIdentifier(5), null);

        // Drop the four padding bytes that follow the absent-partition flag.
        var payload = full.AsSpan(0, full.Length - 4).ToArray();

        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrNamespace.ForRequest(CommandCodes.STORE_CONSUMER_OFFSET_CODE, payload,
                VsrOperation.StoreConsumerOffset));

        Assert.Equal(VsrError.INVALID_IDENTIFIER, exception.StatusCode);
    }

    /// <summary>
    ///     A metadata length near int.MaxValue overflows when four is added to it, so the bounds check has to be
    ///     written as a subtraction or it passes and the slice throws an untyped exception instead.
    /// </summary>
    [Fact]
    public void ForRequest_SendMessagesMetadataLengthNearIntMaxIsInvalidCommand()
    {
        var payload = new byte[64];
        BinaryPrimitives.WriteUInt32LittleEndian(payload, (uint)int.MaxValue - 2);

        var error = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrNamespace.ForRequest(CommandCodes.SEND_MESSAGES_CODE, payload, VsrOperation.SendMessages));

        Assert.Equal(VsrError.INVALID_COMMAND, error.StatusCode);
    }

    [Fact]
    public void ForRequest_DeleteSegmentsPacksThePartition()
    {
        var payload = VsrTestPayloads.DeleteSegments(VsrTestPayloads.NumericIdentifier(7),
            VsrTestPayloads.NumericIdentifier(8), 9);

        Assert.Equal(VsrNamespace.Pack(7, 8, 9),
            VsrNamespace.ForRequest(CommandCodes.DELETE_SEGMENTS_CODE, payload, VsrOperation.DeleteSegments));
    }

    [Theory]
    [InlineData(VsrNamespace.MAX_STREAMS, 1, 1)]
    [InlineData(1, VsrNamespace.MAX_TOPICS, 1)]
    [InlineData(1, 1, VsrNamespace.MAX_PARTITIONS)]
    public void ForRequest_RejectsIdentifiersOutsideThePackableRange(uint streamId, uint topicId, uint partitionId)
    {
        var payload = VsrTestPayloads.SendMessagesToPartition(streamId, topicId, partitionId);

        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrNamespace.ForRequest(CommandCodes.SEND_MESSAGES_CODE, payload, VsrOperation.SendMessages));

        Assert.Equal(VsrError.INVALID_IDENTIFIER, exception.StatusCode);
    }

    /// <summary>A partition id is four bytes; anything else means the caller framed a different value.</summary>
    [Theory]
    [InlineData(2)]
    [InlineData(8)]
    public void ForRequest_SendMessagesRejectsAPartitionIdOfTheWrongWidth(byte length)
    {
        var payload = VsrTestPayloads.SendMessages(VsrTestPayloads.NumericIdentifier(1),
            VsrTestPayloads.NumericIdentifier(2), 2, new byte[length]);

        var error = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrNamespace.ForRequest(CommandCodes.SEND_MESSAGES_CODE, payload, VsrOperation.SendMessages));
        Assert.Equal(VsrError.INVALID_COMMAND, error.StatusCode);
    }

    [Fact]
    public void ForRequest_TruncatedSendMessagesPayloadIsInvalidCommand()
    {
        var payload = VsrTestPayloads.SendMessagesToPartition(1, 2, 3);
        for (var length = 0; length < payload.Length; length++)
        {
            var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
                VsrNamespace.ForRequest(CommandCodes.SEND_MESSAGES_CODE, payload.AsSpan(0, length).ToArray(),
                    VsrOperation.SendMessages));

            Assert.Equal(VsrError.INVALID_COMMAND, exception.StatusCode);
        }
    }

    [Fact]
    public void ForRequest_TruncatedConsumerOffsetPayloadIsInvalidCommand()
    {
        var payload = VsrTestPayloads.ConsumerOffset(VsrTestPayloads.NumericIdentifier(4),
            VsrTestPayloads.NumericIdentifier(5), 6);
        for (var length = 0; length < payload.Length; length++)
        {
            var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
                VsrNamespace.ForRequest(CommandCodes.STORE_CONSUMER_OFFSET_CODE, payload.AsSpan(0, length).ToArray(),
                    VsrOperation.StoreConsumerOffset));

            Assert.Equal(VsrError.INVALID_COMMAND, exception.StatusCode);
        }
    }

    [Fact]
    public void ForRequest_TruncatedDeleteSegmentsPayloadIsInvalidCommand()
    {
        var payload = VsrTestPayloads.DeleteSegments(VsrTestPayloads.NumericIdentifier(7),
            VsrTestPayloads.NumericIdentifier(8), 9);
        for (var length = 0; length < payload.Length - 4; length++)
        {
            var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
                VsrNamespace.ForRequest(CommandCodes.DELETE_SEGMENTS_CODE, payload.AsSpan(0, length).ToArray(),
                    VsrOperation.DeleteSegments));

            Assert.Equal(VsrError.INVALID_COMMAND, exception.StatusCode);
        }
    }

    [Fact]
    public void ForRequest_UnroutablePartitionCodeIsFeatureUnavailable()
    {
        var exception = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            VsrNamespace.ForRequest(CommandCodes.POLL_MESSAGES_CODE, [], VsrOperation.SendMessages));

        Assert.Equal(VsrError.FEATURE_UNAVAILABLE, exception.StatusCode);
    }
}
