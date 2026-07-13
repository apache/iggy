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
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Encryption;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Mappers;
using Apache.Iggy.Messages;
using Apache.Iggy.Shared;
using Apache.Iggy.Utils;

namespace Apache.Iggy.Tests.PublisherTests;

public class MessageEncryptionTests
{
    private static AesMessageEncryptor CreateEncryptor()
    {
        return new AesMessageEncryptor(AesMessageEncryptor.GenerateKey());
    }

    private static Dictionary<HeaderKey, HeaderValue> CreateHeaders()
    {
        return new Dictionary<HeaderKey, HeaderValue>
        {
            {
                new HeaderKey
                {
                    Kind = HeaderKind.String,
                    Value = "key"u8.ToArray()
                },
                new HeaderValue
                {
                    Kind = HeaderKind.String,
                    Value = "value"u8.ToArray()
                }
            }
        };
    }

    [Fact]
    public void EncryptCopy_LeavesOriginalUntouched_AndRoundtrips()
    {
        var encryptor = CreateEncryptor();
        var message = new Message(Guid.NewGuid(), "payload"u8.ToArray());

        var copy = HttpMessageStream.EncryptCopy(message, encryptor);

        Assert.Equal("payload"u8.ToArray(), message.Payload.ToArray());
        Assert.Equal("payload"u8.Length, message.Header.PayloadLength);

        Assert.Equal(copy.Payload.Length, copy.Header.PayloadLength);
        Assert.Equal(encryptor.GetMaxEncryptedLength("payload"u8.Length), copy.Payload.Length);
        Assert.Equal("payload"u8.ToArray(), encryptor.DecryptToArray(copy.Payload.Span));
    }

    [Fact]
    public void EncryptCopy_MovesUserHeadersToEncryptedRawForm()
    {
        var encryptor = CreateEncryptor();
        Dictionary<HeaderKey, HeaderValue> headers = CreateHeaders();
        var message = new Message(Guid.NewGuid(), "payload"u8.ToArray(), headers);

        var copy = HttpMessageStream.EncryptCopy(message, encryptor);

        Assert.Same(headers, message.UserHeaders);
        Assert.True(message.RawUserHeaders.IsEmpty);

        Assert.Null(copy.UserHeaders);
        Assert.False(copy.RawUserHeaders.IsEmpty);
        Assert.Equal(copy.RawUserHeaders.Length, copy.Header.UserHeadersLength);

        var plainHeaders = encryptor.DecryptToArray(copy.RawUserHeaders.Span);
        Dictionary<HeaderKey, HeaderValue> roundTripped = BinaryMapper.MapHeaders(plainHeaders);
        Assert.Single(roundTripped);
    }

    [Fact]
    public void Encrypt_ProducesFreshNoncePerCall()
    {
        var encryptor = CreateEncryptor();

        var first = encryptor.EncryptToArray("payload"u8);
        var second = encryptor.EncryptToArray("payload"u8);

        Assert.False(first.AsSpan(0, 12).SequenceEqual(second.AsSpan(0, 12)));
        Assert.False(first.AsSpan().SequenceEqual(second));
        Assert.Equal("payload"u8.ToArray(), encryptor.DecryptToArray(first));
        Assert.Equal("payload"u8.ToArray(), encryptor.DecryptToArray(second));
    }

    [Fact]
    public void Dispose_ReleasesCiphers_DoesNotClobberCallerKey_AndIsIdempotent()
    {
        var key = AesMessageEncryptor.GenerateKey();
        var keySnapshot = (byte[])key.Clone();

        var encryptor = new AesMessageEncryptor(key);
        var ciphertext = encryptor.EncryptToArray("payload"u8);

        encryptor.Dispose();
        encryptor.Dispose();

        Assert.Equal(keySnapshot, key);
        Assert.Throws<ObjectDisposedException>(() => encryptor.EncryptToArray("payload"u8));

        using var reopened = new AesMessageEncryptor(key);
        Assert.Equal("payload"u8.ToArray(), reopened.DecryptToArray(ciphertext));
    }

    [Fact]
    public void CreateMessage_WithEncryptor_EncryptsIntoWireBuffer_AndReportsWrittenSize()
    {
        var encryptor = CreateEncryptor();
        var streamId = Identifier.Numeric(1);
        var topicId = Identifier.Numeric(1);
        var partitioning = Partitioning.None();
        var messages = new[]
        {
            new Message(Guid.NewGuid(), "first-payload"u8.ToArray()),
            new Message(Guid.NewGuid(), "second-payload"u8.ToArray(), CreateHeaders())
        };

        var maxSize = TcpMessageStreamHelpers.CalculateMessageBytesCount(messages, encryptor)
                      + 2 + streamId.Length + 2 + topicId.Length + 2 + partitioning.Length + 4 + 4;
        var buffer = new byte[maxSize];

        var written = TcpContracts.CreateMessage(buffer, streamId, topicId, partitioning, messages, encryptor);

        Assert.Equal(maxSize, written);

        Assert.Equal("first-payload"u8.ToArray(), messages[0].Payload.ToArray());
        Assert.Equal("second-payload"u8.ToArray(), messages[1].Payload.ToArray());
        Assert.NotNull(messages[1].UserHeaders);

        Span<byte> span = buffer.AsSpan(0, written);
        var metadataLength = BinaryPrimitives.ReadInt32LittleEndian(span[..4]);
        var position = 4 + metadataLength + 16 * messages.Length;

        var firstPayloadLength = BinaryPrimitives.ReadInt32LittleEndian(span[(position + 52)..(position + 56)]);
        Assert.Equal(encryptor.GetMaxEncryptedLength("first-payload"u8.Length), firstPayloadLength);
        Assert.Equal("first-payload"u8.ToArray(),
            encryptor.DecryptToArray(span.Slice(position + 64, firstPayloadLength)));

        position += 64 + firstPayloadLength;
        var secondHeadersLength = BinaryPrimitives.ReadInt32LittleEndian(span[(position + 48)..(position + 52)]);
        var secondPayloadLength = BinaryPrimitives.ReadInt32LittleEndian(span[(position + 52)..(position + 56)]);
        Assert.Equal("second-payload"u8.ToArray(),
            encryptor.DecryptToArray(span.Slice(position + 64, secondPayloadLength)));

        var plainHeaders = encryptor.DecryptToArray(
            span.Slice(position + 64 + secondPayloadLength, secondHeadersLength));
        Assert.Single(BinaryMapper.MapHeaders(plainHeaders));
    }
}
