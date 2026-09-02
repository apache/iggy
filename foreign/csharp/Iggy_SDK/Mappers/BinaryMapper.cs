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

using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Extensions;
using Apache.Iggy.Headers;
using Apache.Iggy.Messages;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Mappers;

internal static class BinaryMapper
{
    private const int CONSUMER_GROUP_HEADER_SIZE = 13;
    private const int MEMBER_HEADER_SIZE = 8;
    private const int CONSUMER_GROUP_INFO_SIZE = 12;
    private const int CACHE_METRICS_ENTRY_SIZE = 32;
    private const int MIN_TOPIC_SIZE = 50 + 4 + 4;
    private const int MIN_OPTION_SPEC_SIZE = 1 + 1 + 4 + 4;
    private const int CLUSTER_NODE_TAIL_SIZE = 4 * 2 + 1 + 1;
    private const int MIN_CLUSTER_NODE_SIZE = 4 + 4 + CLUSTER_NODE_TAIL_SIZE;

    internal static RawPersonalAccessToken MapRawPersonalAccessToken(ReadOnlySpan<byte> payload)
    {
        var tokenLength = payload[0];
        var token = Encoding.UTF8.GetString(payload[1..(1 + tokenLength)]);
        return new RawPersonalAccessToken { Token = token };
    }

    internal static IReadOnlyList<PersonalAccessTokenResponse> MapPersonalAccessTokens(ReadOnlySpan<byte> payload)
    {
        if (payload.Length == 0)
        {
            return Array.Empty<PersonalAccessTokenResponse>();
        }

        var result = new List<PersonalAccessTokenResponse>();
        var length = payload.Length;
        var position = 0;
        while (position < length)
        {
            var (response, readBytes) = MapToPersonalAccessTokenResponse(payload, position);
            result.Add(response);
            position += readBytes;
        }

        return result.AsReadOnly();
    }

    private static (PersonalAccessTokenResponse response, int position) MapToPersonalAccessTokenResponse(
        ReadOnlySpan<byte> payload, int position)
    {
        var nameLength = (int)payload[position];
        var name = Encoding.UTF8.GetString(payload[(position + 1)..(1 + position + nameLength)]);
        var expiry = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 1 + nameLength)..]);
        var readBytes = 1 + nameLength + 8;
        return (
            new PersonalAccessTokenResponse
            {
                Name = name,
                ExpiryAt = expiry == 0 ? null : DateTimeOffsetUtils.FromUnixTimeMicroSeconds(expiry).LocalDateTime
            }, readBytes);
    }

    internal static IReadOnlyList<UserResponse> MapUsers(ReadOnlySpan<byte> payload)
    {
        if (payload.Length == 0)
        {
            return Array.Empty<UserResponse>();
        }

        var result = new List<UserResponse>();
        var length = payload.Length;
        var position = 0;
        while (position < length)
        {
            var (response, readBytes) = MapToUserResponse(payload, position);
            result.Add(response);
            position += readBytes;
        }

        return result.AsReadOnly();
    }

    internal static UserResponse MapUser(ReadOnlySpan<byte> payload)
    {
        var (response, position) = MapToUserResponse(payload, 0);
        var hasPermissions = payload[position];
        if (hasPermissions == 1)
        {
            var permissionLength = ReadLength(payload, position + 1, "User permissions");
            ReadOnlySpan<byte> permissionsPayload = payload[(position + 5)..(position + 5 + permissionLength)];
            var permissions = MapPermissions(permissionsPayload);
            return new UserResponse
            {
                Permissions = permissions,
                Id = response.Id,
                CreatedAt = response.CreatedAt,
                Username = response.Username,
                Status = response.Status,
                Options = response.Options
            };
        }

        return new UserResponse
        {
            Id = response.Id,
            CreatedAt = response.CreatedAt,
            Username = response.Username,
            Status = response.Status,
            Permissions = null,
            Options = response.Options
        };
    }

    private static Permissions MapPermissions(ReadOnlySpan<byte> bytes)
    {
        var streamMap = new Dictionary<uint, StreamPermissions>();
        var index = 0;

        var globalPermissions = new GlobalPermissions
        {
            ManageServers = bytes[index++] == 1,
            ReadServers = bytes[index++] == 1,
            ManageUsers = bytes[index++] == 1,
            ReadUsers = bytes[index++] == 1,
            ManageStreams = bytes[index++] == 1,
            ReadStreams = bytes[index++] == 1,
            ManageTopics = bytes[index++] == 1,
            ReadTopics = bytes[index++] == 1,
            PollMessages = bytes[index++] == 1,
            SendMessages = bytes[index++] == 1
        };

        if (bytes[index++] == 1)
        {
            while (true)
            {
                var streamId = BinaryPrimitives.ReadUInt32LittleEndian(bytes[index..(index + 4)]);
                index += sizeof(uint);

                var manageStream = bytes[index++] == 1;
                var readStream = bytes[index++] == 1;
                var manageTopics = bytes[index++] == 1;
                var readTopics = bytes[index++] == 1;
                var pollMessagesStream = bytes[index++] == 1;
                var sendMessagesStream = bytes[index++] == 1;
                var topicsMap = new Dictionary<uint, TopicPermissions>();

                if (bytes[index++] == 1)
                {
                    while (true)
                    {
                        var topicId = BinaryPrimitives.ReadUInt32LittleEndian(bytes[index..(index + 4)]);
                        index += sizeof(uint);

                        var manageTopic = bytes[index++] == 1;
                        var readTopic = bytes[index++] == 1;
                        var pollMessagesTopic = bytes[index++] == 1;
                        var sendMessagesTopic = bytes[index++] == 1;

                        topicsMap.Add(topicId,
                            new TopicPermissions
                            {
                                ManageTopic = manageTopic,
                                ReadTopic = readTopic,
                                PollMessages = pollMessagesTopic,
                                SendMessages = sendMessagesTopic
                            });

                        if (bytes[index++] == 0)
                        {
                            break;
                        }
                    }
                }

                streamMap.Add(streamId,
                    new StreamPermissions
                    {
                        ManageStream = manageStream,
                        ReadStream = readStream,
                        ManageTopics = manageTopics,
                        ReadTopics = readTopics,
                        PollMessages = pollMessagesStream,
                        SendMessages = sendMessagesStream,
                        Topics = topicsMap.Count > 0 ? topicsMap : null
                    });

                if (bytes[index++] == 0)
                {
                    break;
                }
            }
        }

        return new Permissions
        {
            Global = globalPermissions,
            Streams = streamMap.Count > 0 ? streamMap : null
        };
    }

    private static (UserResponse response, int position) MapToUserResponse(ReadOnlySpan<byte> payload, int position)
    {
        var id = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        var createdAt = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 4)..(position + 12)]);
        var status = payload[position + 12];
        var userStatus = status switch
        {
            1 => UserStatus.Active,
            2 => UserStatus.Inactive,
            _ => throw new ArgumentOutOfRangeException()
        };
        var usernameLength = payload[position + 13];
        var username = Encoding.UTF8.GetString(payload[(position + 14)..(position + 14 + usernameLength)]);
        var readBytes = 4 + 8 + 1 + 1 + usernameLength;
        var options = MapOptions(payload, position + readBytes, out var optionsReadBytes);
        readBytes += optionsReadBytes;

        return (new UserResponse
        {
            Id = id,
            CreatedAt = createdAt,
            Status = userStatus,
            Username = username,
            Options = options
        },
            readBytes);
    }

    internal static ClientResponse MapClient(ReadOnlySpan<byte> payload)
    {
        var (response, position) = MapClientInfo(payload, 0);
        var consumerGroups = new List<ConsumerGroupInfo>(ValidatedCollectionSize(response.ConsumerGroupsCount,
            payload.Length - position, CONSUMER_GROUP_INFO_SIZE, "Client consumer groups count"));

        for (var i = 0; i < response.ConsumerGroupsCount; i++)
        {
            var streamId = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
            var topicId = BinaryPrimitives.ReadUInt32LittleEndian(payload[(position + 4)..(position + 8)]);
            var consumerGroupId = BinaryPrimitives.ReadUInt32LittleEndian(payload[(position + 8)..(position + 12)]);
            var consumerGroup
                = new ConsumerGroupInfo
                {
                    StreamId = streamId,
                    TopicId = topicId,
                    GroupId = consumerGroupId
                };
            consumerGroups.Add(consumerGroup);
            position += 12;
        }

        return new ClientResponse
        {
            Address = response.Address,
            ClientId = response.ClientId,
            UserId = response.UserId,
            Transport = response.Transport,
            ConsumerGroupsCount = response.ConsumerGroupsCount,
            ConsumerGroups = consumerGroups
        };
    }

    internal static IReadOnlyList<ClientResponse> MapClients(ReadOnlySpan<byte> payload)
    {
        if (payload.Length == 0)
        {
            return [];
        }

        var response = new List<ClientResponse>();
        var length = payload.Length;
        var position = 0;
        while (position < length)
        {
            var (client, readBytes) = MapClientInfo(payload, position);

            response.Add(client);
            position += readBytes;
        }

        return response;
    }

    private static (ClientResponse response, int readBytes) MapClientInfo(ReadOnlySpan<byte> payload, int position)
    {
        var start = position;
        var id = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        var userId = BinaryPrimitives.ReadUInt32LittleEndian(payload[(position + 4)..(position + 8)]);
        var transport = payload[position + 8] switch
        {
            1 => ClientTransport.Tcp,
            2 => ClientTransport.Quic,
            3 => ClientTransport.Http,
            4 => ClientTransport.WebSocket,
            _ => ClientTransport.Unknown
        };
        position += 9;
        var address = ReadString(payload, ref position, "Client address");
        var consumerGroupsCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        position += 4;

        return (new ClientResponse
        {
            ClientId = id,
            UserId = userId == uint.MaxValue ? null : userId,
            Transport = transport,
            Address = address,
            ConsumerGroupsCount = consumerGroupsCount
        }, position - start);
    }

    internal static OffsetResponse MapOffsets(ReadOnlySpan<byte> payload)
    {
        var partitionId = BinaryPrimitives.ReadUInt32LittleEndian(payload[..4]);
        var currentOffset = BinaryPrimitives.ReadUInt64LittleEndian(payload[4..12]);
        var offset = BinaryPrimitives.ReadUInt64LittleEndian(payload[12..20]);

        return new OffsetResponse
        {
            CurrentOffset = currentOffset,
            StoredOffset = offset,
            PartitionId = partitionId
        };
    }

    internal static PolledMessagesRental MapRentedMessages(ReadOnlyMemory<byte> payload,
        IMemoryOwner<byte> payloadOwner, IMessageEncryptor? encryptor = null)
    {
        ReadOnlySpan<byte> span = payload.Span;
        var length = payload.Length;
        var partitionId = BinaryPrimitives.ReadUInt32LittleEndian(span[..4]);
        var currentOffset = BinaryPrimitives.ReadUInt64LittleEndian(span[4..12]);
        var messagesCount = BinaryPrimitives.ReadUInt32LittleEndian(span[12..16]);
        var position = 16;
        if (position >= length)
        {
            return new PolledMessagesRental(payloadOwner)
            {
                PartitionId = partitionId,
                CurrentOffset = currentOffset,
                Messages = []
            };
        }

        ArrayPoolHelper.SlicedMemoryOwner? plaintextOwner = null;
        try
        {
            // One plaintext buffer rented up front (sized by a header-only pre-pass) keeps the rented path
            // allocation-free per message. Tied to the rental's disposal so decrypted slices stay valid.
            var plaintext = Memory<byte>.Empty;
            var plainCursor = 0;
            if (encryptor is not null)
            {
                var total = SumMaxDecryptedLength(span, length, encryptor);
                if (total > 0)
                {
                    plaintextOwner = ArrayPoolHelper.Rent(total, true);
                    plaintext = plaintextOwner.Memory;
                }
            }

            var maxMessages = (length - 16) / BatchWireFormat.FRAME_HEADER_SIZE;
            var capacity = (int)Math.Min(messagesCount, (uint)maxMessages);
            List<RentedMessageResponse> messages = new(capacity);

            while (position < length)
            {
                var batchEnd = ReadBatchExtent(span, length, position, out var baseOffset, out var baseTimestamp,
                    out var batchOriginTimestamp);
                // Broker append time is stamped once per batch record; the per-frame delta applies to the
                // origin timestamp only.
                var timestamp = DateTimeOffsetUtils.FromUnixTimeMicroSeconds(baseTimestamp);
                var cursor = position + BatchWireFormat.BATCH_HEADER_SIZE;
                while (cursor < batchEnd)
                {
                    ReadFrameLengths(span, cursor, batchEnd, out var headersLength, out var payloadLength);

                    var checksum = BinaryPrimitives.ReadUInt64LittleEndian(span[cursor..(cursor + 8)]);
                    var id = BinaryPrimitives.ReadUInt128LittleEndian(span[(cursor + 8)..(cursor + 24)]);
                    var offsetDelta = BinaryPrimitives.ReadUInt32LittleEndian(span[(cursor + 24)..(cursor + 28)]);
                    var timestampDelta = BinaryPrimitives.ReadUInt32LittleEndian(span[(cursor + 28)..(cursor + 32)]);
                    var offset = baseOffset + offsetDelta;

                    var payloadRangeStart = cursor + BatchWireFormat.FRAME_HEADER_SIZE;
                    var headersRangeStart = payloadRangeStart + payloadLength;

                    ReadOnlyMemory<byte> payloadSlice = payload.Slice(payloadRangeStart, payloadLength);
                    ReadOnlyMemory<byte> rawHeaders = headersLength > 0
                        ? payload.Slice(headersRangeStart, headersLength)
                        : ReadOnlyMemory<byte>.Empty;

                    // Decrypt into the shared buffer so the message looks like plaintext downstream. Wire lengths
                    // still drive the cursor advance; only the decrypted lengths land on the header.
                    var storedPayloadLength = payloadLength;
                    var storedHeadersLength = headersLength;
                    if (encryptor is not null)
                    {
                        try
                        {
                            // Bound each destination to this message's reserved slice so an encryptor that overruns
                            // its contract fails fast here instead of corrupting the next message's region.
                            Memory<byte> payloadDest =
                                plaintext.Slice(plainCursor, encryptor.GetMaxDecryptedLength(payloadLength));
                            var writtenPayload = encryptor.Decrypt(payloadSlice.Span, payloadDest.Span);
                            payloadSlice = payloadDest.Slice(0, writtenPayload);
                            storedPayloadLength = writtenPayload;
                            plainCursor += writtenPayload;

                            if (!rawHeaders.IsEmpty)
                            {
                                Memory<byte> headersDest =
                                    plaintext.Slice(plainCursor, encryptor.GetMaxDecryptedLength(headersLength));
                                var writtenHeaders = encryptor.Decrypt(rawHeaders.Span, headersDest.Span);
                                rawHeaders = headersDest.Slice(0, writtenHeaders);
                                storedHeadersLength = writtenHeaders;
                                plainCursor += writtenHeaders;
                            }
                        }
                        catch (Exception ex)
                        {
                            throw new MessageDecryptionException(offset, partitionId, ex);
                        }
                    }

                    messages.Add(new RentedMessageResponse
                    {
                        Header = new MessageHeader
                        {
                            Checksum = checksum,
                            Id = id,
                            Offset = offset,
                            OriginTimestamp = batchOriginTimestamp + timestampDelta,
                            PayloadLength = storedPayloadLength,
                            Timestamp = timestamp,
                            UserHeadersLength = storedHeadersLength,
                            Reserved = 0
                        },
                        RawUserHeaders = rawHeaders,
                        Payload = payloadSlice
                    });

                    cursor = headersRangeStart + headersLength;
                }

                position = batchEnd;
            }

            return new PolledMessagesRental(payloadOwner, plaintextOwner)
            {
                PartitionId = partitionId,
                CurrentOffset = currentOffset,
                Messages = messages
            };
        }
        catch
        {
            plaintextOwner?.Dispose();
            throw;
        }
    }

    // Shared by the decrypt sizing pre-pass and the main map loop so both agree on which frames are included;
    // drift would mis-size the shared plaintext buffer.
    private static int ReadBatchExtent(ReadOnlySpan<byte> span, int length, int position, out ulong baseOffset,
        out ulong baseTimestamp, out ulong originTimestamp)
    {
        if (position + BatchWireFormat.BATCH_HEADER_SIZE > length)
        {
            throw new MalformedResponseException(
                $"Malformed batch record at byte {position}: {length - position} bytes cannot hold a batch header.");
        }

        baseOffset = BinaryPrimitives.ReadUInt64LittleEndian(span[(position + 8)..(position + 16)]);
        baseTimestamp = BinaryPrimitives.ReadUInt64LittleEndian(span[(position + 16)..(position + 24)]);
        originTimestamp = BinaryPrimitives.ReadUInt64LittleEndian(span[(position + 24)..(position + 32)]);
        var batchLength = BinaryPrimitives.ReadUInt64LittleEndian(span[(position + 32)..(position + 40)]);
        if (batchLength < BatchWireFormat.BATCH_HEADER_SIZE || (ulong)position + batchLength > (ulong)length)
        {
            throw new MalformedResponseException(
                $"Malformed batch record at byte {position}: batch length {batchLength} does not fit the response.");
        }

        return position + (int)batchLength;
    }

    private static void ReadFrameLengths(ReadOnlySpan<byte> span, int cursor, int batchEnd,
        out int headersLength, out int payloadLength)
    {
        if (cursor + BatchWireFormat.FRAME_HEADER_SIZE > batchEnd)
        {
            throw new MalformedResponseException(
                $"Malformed message frame at byte {cursor}: {batchEnd - cursor} bytes cannot hold a frame header.");
        }

        headersLength = BinaryPrimitives.ReadInt32LittleEndian(span[(cursor + 32)..(cursor + 36)]);
        payloadLength = BinaryPrimitives.ReadInt32LittleEndian(span[(cursor + 36)..(cursor + 40)]);
        if (headersLength < 0 || payloadLength < 0)
        {
            throw new MalformedResponseException(
                $"Malformed message frame at byte {cursor}: negative payload ({payloadLength}) or header " +
                $"({headersLength}) length.");
        }

        if (BinaryPrimitives.ReadUInt64LittleEndian(span[(cursor + 40)..(cursor + 48)]) != 0)
        {
            throw new MalformedResponseException(
                $"Malformed message frame at byte {cursor}: reserved bytes must be zero.");
        }

        // Overflow-safe: server-controlled lengths can approach int.MaxValue, so compute the bound in long.
        if ((long)cursor + BatchWireFormat.FRAME_HEADER_SIZE + payloadLength + headersLength > batchEnd)
        {
            throw new MalformedResponseException(
                $"Malformed message frame at byte {cursor}: frame runs past its batch record.");
        }
    }

    // Pre-pass summing upper-bound plaintext length so the shared buffer is rented exactly once. Same batch
    // and frame walk as the main loop, so both agree on which messages are included.
    private static int SumMaxDecryptedLength(ReadOnlySpan<byte> span, int length, IMessageEncryptor encryptor)
    {
        var position = 16;
        var total = 0;
        while (position < length)
        {
            var batchEnd = ReadBatchExtent(span, length, position, out _, out _, out _);
            var cursor = position + BatchWireFormat.BATCH_HEADER_SIZE;
            while (cursor < batchEnd)
            {
                ReadFrameLengths(span, cursor, batchEnd, out var headersLength, out var payloadLength);
                total += encryptor.GetMaxDecryptedLength(payloadLength);
                if (headersLength > 0)
                {
                    total += encryptor.GetMaxDecryptedLength(headersLength);
                }

                cursor += BatchWireFormat.FRAME_HEADER_SIZE + payloadLength + headersLength;
            }

            position = batchEnd;
        }

        return total;
    }

    internal static PolledMessages MaterializeMessages(PolledMessagesRental rental)
    {
        var messages = new List<MessageResponse>(rental.Messages.Count);
        foreach (var message in rental.Messages)
        {
            messages.Add(new MessageResponse
            {
                Header = message.Header,
                RawUserHeaders = message.RawUserHeaders.IsEmpty ? null : message.RawUserHeaders.ToArray(),
                Payload = message.Payload.ToArray()
            });
        }

        return new PolledMessages
        {
            PartitionId = rental.PartitionId,
            CurrentOffset = rental.CurrentOffset,
            Messages = messages
        };
    }

    internal static PolledMessagesRental ToRentedMessages(PolledMessages messages)
    {
        var rentedMessages = new List<RentedMessageResponse>(messages.Messages.Count);
        foreach (var message in messages.Messages)
        {
            rentedMessages.Add(new RentedMessageResponse
            {
                Header = message.Header,
                Payload = message.Payload,
                RawUserHeaders = message.RawUserHeaders ?? ReadOnlyMemory<byte>.Empty,
                UserHeaders = message.UserHeaders
            });
        }

        return new PolledMessagesRental(EmptyMemoryOwner.Instance)
        {
            PartitionId = messages.PartitionId,
            CurrentOffset = messages.CurrentOffset,
            Messages = rentedMessages
        };
    }

    internal static Dictionary<HeaderKey, HeaderValue> MapHeaders(ReadOnlySpan<byte> payload)
    {
        var headers = new Dictionary<HeaderKey, HeaderValue>();
        var position = 0;

        while (position < payload.Length)
        {
            var keyKind = ReadHeaderKind(payload, ref position, "key");
            var keyValue = ReadHeaderField(payload, ref position, keyKind, "key");
            var valueKind = ReadHeaderKind(payload, ref position, "value");
            var value = ReadHeaderField(payload, ref position, valueKind, "value");

            headers[new HeaderKey
            {
                Kind = keyKind,
                Value = keyValue
            }] =
                new HeaderValue
                {
                    Kind = valueKind,
                    Value = value
                };
        }

        return headers;
    }

    private static HeaderKind ReadHeaderKind(ReadOnlySpan<byte> payload, ref int position, string field)
    {
        if (position >= payload.Length)
        {
            throw new MalformedResponseException($"Header {field} kind at byte {position} is missing.");
        }

        if (!TryMapHeaderKind(payload[position], out var kind))
        {
            throw new MalformedResponseException(
                $"Header {field} kind {payload[position]} at byte {position} is unknown.");
        }

        position++;
        return kind;
    }

    private static byte[] ReadHeaderField(ReadOnlySpan<byte> payload, ref int position, HeaderKind kind,
        string field)
    {
        if (position + 4 > payload.Length)
        {
            throw new MalformedResponseException($"Header {field} length at byte {position} is truncated.");
        }

        var length = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        if (length is 0 or > 255)
        {
            throw new MalformedResponseException(
                $"Header {field} length {length} at byte {position} must be between 1 and 255.");
        }

        if (!ValueLengthMatchesKind(kind, (int)length))
        {
            throw new MalformedResponseException(
                $"Header {field} of kind {kind} has {length} bytes, expected {HeaderKindWidth(kind)}.");
        }

        position += 4;
        if (position + length > payload.Length)
        {
            throw new MalformedResponseException(
                $"Header {field} of {length} bytes at byte {position} exceeds the {payload.Length}-byte payload.");
        }

        var value = payload[position..(position + (int)length)].ToArray();
        position += (int)length;
        return value;
    }

    internal static HeaderKind MapHeaderKind(byte value)
    {
        return value switch
        {
            1 => HeaderKind.Raw,
            2 => HeaderKind.String,
            3 => HeaderKind.Bool,
            4 => HeaderKind.Int8,
            5 => HeaderKind.Int16,
            6 => HeaderKind.Int32,
            7 => HeaderKind.Int64,
            8 => HeaderKind.Int128,
            9 => HeaderKind.Uint8,
            10 => HeaderKind.Uint16,
            11 => HeaderKind.Uint32,
            12 => HeaderKind.Uint64,
            13 => HeaderKind.Uint128,
            14 => HeaderKind.Float,
            15 => HeaderKind.Double,
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, null)
        };
    }

    /// <summary>
    ///     The typed accessors on <see cref="HeaderValue" /> slice the raw bytes by kind, so a value whose
    ///     length disagrees with its kind byte is rejected at parse time instead of throwing in the caller's
    ///     message handler.
    /// </summary>
    private static bool ValueLengthMatchesKind(HeaderKind kind, int length)
    {
        var width = HeaderKindWidth(kind);
        return width == 0 || width == length;
    }

    private static int HeaderKindWidth(HeaderKind kind)
    {
        return kind switch
        {
            HeaderKind.Bool or HeaderKind.Int8 or HeaderKind.Uint8 => 1,
            HeaderKind.Int16 or HeaderKind.Uint16 => 2,
            HeaderKind.Int32 or HeaderKind.Uint32 or HeaderKind.Float => 4,
            HeaderKind.Int64 or HeaderKind.Uint64 or HeaderKind.Double => 8,
            HeaderKind.Int128 or HeaderKind.Uint128 => 16,
            _ => 0
        };
    }

    /// <summary>
    ///     The bytes left in the payload bound how many elements can exist, so a count above that is rejected
    ///     before the list is pre-sized instead of failing mid-loop.
    /// </summary>
    private static int ValidatedCollectionSize(uint count, int remaining, int minElementSize, string field)
    {
        if (count > Math.Max(remaining, 0) / minElementSize)
        {
            throw new MalformedResponseException(
                $"{field} {count} exceeds remaining payload of {remaining} bytes.");
        }

        return (int)count;
    }

    /// <summary>
    ///     Length prefixes are u32 on the wire. Reading them signed would let 2^31 and above go negative and
    ///     surface as a slicing exception instead of <see cref="MalformedResponseException" />.
    /// </summary>
    private static int ReadLength(ReadOnlySpan<byte> payload, int position, string field)
    {
        if (position + 4 > payload.Length)
        {
            throw new MalformedResponseException($"{field} length prefix at byte {position} is truncated.");
        }

        var length = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        var remaining = payload.Length - position - 4;
        if (length > remaining)
        {
            throw new MalformedResponseException(
                $"{field} length {length} exceeds remaining payload of {remaining} bytes.");
        }

        return (int)length;
    }

    private static string ReadString(ReadOnlySpan<byte> payload, ref int position, string field)
    {
        var length = ReadLength(payload, position, field);
        position += 4;
        var value = Encoding.UTF8.GetString(payload[position..(position + length)]);
        position += length;
        return value;
    }

    private static bool TryMapHeaderKind(byte value, out HeaderKind kind)
    {
        if (value is >= 1 and <= 15)
        {
            kind = MapHeaderKind(value);
            return true;
        }

        kind = default;
        return false;
    }

    private static Dictionary<HeaderKey, HeaderValue> MapOptions(ReadOnlySpan<byte> payload, int position,
        out int readBytes)
    {
        // Every length here is server-controlled. Bound each entry against the
        // block before slicing: an entry that overruns `end` would otherwise be
        // accepted and silently consume the response bytes that follow the block.
        var optionsLength = ReadLength(payload, position, "Options block");
        readBytes = 4 + optionsLength;

        var options = new Dictionary<HeaderKey, HeaderValue>();
        var cursor = position + 4;
        var end = cursor + optionsLength;
        while (cursor < end)
        {
            var keyKindCode = ReadOptionByte(payload, ref cursor, end, position);
            var key = ReadOptionField(payload, ref cursor, end, position, "key");

            var valueKindCode = ReadOptionByte(payload, ref cursor, end, position);
            var value = ReadOptionField(payload, ref cursor, end, position, "value");

            // A newer server may encode an option under a kind this build has no name for.
            // Its bytes are already consumed, so dropping just this entry keeps the rest of
            // the block, and the response fields behind it, readable.
            if (!TryMapHeaderKind(keyKindCode, out var keyKind) ||
                !TryMapHeaderKind(valueKindCode, out var valueKind))
            {
                continue;
            }

            if (!ValueLengthMatchesKind(keyKind, key.Length))
            {
                throw new MalformedResponseException(
                    $"Malformed options block at byte {position}: key of kind {keyKind} has {key.Length} bytes.");
            }

            if (!ValueLengthMatchesKind(valueKind, value.Length))
            {
                throw new MalformedResponseException(
                    $"Malformed options block at byte {position}: value of kind {valueKind} has {value.Length} " +
                    "bytes.");
            }

            options[new HeaderKey
            {
                Kind = keyKind,
                Value = key
            }] = new HeaderValue
            {
                Kind = valueKind,
                Value = value
            };
        }

        if (cursor != end)
        {
            throw new MalformedResponseException(
                $"Malformed options block at byte {position}: entries ended at {cursor}, block ends at {end}.");
        }

        return options;
    }

    private static byte ReadOptionByte(ReadOnlySpan<byte> payload, ref int cursor, int end, int blockStart)
    {
        if (cursor + 1 > end)
        {
            throw new MalformedResponseException(
                $"Malformed options block at byte {blockStart}: entry kind runs past the end of the block.");
        }

        var value = payload[cursor];
        cursor += 1;
        return value;
    }

    private static byte[] ReadOptionField(ReadOnlySpan<byte> payload, ref int cursor, int end, int blockStart,
        string field)
    {
        if (cursor + 4 > end)
        {
            throw new MalformedResponseException(
                $"Malformed options block at byte {blockStart}: {field} length runs past the end of the block.");
        }

        var length = BinaryPrimitives.ReadUInt32LittleEndian(payload[cursor..(cursor + 4)]);
        cursor += 4;
        if (length is < 1 or > 255)
        {
            throw new MalformedResponseException(
                $"Malformed options block at byte {blockStart}: {field} length {length} is outside 1..=255.");
        }

        if (cursor + (int)length > end)
        {
            throw new MalformedResponseException(
                $"Malformed options block at byte {blockStart}: {field} of {length} bytes runs past the end of " +
                "the block.");
        }

        var bytes = payload[cursor..(cursor + (int)length)].ToArray();
        cursor += (int)length;
        return bytes;
    }

    internal static IReadOnlyList<StreamResponse> MapStreams(ReadOnlySpan<byte> payload)
    {
        List<StreamResponse> streams = new();
        var length = payload.Length;
        var position = 0;

        while (position < length)
        {
            var (stream, readBytes) = MapToStream(payload, position);
            streams.Add(stream);
            position += readBytes;
        }

        return streams.AsReadOnly();
    }

    /// <summary>
    ///     Maps a send messages reply: <c>[count:4][stream_id:4][topic_id:4][partition_id:4][base_offset:8]*</c>.
    /// </summary>
    /// <remarks>
    ///     Strict: the payload is the whole reply body, so any length that does not match the count
    ///     is a shape this build cannot read, not a prefix of a larger value. Entries are read at a
    ///     fixed 20-byte stride, so tolerating a tail would return garbage as a successful decode.
    /// </remarks>
    internal static SendMessagesResponse MapSendMessages(ReadOnlySpan<byte> payload)
    {
        const int confirmationSize = 4 + 4 + 4 + 8;

        if (payload.Length < 4)
        {
            throw new InvalidResponseException("Send messages reply is shorter than the confirmation count prefix.");
        }

        var count = BinaryPrimitives.ReadUInt32LittleEndian(payload[..4]);
        if (payload.Length - 4 != count * (long)confirmationSize)
        {
            throw new InvalidResponseException(
                $"Send messages reply length {payload.Length} does not match {count} confirmations.");
        }

        var confirmations = new SendMessagesConfirmation[count];
        var position = 4;
        for (var i = 0; i < confirmations.Length; i++)
        {
            confirmations[i] = new SendMessagesConfirmation
            {
                StreamId = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]),
                TopicId = BinaryPrimitives.ReadUInt32LittleEndian(payload[(position + 4)..(position + 8)]),
                PartitionId = BinaryPrimitives.ReadUInt32LittleEndian(payload[(position + 8)..(position + 12)]),
                BaseOffset = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 12)..(position + 20)])
            };
            position += confirmationSize;
        }

        return new SendMessagesResponse { Confirmations = confirmations };
    }

    internal static StreamResponse MapStream(ReadOnlySpan<byte> payload)
    {
        var (stream, position) = MapToStream(payload, 0);

        List<TopicResponse> topics = new(ValidatedCollectionSize(stream.TopicsCount, payload.Length - position,
            MIN_TOPIC_SIZE, "Stream topics count"));
        for (var i = 0; i < stream.TopicsCount; i++)
        {
            var (topic, readBytes) = MapToTopic(payload, position);
            topics.Add(topic);
            position += readBytes;
        }

        return new StreamResponse
        {
            Id = stream.Id,
            TopicsCount = stream.TopicsCount,
            Name = stream.Name,
            Topics = topics,
            CreatedAt = stream.CreatedAt,
            MessagesCount = stream.MessagesCount,
            Size = stream.Size,
            Options = stream.Options
        };
    }

    private static (StreamResponse stream, int readBytes) MapToStream(ReadOnlySpan<byte> payload, int position)
    {
        var id = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        var createdAt = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 4)..(position + 12)]);
        var topicsCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[(position + 12)..(position + 16)]);
        var sizeBytes = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 16)..(position + 24)]);
        var messagesCount = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 24)..(position + 32)]);
        var nameLength = (int)payload[position + 32];

        var name = Encoding.UTF8.GetString(payload[(position + 33)..(position + 33 + nameLength)]);
        var readBytes = 4 + 4 + 8 + 8 + 8 + 1 + nameLength;
        var options = MapOptions(payload, position + readBytes, out var optionsReadBytes);
        readBytes += optionsReadBytes;

        return (
            new StreamResponse
            {
                Id = id,
                TopicsCount = topicsCount,
                Name = name,
                Size = sizeBytes,
                MessagesCount = messagesCount,
                CreatedAt = DateTimeOffsetUtils.FromUnixTimeMicroSeconds(createdAt).LocalDateTime,
                Options = options
            }, readBytes);
    }

    internal static IReadOnlyList<TopicResponse> MapTopics(ReadOnlySpan<byte> payload)
    {
        var topicsCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[..4]);
        var position = 4;
        List<TopicResponse> topics = new(ValidatedCollectionSize(topicsCount, payload.Length - position,
            MIN_TOPIC_SIZE, "Topics count"));

        for (var i = 0; i < topicsCount; i++)
        {
            var (topic, readBytes) = MapToTopic(payload, position);
            topics.Add(topic);
            position += readBytes;
        }

        return topics.AsReadOnly();
    }

    internal static TopicResponse MapTopic(ReadOnlySpan<byte> payload)
    {
        var (topic, position) = MapToTopic(payload, 0);
        List<PartitionResponse> partitions = new();
        var length = payload.Length;

        while (position < length)
        {
            var (partition, readBytes) = MapToPartition(payload, position);
            partitions.Add(partition);
            position += readBytes;
        }

        return new TopicResponse
        {
            Id = topic.Id,
            Name = topic.Name,
            PartitionsCount = topic.PartitionsCount,
            CompressionAlgorithm = topic.CompressionAlgorithm,
            CreatedAt = topic.CreatedAt,
            MessageExpiry = topic.MessageExpiry,
            MessagesCount = topic.MessagesCount,
            Size = topic.Size,
            MaxTopicSize = topic.MaxTopicSize,
            Partitions = partitions,
            Options = topic.Options,
            DerivedOptions = topic.DerivedOptions
        };
    }

    private static (TopicResponse topic, int readBytes) MapToTopic(ReadOnlySpan<byte> payload, int position)
    {
        var id = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        var createdAt = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 4)..(position + 12)]);
        var partitionsCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[(position + 12)..(position + 16)]);
        var messageExpiry = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 16)..(position + 24)]);
        var compressionAlgorithm = payload[position + 24];
        var maxTopicSize = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 25)..(position + 33)]);
        var sizeBytes = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 33)..(position + 41)]);
        var messagesCount = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 41)..(position + 49)]);
        var nameLength = (int)payload[position + 49];
        var name = Encoding.UTF8.GetString(payload[(position + 50)..(position + 50 + nameLength)]);
        var readBytes = 4 + 8 + 4 + 8 + 1 + 8 + 8 + 8 + 1 + nameLength;
        var options = MapOptions(payload, position + readBytes, out var optionsReadBytes);
        readBytes += optionsReadBytes;
        var derivedOptions = MapOptions(payload, position + readBytes, out var derivedOptionsReadBytes);
        readBytes += derivedOptionsReadBytes;

        return (
            new TopicResponse
            {
                Id = id,
                PartitionsCount = partitionsCount,
                Name = name,
                CompressionAlgorithm = (CompressionAlgorithm)compressionAlgorithm,
                MessagesCount = messagesCount,
                Size = sizeBytes,
                CreatedAt = DateTimeOffsetUtils.FromUnixTimeMicroSeconds(createdAt).LocalDateTime,
                MessageExpiry = DurationHelpers.FromDuration(messageExpiry),
                MaxTopicSize = maxTopicSize,
                Options = options,
                DerivedOptions = derivedOptions
            }, readBytes);
    }

    private static (PartitionResponse partition, int readBytes) MapToPartition(ReadOnlySpan<byte>
        payload, int position)
    {
        var id = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        var createdAt = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 4)..(position + 12)]);
        var segmentsCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[(position + 12)..(position + 16)]);
        var currentOffset = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 16)..(position + 24)]);
        var sizeBytes = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 24)..(position + 32)]);
        var messagesCount = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 32)..(position + 40)]);
        var readBytes = 4 + 4 + 8 + 8 + 8 + 8;

        return (
            new PartitionResponse
            {
                Id = id,
                SegmentsCount = segmentsCount,
                CurrentOffset = currentOffset,
                Size = sizeBytes,
                CreatedAt = DateTimeOffsetUtils.FromUnixTimeMicroSeconds(createdAt).LocalDateTime,
                MessagesCount = messagesCount
            }, readBytes);
    }

    internal static List<ConsumerGroupResponse> MapConsumerGroups(ReadOnlySpan<byte> payload)
    {
        List<ConsumerGroupResponse> consumerGroups = new();
        var length = payload.Length;
        var position = 0;
        while (position < length)
        {
            var (consumerGroup, readBytes) = MapToConsumerGroup(payload, position);
            consumerGroups.Add(consumerGroup);
            position += readBytes;
        }

        return consumerGroups;
    }

    internal static StatsResponse MapStats(ReadOnlySpan<byte> payload)
    {
        var processId = BinaryPrimitives.ReadUInt32LittleEndian(payload[..4]);
        var cpuUsage = BitConverter.ToSingle(payload[4..8]);
        var totalCpuUsage = BitConverter.ToSingle(payload[8..12]);
        var memoryUsage = BinaryPrimitives.ReadUInt64LittleEndian(payload[12..20]);
        var totalMemory = BinaryPrimitives.ReadUInt64LittleEndian(payload[20..28]);
        var availableMemory = BinaryPrimitives.ReadUInt64LittleEndian(payload[28..36]);
        var runTime = BinaryPrimitives.ReadUInt64LittleEndian(payload[36..44]);
        var startTime = BinaryPrimitives.ReadUInt64LittleEndian(payload[44..52]);
        var readBytes = BinaryPrimitives.ReadUInt64LittleEndian(payload[52..60]);
        var writtenBytes = BinaryPrimitives.ReadUInt64LittleEndian(payload[60..68]);
        var totalSizeBytes = BinaryPrimitives.ReadUInt64LittleEndian(payload[68..76]);
        var streamsCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[76..80]);
        var topicsCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[80..84]);
        var partitionsCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[84..88]);
        var segmentsCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[88..92]);
        var messagesCount = BinaryPrimitives.ReadUInt64LittleEndian(payload[92..100]);
        var clientsCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[100..104]);
        var consumerGroupsCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[104..108]);
        var position = 108;

        var hostname = ReadString(payload, ref position, "Stats hostname");
        var osName = ReadString(payload, ref position, "Stats os name");
        var osVersion = ReadString(payload, ref position, "Stats os version");
        var kernelVersion = ReadString(payload, ref position, "Stats kernel version");
        var iggyVersion = ReadString(payload, ref position, "Stats iggy version");
        var iggySemVersion = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        position += 4;

        var cacheMetricsLength = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        position += 4;

        var cacheMetricsList = new Dictionary<CacheMetricsKey, CacheMetrics>(ValidatedCollectionSize(
            cacheMetricsLength, payload.Length - position, CACHE_METRICS_ENTRY_SIZE, "Cache metrics count"));
        for (var i = 0; i < cacheMetricsLength; i++)
        {
            var cacheMetricsKey = new CacheMetricsKey
            {
                StreamId = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]),
                TopicId = BinaryPrimitives.ReadUInt32LittleEndian(payload[(position + 4)..(position + 8)]),
                PartitionId = BinaryPrimitives.ReadUInt32LittleEndian(payload[(position + 8)..(position + 12)])
            };

            var cacheMetrics = new CacheMetrics
            {
                Hits = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 12)..(position + 20)]),
                Misses = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 20)..(position + 28)]),
                HitRatio = BinaryPrimitives.ReadSingleLittleEndian(payload[(position + 28)..(position + 32)])
            };

            cacheMetricsList.Add(cacheMetricsKey, cacheMetrics);
            position += 32;
        }

        var threadsCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        position += 4;
        var freeDiskSpace = BinaryPrimitives.ReadUInt64LittleEndian(payload[position..(position + 8)]);
        position += 8;
        var totalDiskSpace = BinaryPrimitives.ReadUInt64LittleEndian(payload[position..(position + 8)]);
        position += 8;

        return new StatsResponse
        {
            ProcessId = processId,
            Hostname = hostname,
            ClientsCount = clientsCount,
            CpuUsage = cpuUsage,
            TotalCpuUsage = totalCpuUsage,
            MemoryUsage = memoryUsage,
            TotalMemory = totalMemory,
            AvailableMemory = availableMemory,
            RunTime = runTime,
            StartTime = DateTimeOffsetUtils.FromUnixTimeMicroSeconds(startTime),
            ReadBytes = readBytes,
            WrittenBytes = writtenBytes,
            StreamsCount = streamsCount,
            KernelVersion = kernelVersion,
            MessagesCount = messagesCount,
            TopicsCount = topicsCount,
            PartitionsCount = partitionsCount,
            SegmentsCount = segmentsCount,
            OsName = osName,
            OsVersion = osVersion,
            ConsumerGroupsCount = consumerGroupsCount,
            MessagesSizeBytes = totalSizeBytes,
            IggyServerVersion = iggyVersion,
            IggyServerSemver = iggySemVersion,
            CacheMetrics = cacheMetricsList,
            ThreadsCount = threadsCount,
            FreeDiskSpace = freeDiskSpace,
            TotalDiskSpace = totalDiskSpace
        };
    }

    internal static ConsumerGroupResponse MapConsumerGroup(ReadOnlySpan<byte> payload)
    {
        var (consumerGroup, position) = MapToConsumerGroup(payload, 0);
        var members = new List<ConsumerGroupMember>();
        while (position < payload.Length)
        {
            var (member, readBytes) = MapToMember(payload, position);
            members.Add(member);
            position += readBytes;
        }

        return new ConsumerGroupResponse
        {
            Id = consumerGroup.Id,
            MembersCount = consumerGroup.MembersCount,
            PartitionsCount = consumerGroup.PartitionsCount,
            Name = consumerGroup.Name,
            Members = members
        };
    }

    private static (ConsumerGroupMember, int readBytes) MapToMember(ReadOnlySpan<byte> payload, int position)
    {
        if (position + MEMBER_HEADER_SIZE > payload.Length)
        {
            throw new MalformedResponseException(
                $"Malformed consumer group member at byte {position}: {payload.Length - position} bytes cannot " +
                "hold a member header.");
        }

        var id = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        var partitionsCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[(position + 4)..(position + 8)]);

        var readBytes = MEMBER_HEADER_SIZE + (long)partitionsCount * 4;
        if (position + readBytes > payload.Length)
        {
            throw new MalformedResponseException(
                $"Malformed consumer group member at byte {position}: partitions count {partitionsCount} does not " +
                "fit the response.");
        }

        var partitions = new List<uint>((int)partitionsCount);
        for (var i = 0; i < partitionsCount; i++)
        {
            var partitionStart = position + MEMBER_HEADER_SIZE + i * 4;
            var partitionId = BinaryPrimitives.ReadUInt32LittleEndian(payload[partitionStart..(partitionStart + 4)]);
            partitions.Add(partitionId);
        }

        return (new ConsumerGroupMember
        {
            Id = id,
            PartitionsCount = partitionsCount,
            Partitions = partitions
        },
            (int)readBytes);
    }

    private static (ConsumerGroupResponse consumerGroup, int readBytes) MapToConsumerGroup(ReadOnlySpan<byte> payload,
        int position)
    {
        if (position + CONSUMER_GROUP_HEADER_SIZE > payload.Length)
        {
            throw new MalformedResponseException(
                $"Malformed consumer group at byte {position}: {payload.Length - position} bytes cannot hold a " +
                "consumer group header.");
        }

        var id = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        var partitionsCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[(position + 4)..(position + 8)]);
        var membersCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[(position + 8)..(position + 12)]);
        var nameLength = payload[position + 12];
        if (position + CONSUMER_GROUP_HEADER_SIZE + nameLength > payload.Length)
        {
            throw new MalformedResponseException(
                $"Malformed consumer group at byte {position}: name length {nameLength} does not fit the response.");
        }

        var name = Encoding.UTF8.GetString(payload[(position + CONSUMER_GROUP_HEADER_SIZE)..(position + CONSUMER_GROUP_HEADER_SIZE + nameLength)]);

        return (new ConsumerGroupResponse
        {
            Id = id,
            Name = name,
            MembersCount = membersCount,
            PartitionsCount = partitionsCount
        }, CONSUMER_GROUP_HEADER_SIZE + nameLength);
    }

    internal static IReadOnlyList<OptionSpec> MapOptionSpecs(ReadOnlySpan<byte> payload)
    {
        var count = BinaryPrimitives.ReadUInt32LittleEndian(payload[..4]);
        var position = 4;
        var specs = new List<OptionSpec>(ValidatedCollectionSize(count, payload.Length - position,
            MIN_OPTION_SPEC_SIZE, "Option specs count"));
        for (var i = 0; i < count; i++)
        {
            var keyLength = payload[position];
            position += 1;
            if (position + keyLength > payload.Length)
            {
                throw new MalformedResponseException(
                    $"Malformed DescribeOptions response: option key of {keyLength} bytes at offset {position} " +
                    $"overruns the {payload.Length}-byte payload");
            }

            var key = Encoding.UTF8.GetString(payload[position..(position + keyLength)]);
            position += keyLength;

            var kind = payload[position];
            position += 1;

            var defaultLength = ReadLength(payload, position, "Option default value");
            position += 4;
            var defaultValue = payload[position..(position + defaultLength)].ToArray();
            position += defaultLength;

            var description = ReadString(payload, ref position, "Option description");

            specs.Add(new OptionSpec
            {
                Key = key,
                Kind = MapHeaderKind(kind),
                DefaultValue = defaultValue,
                Description = description
            });
        }

        return specs;
    }

    internal static ClusterMetadata MapClusterMetadata(ReadOnlySpan<byte> payload)
    {
        var position = 0;
        var clusterName = ReadString(payload, ref position, "Cluster name");
        if (position + 4 > payload.Length)
        {
            throw new MalformedResponseException($"Cluster nodes count at byte {position} is truncated.");
        }

        var nodesCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        position += 4;

        var nodes = new ClusterNode[ValidatedCollectionSize(nodesCount, payload.Length - position,
            MIN_CLUSTER_NODE_SIZE, "Cluster nodes count")];
        for (var i = 0; i < nodes.Length; i++)
        {
            nodes[i] = MapClusterNode(payload, ref position);
        }

        return new ClusterMetadata
        {
            Name = clusterName,
            Nodes = nodes
        };
    }

    private static ClusterNode MapClusterNode(ReadOnlySpan<byte> payload, ref int position)
    {
        var name = ReadString(payload, ref position, "Cluster node name");
        var ip = ReadString(payload, ref position, "Cluster node ip");
        if (position + CLUSTER_NODE_TAIL_SIZE > payload.Length)
        {
            throw new MalformedResponseException(
                $"Cluster node at byte {position}: {payload.Length - position} bytes cannot hold the ports, role and status.");
        }

        var tcp = BinaryPrimitives.ReadUInt16LittleEndian(payload[position..(position + 2)]);
        var quic = BinaryPrimitives.ReadUInt16LittleEndian(payload[(position + 2)..(position + 4)]);
        var http = BinaryPrimitives.ReadUInt16LittleEndian(payload[(position + 4)..(position + 6)]);
        var webSocket = BinaryPrimitives.ReadUInt16LittleEndian(payload[(position + 6)..(position + 8)]);
        var role = payload[position + 8] switch
        {
            0 => ClusterNodeRole.Leader,
            1 => ClusterNodeRole.Follower,
            var unknown => throw new MalformedResponseException($"Unknown cluster node role {unknown}.")
        };
        var status = payload[position + 9] switch
        {
            0 => ClusterNodeStatus.Healthy,
            1 => ClusterNodeStatus.Starting,
            2 => ClusterNodeStatus.Stopping,
            3 => ClusterNodeStatus.Unreachable,
            4 => ClusterNodeStatus.Maintenance,
            _ => ClusterNodeStatus.Unknown
        };
        position += 10;

        return new ClusterNode
        {
            Name = name,
            Ip = ip,
            Endpoints = new TransportEndpoints
            {
                Tcp = tcp,
                Quic = quic,
                Http = http,
                WebSocket = webSocket
            },
            Role = role,
            Status = status
        };
    }
}
