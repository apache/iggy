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
using Apache.Iggy.Enums;
using Apache.Iggy.Extensions;
using Apache.Iggy.Headers;
using Apache.Iggy.Messages;

namespace Apache.Iggy.Mappers;

internal static class BinaryMapper
{
    private const int PropertiesSize = 56;

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
            var permissionLength = BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 1)..(position + 5)]);
            ReadOnlySpan<byte> permissionsPayload = payload[(position + 5)..(position + 5 + permissionLength)];
            var permissions = MapPermissions(permissionsPayload);
            return new UserResponse
            {
                Permissions = permissions,
                Id = response.Id,
                CreatedAt = response.CreatedAt,
                Username = response.Username,
                Status = response.Status
            };
        }

        return new UserResponse
        {
            Id = response.Id,
            CreatedAt = response.CreatedAt,
            Username = response.Username,
            Status = response.Status,
            Permissions = null
        };
    }

    private static Permissions MapPermissions(ReadOnlySpan<byte> bytes)
    {
        var streamMap = new Dictionary<int, StreamPermissions>();
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
                var streamId = BinaryPrimitives.ReadInt32LittleEndian(bytes[index..(index + 4)]);
                index += sizeof(int);

                var manageStream = bytes[index++] == 1;
                var readStream = bytes[index++] == 1;
                var manageTopics = bytes[index++] == 1;
                var readTopics = bytes[index++] == 1;
                var pollMessagesStream = bytes[index++] == 1;
                var sendMessagesStream = bytes[index++] == 1;
                var topicsMap = new Dictionary<int, TopicPermissions>();

                if (bytes[index++] == 1)
                {
                    while (true)
                    {
                        var topicId = BinaryPrimitives.ReadInt32LittleEndian(bytes[index..(index + 4)]);
                        index += sizeof(int);

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

        return (new UserResponse
        {
            Id = id,
            CreatedAt = createdAt,
            Status = userStatus,
            Username = username
        },
            readBytes);
    }

    internal static ClientResponse MapClient(ReadOnlySpan<byte> payload)
    {
        var (response, position) = MapClientInfo(payload, 0);
        var consumerGroups = new List<ConsumerGroupInfo>();
        var length = payload.Length;

        while (position < length)
        {
            for (var i = 0; i < response.ConsumerGroupsCount; i++)
            {
                var streamId = BinaryPrimitives.ReadInt32LittleEndian(payload[position..(position + 4)]);
                var topicId = BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 4)..(position + 8)]);
                var consumerGroupId = BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 8)..(position + 12)]);
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

    private static (ClientResponse response, int position) MapClientInfo(ReadOnlySpan<byte> payload, int position)
    {
        int readBytes;
        var id = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        var userId = BinaryPrimitives.ReadUInt32LittleEndian(payload[(position + 4)..(position + 8)]);
        var transportByte = payload[position + 8];
        var transport = transportByte switch
        {
            1 => "TCP",
            2 => "QUIC",
            _ => "Unknown"
        };
        var addressLength = BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 9)..(position + 13)]);
        var address = Encoding.UTF8.GetString(payload[(position + 13)..(position + 13 + addressLength)]);
        readBytes = 4 + 1 + 4 + 4 + addressLength;
        position += readBytes;
        var consumerGroupsCount = BinaryPrimitives.ReadInt32LittleEndian(payload[position..(position + 4)]);
        readBytes += 4;

        return (new ClientResponse
        {
            ClientId = id,
            UserId = userId,
            Transport = Enum.Parse<Protocol>(transport, true),
            Address = address,
            ConsumerGroupsCount = consumerGroupsCount
        }, readBytes);
    }

    internal static OffsetResponse MapOffsets(ReadOnlySpan<byte> payload)
    {
        var partitionId = BinaryPrimitives.ReadInt32LittleEndian(payload[..4]);
        var currentOffset = BinaryPrimitives.ReadUInt64LittleEndian(payload[4..12]);
        var offset = BinaryPrimitives.ReadUInt64LittleEndian(payload[12..20]);

        return new OffsetResponse
        {
            CurrentOffset = currentOffset,
            StoredOffset = offset,
            PartitionId = partitionId
        };
    }

    internal static PolledMessages MapMessages(ReadOnlySpan<byte> payload,
        Func<byte[], byte[]>? decryptor = null)
    {
        var length = payload.Length;
        var partitionId = BinaryPrimitives.ReadInt32LittleEndian(payload[..4]);
        var currentOffset = BinaryPrimitives.ReadUInt64LittleEndian(payload[4..12]);
        var messagesCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[12..16]);
        var position = 16;
        if (position >= length)
        {
            return PolledMessages.Empty;
        }

        List<MessageResponse> messages = new();

        while (position < length)
        {
            var checksum = BinaryPrimitives.ReadUInt64LittleEndian(payload[position..(position + 8)]);
            var id = BinaryPrimitives.ReadUInt128LittleEndian(payload[(position + 8)..(position + 24)]);
            var offset = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 24)..(position + 32)]);
            var timestamp = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 32)..(position + 40)]);
            var originTimestamp = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 40)..(position + 48)]);
            var headersLength = BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 48)..(position + 52)]);
            var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 52)..(position + 56)]);

            Dictionary<HeaderKey, HeaderValue>? headers = headersLength switch
            {
                0 => null,
                > 0 => MapHeaders(
                    payload[(position + 56 + payloadLength)..(position + 56 + payloadLength + headersLength)]),
                < 0 => throw new ArgumentOutOfRangeException()
            };

            var payloadRangeStart = position + 56;
            var payloadRangeEnd = position + 56 + payloadLength;
            if (payloadRangeStart > length || payloadRangeEnd > length)
            {
                break;
            }

            ReadOnlySpan<byte> payloadSlice = payload[payloadRangeStart..payloadRangeEnd];
            var messagePayload = ArrayPool<byte>.Shared.Rent(payloadSlice.Length);
            var payloadSliceLen = payloadSlice.Length;

            try
            {
                payloadSlice.CopyTo(messagePayload.AsSpan()[..payloadSliceLen]);

                messages.Add(new MessageResponse
                {
                    Header = new MessageHeader
                    {
                        Checksum = checksum,
                        Id = id,
                        Offset = offset,
                        OriginTimestamp = originTimestamp,
                        PayloadLength = payloadLength,
                        Timestamp = DateTimeOffsetUtils.FromUnixTimeMicroSeconds(timestamp),
                        UserHeadersLength = headersLength
                    },
                    UserHeaders = headers,
                    Payload = decryptor is not null
                        ? decryptor(messagePayload[..payloadSliceLen])
                        : messagePayload[..payloadSliceLen]
                });
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(messagePayload);
            }

            position += 56 + payloadLength + headersLength;
            if (position + PropertiesSize >= length)
            {
                break;
            }
        }

        return new PolledMessages
        {
            PartitionId = partitionId,
            CurrentOffset = currentOffset,
            Messages = messages.AsReadOnly()
        };
    }

    private static Dictionary<HeaderKey, HeaderValue> MapHeaders(ReadOnlySpan<byte> payload)
    {
        var headers = new Dictionary<HeaderKey, HeaderValue>();
        var position = 0;

        while (position < payload.Length)
        {
            var keyLength = BinaryPrimitives.ReadInt32LittleEndian(payload[position..(position + 4)]);
            if (keyLength is 0 or > 255)
            {
                throw new ArgumentException("Key has incorrect size, must be between 1 and 255", nameof(keyLength));
            }

            var key = Encoding.UTF8.GetString(payload[(position + 4)..(position + 4 + keyLength)]);
            position += 4 + keyLength;

            var headerKind = MapHeaderKind(payload, position);
            position++;
            var valueLength = BinaryPrimitives.ReadInt32LittleEndian(payload[position..(position + 4)]);
            if (valueLength is 0 or > 255)
            {
                throw new ArgumentException("Value has incorrect size, must be between 1 and 255", nameof(valueLength));
            }

            position += 4;
            ReadOnlySpan<byte> value = payload[position..(position + valueLength)];
            position += valueLength;
            headers.Add(HeaderKey.New(key), new HeaderValue
            {
                Kind = headerKind,
                Value = value.ToArray()
            });
        }

        return headers;
    }

    private static HeaderKind MapHeaderKind(ReadOnlySpan<byte> payload, int position)
    {
        var headerKind = payload[position] switch
        {
            1 => HeaderKind.Raw,
            2 => HeaderKind.String,
            3 => HeaderKind.Bool,
            6 => HeaderKind.Int32,
            7 => HeaderKind.Int64,
            8 => HeaderKind.Int128,
            11 => HeaderKind.Uint32,
            12 => HeaderKind.Uint64,
            13 => HeaderKind.Uint128,
            14 => HeaderKind.Float,
            15 => HeaderKind.Double,
            _ => throw new ArgumentOutOfRangeException()
        };
        return headerKind;
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

    internal static StreamResponse MapStream(ReadOnlySpan<byte> payload)
    {
        var (stream, position) = MapToStream(payload, 0);
        List<TopicResponse> topics = new();
        var length = payload.Length;

        while (position < length)
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
            Size = stream.Size
        };
    }

    private static (StreamResponse stream, int readBytes) MapToStream(ReadOnlySpan<byte> payload, int position)
    {
        var id = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        var createdAt = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 4)..(position + 12)]);
        var topicsCount = BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 12)..(position + 16)]);
        var sizeBytes = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 16)..(position + 24)]);
        var messagesCount = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 24)..(position + 32)]);
        var nameLength = (int)payload[position + 32];

        var name = Encoding.UTF8.GetString(payload[(position + 33)..(position + 33 + nameLength)]);
        var readBytes = 4 + 4 + 8 + 8 + 8 + 1 + nameLength;

        return (
            new StreamResponse
            {
                Id = id,
                TopicsCount = topicsCount,
                Name = name,
                Size = sizeBytes,
                MessagesCount = messagesCount,
                CreatedAt = DateTimeOffsetUtils.FromUnixTimeMicroSeconds(createdAt).LocalDateTime
            }, readBytes);
    }

    internal static IReadOnlyList<TopicResponse> MapTopics(ReadOnlySpan<byte> payload)
    {
        List<TopicResponse> topics = new();
        var length = payload.Length;
        var position = 0;

        while (position < length)
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
            ReplicationFactor = topic.ReplicationFactor,
            MaxTopicSize = topic.MaxTopicSize,
            Partitions = partitions
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
        var replicationFactor = payload[position + 33];
        var sizeBytes = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 34)..(position + 42)]);
        var messagesCount = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 42)..(position + 50)]);
        var nameLength = (int)payload[position + 50];
        var name = Encoding.UTF8.GetString(payload[(position + 51)..(position + 51 + nameLength)]);
        var readBytes = 4 + 8 + 4 + 8 + 1 + 8 + 1 + 8 + 8 + 1 + name.Length;

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
                MessageExpiry = messageExpiry,
                ReplicationFactor = replicationFactor,
                MaxTopicSize = maxTopicSize
            }, readBytes);
    }

    private static (PartitionResponse partition, int readBytes) MapToPartition(ReadOnlySpan<byte>
        payload, int position)
    {
        var id = BinaryPrimitives.ReadInt32LittleEndian(payload[position..(position + 4)]);
        var createdAt = BinaryPrimitives.ReadUInt64LittleEndian(payload[(position + 4)..(position + 12)]);
        var segmentsCount = BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 12)..(position + 16)]);
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
        var processId = BinaryPrimitives.ReadInt32LittleEndian(payload[..4]);
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
        var streamsCount = BinaryPrimitives.ReadInt32LittleEndian(payload[76..80]);
        var topicsCount = BinaryPrimitives.ReadInt32LittleEndian(payload[80..84]);
        var partitionsCount = BinaryPrimitives.ReadInt32LittleEndian(payload[84..88]);
        var segmentsCount = BinaryPrimitives.ReadInt32LittleEndian(payload[88..92]);
        var messagesCount = BinaryPrimitives.ReadUInt64LittleEndian(payload[92..100]);
        var clientsCount = BinaryPrimitives.ReadInt32LittleEndian(payload[100..104]);
        var consumerGroupsCount = BinaryPrimitives.ReadInt32LittleEndian(payload[104..108]);
        var position = 108;

        var hostnameLength = BinaryPrimitives.ReadInt32LittleEndian(payload[position..(position + 4)]);
        var hostname = Encoding.UTF8.GetString(payload[(position + 4)..(position + 4 + hostnameLength)]);
        position += 4 + hostnameLength;
        var osNameLength = BinaryPrimitives.ReadInt32LittleEndian(payload[position..(position + 4)]);
        var osName = Encoding.UTF8.GetString(payload[(position + 4)..(position + 4 + osNameLength)]);
        position += 4 + osNameLength;
        var osVersionLength = BinaryPrimitives.ReadInt32LittleEndian(payload[position..(position + 4)]);
        var osVersion = Encoding.UTF8.GetString(payload[(position + 4)..(position + 4 + osVersionLength)]);
        position += 4 + osVersionLength;
        var kernelVersionLength = BinaryPrimitives.ReadInt32LittleEndian(payload[position..(position + 4)]);
        var kernelVersion = Encoding.UTF8.GetString(payload[(position + 4)..(position + 4 + kernelVersionLength)]);
        position += 4 + kernelVersionLength;
        var iggyVersionLength = BinaryPrimitives.ReadInt32LittleEndian(payload[position..(position + 4)]);
        var iggyVersion = Encoding.UTF8.GetString(payload[(position + 4)..(position + 4 + iggyVersionLength)]);
        position += 4 + iggyVersionLength;
        var iggySemVersion = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        position += 4;

        var cacheMetricsLength = BinaryPrimitives.ReadInt32LittleEndian(payload[position..(position + 4)]);
        position += 4;

        var cacheMetricsList = new Dictionary<CacheMetricsKey, CacheMetrics>(cacheMetricsLength);
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
                HitRatio = BinaryPrimitives.ReadSingleLittleEndian(payload[(position + 28)..(position + 36)])
            };

            cacheMetricsList.Add(cacheMetricsKey, cacheMetrics);
        }

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
            CacheMetrics = cacheMetricsList
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
        var id = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        var partitionsCount = BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 4)..(position + 8)]);
        var partitions = new List<int>();
        for (var i = 0; i < partitionsCount; i++)
        {
            var partitionId
                = BinaryPrimitives.ReadInt32LittleEndian(payload[(position + 8 + i * 4)..(position + 8 + (i + 1) * 4)]);
            partitions.Add(partitionId);
        }

        return (new ConsumerGroupMember
        {
            Id = id,
            PartitionsCount = partitionsCount,
            Partitions = partitions
        },
            8 + partitionsCount * 4);
    }

    private static (ConsumerGroupResponse consumerGroup, int readBytes) MapToConsumerGroup(ReadOnlySpan<byte> payload,
        int position)
    {
        var id = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        var partitionsCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[(position + 4)..(position + 8)]);
        var membersCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[(position + 8)..(position + 12)]);
        var nameLength = payload[position + 12];
        var name = Encoding.UTF8.GetString(payload[(position + 13)..(position + 13 + nameLength)]);

        return (new ConsumerGroupResponse
        {
            Id = id,
            Name = name,
            MembersCount = membersCount,
            PartitionsCount = partitionsCount
        }, 13 + name.Length);
    }

    internal static ClusterMetadata MapClusterMetadata(ReadOnlySpan<byte> payload)
    {
        var nameLength = BinaryPrimitives.ReadUInt32LittleEndian(payload[..4]);
        var clusterName = Encoding.UTF8.GetString(payload[4..(4 + (int)nameLength)]);
        var position = 4 + (int)nameLength;

        var clusterId = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        position += 4;

        var protocol = (Protocol)payload[position];
        position += 1;

        var nodesCount = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        position += 4;

        var nodes = new ClusterNode[nodesCount];
        for (var i = 0; i < nodesCount; i++)
        {
            var node = MapClusterNode(payload[position..]);
            nodes[i] = node;
            position += node.GetSize();
        }

        return new ClusterMetadata
        {
            Id = clusterId,
            Name = clusterName,
            Transport = protocol,
            Nodes = nodes
        };
    }

    private static ClusterNode MapClusterNode(ReadOnlySpan<byte> payload)
    {
        var id = BinaryPrimitives.ReadUInt32LittleEndian(payload[..4]);
        var position = 4;

        var nameLength = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        position += 4;

        var name = Encoding.UTF8.GetString(payload[position..(position + (int)nameLength)]);
        position += (int)nameLength;

        var addressLength = BinaryPrimitives.ReadUInt32LittleEndian(payload[position..(position + 4)]);
        position += 4;

        var address = Encoding.UTF8.GetString(payload[position..(position + (int)addressLength)]);
        position += (int)addressLength;

        var role = (ClusterNodeRole)payload[position++];
        var status = (ClusterNodeStatus)payload[position];

        return new ClusterNode
        {
            Id = id,
            Name = name,
            Address = address,
            Role = role,
            Status = status
        };
    }
}
