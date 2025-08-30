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
using System.Security.Cryptography;
using System.Text;
using Apache.Iggy;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Factory;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Apache.Iggy.Producer;
using Apache.Iggy.Shared;
using Microsoft.Extensions.Logging;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

var protocol = Protocol.Tcp;
var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .AddFilter("Iggy_SDK.IggyClient.Implementations;", LogLevel.Trace)
        .AddConsole();
});
var bus = MessageStreamFactory.CreateMessageStream(options =>
{
    options.BaseAdress = "127.0.0.1:8090";
    options.Protocol = protocol;
    options.MessageBatchingSettings = x =>
    {
        x.Enabled = false;
        x.Interval = TimeSpan.FromMilliseconds(101);
        x.MaxMessagesPerBatch = 1000;
        x.MaxRequests = 4096;
    };
    options.TlsSettings = x =>
    {
        x.Enabled = false;
        x.Hostname = "iggy";
        x.Authenticate = false;
    };
}, loggerFactory);

try
{
    var response = await bus.LoginUser("iggy", "iggy");
}
catch
{
    await bus.CreateUser("test_user", "pa55w0rD!@", UserStatus.Active);

    var response = await bus.LoginUser("iggy", "iggy");
}

Console.WriteLine("Using protocol : {0}", protocol.ToString());

var streamIdVal = 1u;
var topicIdVal = 1u;
var streamId = Identifier.Numeric(streamIdVal);
var topicId = Identifier.Numeric(topicIdVal);

Console.WriteLine($"Producer has started, selected protocol {protocol.ToString()}");

try
{
    var stream = await bus.GetStreamByIdAsync(streamId);
    var topic = await bus.GetTopicByIdAsync(streamId, topicId);
}
catch
{
    Console.WriteLine($"Creating stream with id:{streamId}");
    await bus.CreateStreamAsync("producer-stream", streamIdVal);

    Console.WriteLine($"Creating topic with id:{topicId}");
    await bus.CreateTopicAsync(streamId,
        topicId: topicIdVal,
        name: "producer-topic",
        compressionAlgorithm: CompressionAlgorithm.None,
        messageExpiry: 0,
        maxTopicSize: 0,
        replicationFactor: 3,
        partitionsCount: 3);
}

var actualStream = await bus.GetStreamByIdAsync(streamId);
var actualTopic = await bus.GetTopicByIdAsync(streamId, topicId);

await ProduceMessages(bus, actualStream, actualTopic);

async Task ProduceMessages(IIggyClient bus, StreamResponse? stream, TopicResponse? topic)
{
    var messageBatchCount = 1;
    var intervalInMs = 1000;
    Console.WriteLine(
        $"Messages will be sent to stream {stream!.Id}, topic {topic!.Id}, partition {topic.PartitionsCount} with interval {intervalInMs} ms");
    Func<Envelope, byte[]> serializer = static envelope =>
    {
        Span<byte> buffer = stackalloc byte[envelope.MessageType.Length + 4 + envelope.Payload.Length];
        BinaryPrimitives.WriteInt32LittleEndian(buffer[..4], envelope.MessageType.Length);
        Encoding.UTF8.GetBytes(envelope.MessageType).CopyTo(buffer[4..(envelope.MessageType.Length + 4)]);
        Encoding.UTF8.GetBytes(envelope.Payload).CopyTo(buffer[(envelope.MessageType.Length + 4)..]);
        return buffer.ToArray();
    };
    //can this be optimized ? this lambda doesn't seem to get cached
    Func<byte[], byte[]> encryptor = static payload =>
    {
        var aes_key = "AXe8YwuIn1zxt3FPWTZFlAa14EHdPAdN9FaZ9RQWihc=";
        var aes_iv = "bsxnWolsAyO7kCfWuyrnqg==";
        var key = Convert.FromBase64String(aes_key);
        var iv = Convert.FromBase64String(aes_iv);

        using var aes = Aes.Create();
        var encryptor = aes.CreateEncryptor(key, iv);
        using var memoryStream = new MemoryStream();
        using var cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write);
        using (var streamWriter = new BinaryWriter(cryptoStream))
        {
            streamWriter.Write(payload);
        }

        return memoryStream.ToArray();
    };

    var byteArray = new byte[] { 6, 9, 4, 2, 0 };


    var headers = new Dictionary<HeaderKey, HeaderValue>();
    headers.Add(new HeaderKey { Value = "key_1".ToLower() }, HeaderValue.FromString("test-value-1"));
    headers.Add(new HeaderKey { Value = "key_2".ToLower() }, HeaderValue.FromInt32(69));
    headers.Add(new HeaderKey { Value = "key_3".ToLower() }, HeaderValue.FromFloat(420.69f));
    headers.Add(new HeaderKey { Value = "key_4".ToLower() }, HeaderValue.FromBool(true));
    headers.Add(new HeaderKey { Value = "key_5".ToLower() }, HeaderValue.FromBytes(byteArray));
    headers.Add(new HeaderKey { Value = "key_6".ToLower() }, HeaderValue.FromInt128(new Int128(6969696969, 420420420)));
    headers.Add(new HeaderKey { Value = "key_7".ToLower() }, HeaderValue.FromGuid(Guid.NewGuid()));

    while (true)
    {
        var debugMessages = new List<ISerializableMessage>();
        var messages = new Envelope[messageBatchCount];

        for (var i = 0; i < messageBatchCount; i++)
        {
            var message = MessageGenerator.GenerateMessage();
            var envelope = message.ToEnvelope();

            debugMessages.Add(message);
            messages[i] = envelope;
        }

        var messagesSerialized = new List<Message>();
        foreach (var message in messages)
        {
            messagesSerialized.Add(new Message(Guid.NewGuid(), encryptor(serializer(message)), headers));
        }

        try
        {
            await bus.SendMessagesAsync(new MessageSendRequest<Envelope>
                {
                    StreamId = streamId,
                    TopicId = topicId,
                    Partitioning = Partitioning.PartitionId(3),
                    Messages = messages
                },
                serializer,
                encryptor, headers);
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
            throw;
        }

        Console.WriteLine(
            $"Sent messages: {string.Join(Environment.NewLine, debugMessages.ConvertAll(m => m.ToString()))}");
        await Task.Delay(intervalInMs);
    }
}


namespace Apache.Iggy.Producer
{
    public static class EncryptorData
    {
        private static readonly byte[] key =
        {
            0x2b, 0x7e, 0x15, 0x16, 0x28, 0xae, 0xd2, 0xa6, 0xab, 0xf7,
            0x15, 0x88, 0x09, 0xcf, 0x4f, 0x3c, 0xa8, 0x8d, 0x2d, 0x0a,
            0x9f, 0x9d, 0xea, 0x43, 0x6c, 0x25, 0x17, 0x13, 0x20, 0x45,
            0x78, 0xc8
        };

        private static readonly byte[] iv =
        {
            0x5f, 0x8a, 0xe4, 0x78, 0x9c, 0x3d, 0x2b, 0x0f, 0x12, 0x6a,
            0x7e, 0x45, 0x91, 0xba, 0xdf, 0x33
        };

        public static byte[] GetKey()
        {
            return key;
        }

        public static byte[] GetIv()
        {
            return iv;
        }
    }
}