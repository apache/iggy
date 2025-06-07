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

using Iggy_SDK;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Contracts.Http.Auth;
using Iggy_SDK.Enums;
using Iggy_SDK.Factory;
using Iggy_SDK.JsonConfiguration;
using Iggy_SDK.Kinds;
using Microsoft.Extensions.Logging;
using Shared;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

var jsonOptions = new JsonSerializerOptions();
jsonOptions.PropertyNamingPolicy = new ToSnakeCaseNamingPolicy();
jsonOptions.WriteIndented = true;
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
        x.Interval = TimeSpan.FromMilliseconds(100);
        x.MaxMessagesPerBatch = 1000;
        x.MaxRequests = 4096;
    };
    options.MessagePollingSettings = x =>
    {
        x.Interval = TimeSpan.FromMilliseconds(100);
        x.StoreOffsetStrategy = StoreOffset.AfterProcessingEachMessage;
    };
    options.TlsSettings = x =>
    {
        x.Enabled = false;
        x.Hostname = "iggy";
        x.Authenticate = false;
    };
}, loggerFactory);

var response = await bus.LoginUser(new LoginUserRequest
{
    Password = "iggy",
    Username = "iggy",
});

Console.WriteLine("Using protocol : {0}", protocol.ToString());
int streamIdVal = 1;
int topicIdVal = 1;
var streamId = Identifier.Numeric(streamIdVal);
var topicId = Identifier.Numeric(topicIdVal);
var partitionId = 3;
var consumerId = 1;


Console.WriteLine($"Consumer has started, selected protocol {protocol}");

await ValidateSystem(streamId, topicId, partitionId);
await ConsumeMessages();

async Task ConsumeMessages()
{
    int intervalInMs = 1000;
    Console.WriteLine($"Messages will be polled from stream {streamId}, topic {topicId}, partition {partitionId} with interval {intervalInMs} ms");
    Func<byte[], Envelope> deserializer = serializedData =>
    {
        Envelope envelope = new Envelope();
        int messageTypeLength = BitConverter.ToInt32(serializedData, 0);
        envelope.MessageType = Encoding.UTF8.GetString(serializedData, 4, messageTypeLength);
        envelope.Payload = Encoding.UTF8.GetString(serializedData, 4 + messageTypeLength, serializedData.Length - (4 + messageTypeLength));
        return envelope;
    };
    Func<byte[], byte[]> decryptor = static payload =>
    {
        string aes_key = "AXe8YwuIn1zxt3FPWTZFlAa14EHdPAdN9FaZ9RQWihc=";
        string aes_iv = "bsxnWolsAyO7kCfWuyrnqg==";
        var key = Convert.FromBase64String(aes_key);
        var iv = Convert.FromBase64String(aes_iv);
        
        using Aes aes = Aes.Create();
        ICryptoTransform decryptor = aes.CreateDecryptor(key, iv);
        using MemoryStream memoryStream = new MemoryStream(payload);
        using CryptoStream cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read);
        using BinaryReader binaryReader = new BinaryReader(cryptoStream);
        return binaryReader.ReadBytes(payload.Length);
    };
    
    var messages = await bus.FetchMessagesAsync<Envelope>(new MessageFetchRequest
    {
        StreamId = streamId,
        TopicId = topicId,
        Consumer = Consumer.New(1),
        Count = 1,
        PartitionId = 1,
        PollingStrategy = PollingStrategy.Next(),
        AutoCommit = true
    }, deserializer, decryptor);
    await foreach (var msgResponse in bus.PollMessagesAsync<Envelope>(new PollMessagesRequest
                   {
                       Consumer = Consumer.New(consumerId),
                       Count = 1,
                       TopicId = topicId,
                       StreamId = streamId,
                       PartitionId = partitionId,
                       PollingStrategy = PollingStrategy.Next(),
                   }, deserializer, decryptor))
    {
        HandleMessage(msgResponse);
    }
}

void HandleMessage(MessageResponse<Envelope> messageResponse)
{
    Console.Write(
        $"Handling message type: {messageResponse.Message.MessageType} with checksum: {messageResponse.Header.Checksum}, at offset: {messageResponse.Header.Offset} with message Id:{messageResponse.Header.Id.ToString()} ");
    Console.WriteLine();
    Console.WriteLine("---------------------------MESSAGE-----------------------------------");
    Console.WriteLine();

    switch (messageResponse.Message.MessageType)
    {
        case "order_created":
            {
                var orderCreated = JsonSerializer.Deserialize<OrderCreated>(messageResponse.Message.Payload, jsonOptions);
                Console.WriteLine(orderCreated);
                break;
            }
        case "order_confirmed":
            {
                var orderConfirmed =
                    JsonSerializer.Deserialize<OrderConfirmed>(messageResponse.Message.Payload, jsonOptions);
                Console.WriteLine(orderConfirmed);
                break;
            }
        case "order_rejected":
            {
                var orderRejected = JsonSerializer.Deserialize<OrderRejected>(messageResponse.Message.Payload, jsonOptions);
                Console.WriteLine(orderRejected);
                break;
            }
    }


    if (messageResponse.UserHeaders is not null)
    {
        Console.WriteLine();
        Console.WriteLine("---------------------------HEADERS-----------------------------------");
        Console.WriteLine();
        foreach (var (headerKey, headerValue) in messageResponse.UserHeaders)
        {
            Console.WriteLine("Found Header: {0} with value: {1}, ", headerKey.ToString(), headerValue.ToString());
                }
        Console.WriteLine();
    }
    //await Task.Delay(1000);
}


async Task ValidateSystem(Identifier streamId, Identifier topicId, int partitionId)
{
    try
    {
        Console.WriteLine($"Validating if stream exists.. {streamId}");
        
        var result = await bus.GetStreamByIdAsync(streamId);
        
        Console.WriteLine(result.Name);
        
        Console.WriteLine($"Validating if topic exists.. {topicId}");
        
        var topicResult = await bus.GetTopicByIdAsync(streamId, topicId);
        
        if (topicResult!.PartitionsCount < partitionId)
        {
            throw new SystemException(
                $"Topic {topicId} has only {topicResult.PartitionsCount} partitions, but partition {partitionId} was requested");
        }
    }
    catch
    {
        Console.WriteLine($"Creating stream with {streamId}");
        
        await bus.CreateStreamAsync(new StreamRequest
        {
            StreamId = streamIdVal,
            Name = "Test Consumer Stream",
        });
        
        Console.WriteLine($"Creating topic with {topicId}");
        
        await bus.CreateTopicAsync(streamId, new TopicRequest(
            TopicId: topicIdVal,
            Name: "Test Consumer Topic",
            CompressionAlgorithm: CompressionAlgorithm.None,
            MessageExpiry: 0,
            MaxTopicSize: 1_000_000_000,
            ReplicationFactor: 3,
            PartitionsCount: 3));
        
        var topicRes = await bus.GetTopicByIdAsync(streamId, topicId);
        
        if (topicRes!.PartitionsCount < partitionId)
        {
            throw new SystemException(
                $"Topic {topicId} has only {topicRes.PartitionsCount} partitions, but partition {partitionId} was requested");
        }
    }

}