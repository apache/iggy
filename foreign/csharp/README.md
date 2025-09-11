# C# SDK for [Iggy](https://github.com/apache/iggy)

<div align="center">

[![Nuget (with prereleases)](https://img.shields.io/nuget/v/Apache.Iggy)](https://www.nuget.org/packages/Apache.Iggy)

</div>

## Getting Started

Currently supported transfer protocols

- TCP
- HTTP

The whole SDK revolves around `IIggyClient` interface to create an instance of it, use following code

```c#
var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .AddFilter("Iggy_SDK.MessageStream.Implementations;", LogLevel.Trace)
        .AddConsole();
});
var bus = MessageStreamFactory.CreateMessageStream(options =>
{
    options.BaseAdress = "127.0.0.1:8090";
    options.Protocol = Protocol.Tcp;
    options.TlsSettings = x =>
    {
        x.Enabled = false;
        x.Hostname = "iggy";
        x.Authenticate = false;
    };
}, loggerFactory);
```

Iggy necessitates the use of `ILoggerFactory` to generate logs from locations that are inaccessible to the user.

In addition to the basic configuration settings, Iggy provides support for batching send/poll messages at intervals,
which effectively decreases the frequency of network calls, this option is enabled by default.

```c#
//---Snip---
var bus = MessageStreamFactory.CreateMessageStream(options =>
{
    options.BaseAdress = "127.0.0.1:8090";
    options.Protocol = protocol;
    options.TlsSettings = x =>
    {
        x.Enabled = false;
        x.Hostname = "iggy";
        x.Authenticate = false;
    };

    options.IntervalBatchingConfig = x =>
    {
        x.Enabled = true;
        x.Interval = TimeSpan.FromMilliseconds(100);
        x.MaxMessagesPerBatch = 1000;
        x.MaxRequests = 4096;
    };
    options.MessagePollingSettings = x =>
    {
        x.Interval = TimeSpan.FromMilliseconds(100);
        x.StoreOffsetStrategy = StoreOffset.AfterProcessingEachMessage;
    };
}, loggerFactory);
```

### Creating and logging in a user

To begin, utilize the root account (note that the root account cannot be removed or updated).

```c#
var response = await bus.LoginUser(new LoginUserRequest
{
    Username = "iggy",
    Password = "iggy",
});
```

Furthermore, after logging in, you have the option to create an account with customizable `Permissions`.

```c#
//---Snip---
await bus.CreateUser(new CreateUserRequest
{
    Username = "test_user",
    Password = "pa55w0rD!@",
    Status = UserStatus.Active,
    Permissions = new Permissions
    {
        Global = new GlobalPermissions
        {
            ManageServers = true,
            ManageUsers = true,
            ManageStreams = true,
            ManageTopics = true,
            PollMessages = true,
            ReadServers = true,
            ReadStreams = true,
            ReadTopics = true,
            ReadUsers = true,
            SendMessages = true
        },
        Streams = new Dictionary<int, StreamPermissions>
        {
            {
                streamId, new StreamPermissions
                {
                    ManageStream = true,
                    ReadStream = true,
                    SendMessages = true,
                    PollMessages = true,
                    ManageTopics = true,
                    ReadTopics = true,
                    Topics = new Dictionary<int, TopicPermissions>
                    {
                        {
                            topicId, new TopicPermissions
                            {
                                ManageTopic = true,
                                ReadTopic = true,
                                PollMessages = true,
                                SendMessages = true
                            }
                        }
                    }
                }
            }
        }
    }
});

var response = await bus.LoginUser(new LoginUserRequest
{
    Username = "test_user",
    Password = "pa55w0rD!@",
});
```

Alternatively, once you've logged in, you can create a `Personal Access Token` that can be reused for further logins.

```c#
var response = await bus.LoginUser(new LoginUserRequest
{
    Username = "your_username",
    Password = "your_password",
});

var patResponse = await bus.CreatePersonalAccessTokenAsync(new CreatePersonalAccessTokenRequest
{
    Name = "first-pat",
    Expiry = 60, // seconds from creation time
});
await bus.LoginWithPersonalAccessToken(new LoginWithPersonalAccessToken
{
    Token = patResponse.Token
});
```

### Creating first stream and topic

In order to create stream use `CreateStreamAsync` method.

```c#
await bus.CreateStreamAsync(new StreamRequest
{
    StreamId = 1,
    Name = "first-stream",
});
```

Every stream has a topic to which you can broadcast messages, for the purpose of create one
use `CreateTopicAsync` method.

```c#
var streamId = Identifier.Numeric(1);
await bus.CreateTopicAsync(streamId, new TopicRequest
{
    Name = "first-topic",
    PartitionsCount = 3,
    TopicId = 1
});
```

Notice that both Stream aswell as Topic use `-` instead of space in its name, Iggy will replace any spaces in
name with `-` instead, so keep that in mind.

### Sending messages

To send messages you can use `SendMessagesAsync` method.

```c#
Func<byte[], byte[]> encryptor = static payload =>
{
    string aes_key = "AXe8YwuIn1zxt3FPWTZFlAa14EHdPAdN9FaZ9RQWihc=";
    string aes_iv = "bsxnWolsAyO7kCfWuyrnqg==";

    var key = Convert.FromBase64String(aes_key);
    var iv = Convert.FromBase64String(aes_iv);

    using Aes aes = Aes.Create();
    ICryptoTransform encryptor = aes.CreateEncryptor(key, iv);

    using MemoryStream memoryStream = new MemoryStream();
    using CryptoStream cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write);
    using BinaryWriter streamWriter = new BinaryWriter(cryptoStream);
    streamWriter.Write(payload);

    return memoryStream.ToArray();
};

var messages = new List<Message>(); // your messages
var streamId = Identifier.Numeric(1);
var topicId = Identifier.Numeric(1);
await bus.SendMessagesAsync(new MessageSendRequest
{
    Messages = new List<Message>(),
    Partitioning = Partitioning.PartitionId(1),
    StreamId = streamId,
    TopicId = topicId,
}, encryptor); //encryptor is optional
```

The `Message` struct has two fields `Id` and `Payload`.

```c#
struct Message
{
    public required MessageHeader Header { get; init; }
    public required byte[] Payload { get; init; }
    public Dictionary<HeaderKey, HeaderValue>? UserHeaders { get; init; }
}

public readonly struct MessageHeader
{
    public ulong Checksum { get; init; }
    public UInt128 Id { get; init; }
    public ulong Offset { get; init; }
    public DateTimeOffset Timestamp { get; init; }
    public ulong OriginTimestamp { get; init; }
    public int UserHeadersLength { get; init; }
    public int PayloadLength { get; init; }
}
```

Furthermore, there's a generic overload for this method that takes binary serializer as argument.

```c#
//---Snip---
Func<Envelope, byte[]> serializer = static envelope =>
{
    Span<byte> buffer = stackalloc byte[envelope.MessageType.Length + 4 + envelope.Payload.Length];

    BinaryPrimitives.WriteInt32LittleEndian(buffer[..4], envelope.MessageType.Length);
    Encoding.UTF8.GetBytes(envelope.MessageType).CopyTo(buffer[4..(envelope.MessageType.Length + 4)]);
    Encoding.UTF8.GetBytes(envelope.Payload).CopyTo(buffer[(envelope.MessageType.Length + 4)..]);

    return buffer.ToArray();
};

var messages = new List<Envelope>(); // your messages
await bus.SendMessagesAsync(new MessageSendRequest<Envelope>
{
    StreamId = streamId,
    TopicId = topicId,
    Partitioning = Partitioning.PartitionId(1),
    Messages = messages
},
serializer,
encryptor);
```

Both generic and non generic method accept optional `Headers` dictionary.

```c#
//---Snip---
var headers = new Dictionary<HeaderKey, HeaderValue>
{
    { new HeaderKey { Value = "key_1".ToLower() }, HeaderValue.FromString("test-value-1") },
    { new HeaderKey { Value = "key_2".ToLower() }, HeaderValue.FromInt32(69) },
    { new HeaderKey { Value = "key_3".ToLower() }, HeaderValue.FromFloat(420.69f) },
    { new HeaderKey { Value = "key_4".ToLower() }, HeaderValue.FromBool(true) },
    { new HeaderKey { Value = "key_5".ToLower() }, HeaderValue.FromBytes(byteArray) },
    { new HeaderKey { Value = "key_6".ToLower() }, HeaderValue.FromInt128(new Int128(6969696969, 420420420)) },
    { new HeaderKey { Value = "key7".ToLower() }, HeaderValue.FromGuid(Guid.NewGuid()) }
};

await bus.SendMessagesAsync<Envelope>(new MessageSendRequest<Envelope>
{
    StreamId = streamId,
    TopicId = topicId,
    Partitioning = Partitioning.PartitionId(1),
    Messages = messages
},
serializer,
encryptor,
headers);
```

### Fetching Messages

Fetching messages is done with `FetchMessagesAsync`.

```c#
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

var messages = await bus.FetchMessagesAsync(new MessageFetchRequest
{
    StreamId = streamId,
    TopicId = topicId,
    Consumer = Consumer.New(1),
    Count = 1,
    PartitionId = 1,
    PollingStrategy = PollingStrategy.Next(),
    AutoCommit = true
},
decryptor);
```

Similarly, as with `SendMessagesAsync`, there's a generic overload that accepts a binary deserializer.

```c#
//---Snip---
Func<byte[], Envelope> deserializer = serializedData =>
{
    Envelope envelope = new Envelope();
    int messageTypeLength = BitConverter.ToInt32(serializedData, 0);
    envelope.MessageType = Encoding.UTF8.GetString(serializedData, 4, messageTypeLength);
    envelope.Payload = Encoding.UTF8.GetString(serializedData, 4 + messageTypeLength, serializedData.Length - (4 + messageTypeLength));
    return envelope;
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
```

Beyond the `FetchMessagesAsync` functionality, there's also a `PollMessagesAsync` method that spawns new thread which
polls messages in background.

```c#
//---Snip---
await foreach (var messageResponse in bus.PollMessagesAsync<Envelope>(new PollMessagesRequest
{
    Consumer = Consumer.New(consumerId),
    Count = 1,
    TopicId = topicId,
    StreamId = streamId,
    PartitionId = 1,
    PollingStrategy = PollingStrategy.Next(),
}, deserializer, decryptor))
{
    //handle the message response
}

```

It is worth noting that every method (except `PollMessagesAsync`) will throw an `InvalidResponseException` when
encountering an error.

If you register `IIggyClient` in a dependency injection container, you will have access to interfaces
that encapsulate smaller parts of the system `IIggyStream` `IIggyTopic` `IIggyPublisher` `IIggyConsumer`
`IIggyConsumerGroup` `IIggyOffset`
`IIggyPartition` `IIggyUsers` `IIggyUtils`

For more information about how Iggy works check its [documentation](https://iggy.apache.org/docs/)

## Producer / Consumer Sample

To run the samples, first get [Iggy](https://github.com/apache/iggy), Run the server with `cargo run --bin iggy-server`,
then get the SDK, cd into `Iggy_SDK`
and run following commands: `dotnet run -c Release --project Iggy_Sample_Producer` for producer,
`dotnet run -c Release --project Iggy_Sample_Consumer`
for consumer.

## Integration Tests

Integration tests are located in `Iggy_SDK/Iggy_Sample_Producer/IntegrationTests` folder.
Tests can be run against a dockerized Iggy server with TestContainers or local Iggy server.
To run with a local Iggy server, the environment variable `IGGY_SERVER_HOST` needs to be set.

## TODO

- Add support for `ASP.NET Core` Dependency Injection
