using BoDi;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Iggy_SDK;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Enums;
using Iggy_SDK.Factory;
using Iggy_SDK_Tests.Utils.SpecFlowTypes;
using Iggy_SDK_Tests.Utils.Streams;
using Iggy_SDK_Tests.Utils.Topics;
using Iggy_SDK.IggyClient;
using Microsoft.Extensions.Logging.Abstractions;
using TechTalk.SpecFlow;

namespace Iggy_SDK_Tests.BehaviorTests.ConsumerGroupPolling.Hooks;

[Binding]
public sealed class IggyDockerHooks
{
    // private static readonly IContainer _container = new ContainerBuilder().WithImage("iggyrs/iggy:latest")
    //     //.WithPortBinding(3000, true)
    //     .WithPortBinding(8090, true)
    //     .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(8090))
    //     //.WithPortBinding(8080, true)
    //     .Build();
    //
    // private static readonly StreamRequest _streamRequest = StreamFactory.CreateStreamRequest();
    // private static readonly TopicRequest _topicConsumersRequest = TopicFactory.CreateTopicRequest();
    // private static readonly TopicRequest _topicConsumerGroupRequest = TopicFactory.CreateTopicRequest();

    private readonly IObjectContainer _dependencyContainer;
    public IggyDockerHooks(IObjectContainer dependencyContainer)
    {
        _dependencyContainer = dependencyContainer;
    }

    [BeforeScenario]
    public async Task RegisterDependencies()
    {
        // var clients = new List<IIggyClient>();
        // var messageBus = MessageStreamFactory.CreateMessageStream(options =>
        // {
        //     options.BaseAdress = $"127.0.0.1:{_container.GetMappedPublicPort(8090)}";
        //     options.Protocol = Protocol.Tcp;
        //     options.SendBufferSize = 10000;
        //     options.ReceiveBufferSize = 10000;
        //     options.MessageBatchingSettings = x =>
        //     {
        //         x.Enabled = false;
        //         x.MaxMessagesPerBatch = 1000;
        //         x.Interval = TimeSpan.FromMilliseconds(50);
        //     };
        // }, NullLoggerFactory.Instance);
        // await messageBus.LoginUser(new LoginUserRequest
        // {
        //     Password = "iggy",
        //     Username = "iggy"
        // });
        //
        // for (int i = 0; i < 2; i++)
        // {
        //     var client = MessageStreamFactory.CreateMessageStream(options =>
        //     {
        //         options.BaseAdress = $"127.0.0.1:{_container.GetMappedPublicPort(8090)}";
        //         options.Protocol = Protocol.Tcp;
        //         options.SendBufferSize = 10000;
        //         options.ReceiveBufferSize = 10000;
        //         options.MessageBatchingSettings = x =>
        //         {
        //             x.Enabled = false;
        //             x.MaxMessagesPerBatch = 1000;
        //             x.Interval = TimeSpan.FromMilliseconds(50);
        //             x.MaxMessagesPerBatch = 8912;
        //         };
        //     }, NullLoggerFactory.Instance);
        //     await client.LoginUser(new LoginUserRequest
        //     {
        //         Password = "iggy",
        //         Username = "iggy"
        //     });
        //     clients.Add(client);
        // }
        //
        // _dependencyContainer.RegisterInstanceAs<IIggyClient>(messageBus);
        // _dependencyContainer.RegisterInstanceAs<List<IIggyClient>>(clients);
        // var listOfIds = new ConsumerPollStreamTopicId
        // {
        //     StreamId = (int)_streamRequest.StreamId!,
        //     ConsumerTopicId = (int)_topicConsumersRequest.TopicId!,
        //     ConsumerGroupTopicId = (int)_topicConsumerGroupRequest.TopicId!
        // };
        // _dependencyContainer.RegisterInstanceAs(listOfIds);

    }

    [BeforeTestRun]
    public static async Task InitialSetup()
    {
        // await _container.StartAsync();
        // var messageBus = MessageStreamFactory.CreateMessageStream(options =>
        // {
        //     options.BaseAdress = $"127.0.0.1:{_container.GetMappedPublicPort(8090)}";
        //     options.Protocol = Protocol.Tcp;
        //     options.SendBufferSize = 10000;
        //     options.ReceiveBufferSize = 10000;
        //     options.MessageBatchingSettings = x =>
        //     {
        //         x.Enabled = false;
        //         x.MaxMessagesPerBatch = 1000;
        //         x.Interval = TimeSpan.FromMilliseconds(100);
        //         x.MaxMessagesPerBatch = 8912;
        //     };
        // }, NullLoggerFactory.Instance);
        // await messageBus.LoginUser(new LoginUserRequest
        // {
        //     Password = "iggy",
        //     Username = "iggy"
        // });
        //
        // await messageBus.CreateStreamAsync(_streamRequest);
        // await messageBus.CreateTopicAsync(Identifier.Numeric((int)_streamRequest.StreamId!), _topicConsumersRequest);
        // await messageBus.CreateTopicAsync(Identifier.Numeric((int)_streamRequest.StreamId), _topicConsumerGroupRequest);

    }

    [AfterTestRun]
    public static async Task Teardown()
    {
        // await _container.StopAsync();
    }
}