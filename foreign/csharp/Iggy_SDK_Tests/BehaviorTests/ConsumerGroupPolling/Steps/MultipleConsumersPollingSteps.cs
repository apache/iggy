using FluentAssertions;
using Iggy_SDK;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK_Tests.Utils.Messages;
using Iggy_SDK_Tests.Utils.SpecFlowTypes;
using Iggy_SDK.IggyClient;
using TechTalk.SpecFlow;

namespace Iggy_SDK_Tests.BehaviorTests.ConsumerGroupPolling.Steps;

//TODO: Cleanup this mess
//TODO: All code blocks were commented because the TCP implementation needs to be aligned with Iggyrs core changes
[Binding]
public sealed class MultipleConsumersPollingSteps
{
    private readonly ScenarioContext _scenarioContext;
    private readonly IIggyClient _messageStream;
    private readonly List<IIggyClient> _clients;
    private readonly ConsumerPollStreamTopicId _streamAndTopicFixture;

    private readonly int[] _consumersIds = [1, 2, 3, 4];
    private static readonly int _partitionId = 1;
    private static readonly int _consumerGroupId = 1;

    public MultipleConsumersPollingSteps(ScenarioContext scenarioContext, IIggyClient messageStream,
        List<IIggyClient> clients, ConsumerPollStreamTopicId streamAndTopicFixture)
    {
        _scenarioContext = scenarioContext;
        _messageStream = messageStream;
        _clients = clients;
        _streamAndTopicFixture = streamAndTopicFixture;
    }
    
    [Given(@"Messages are available in topic on single partition")]
    public async Task GivenMessagesAreAvailableOnConsumerTopic()
    {
        // var messages = MessageFactory.GenerateMessages(10);
        // var messageConsumersSendRequest =
        //     MessageFactory.CreateMessageSendRequest(_streamAndTopicFixture.StreamId!, _streamAndTopicFixture.ConsumerTopicId!,
        //         _partitionId, messages);
        //
        // await _messageStream.SendMessagesAsync(messageConsumersSendRequest);
        // Console.WriteLine();
    }

    [When(@"Consumers polls messages")]
    public async Task WhenConsumersPollMessages()
    {
        // var consumerPolledMessages = new List<PolledMessages>();
        // foreach (var consumerId in _consumersIds)
        // {
        //     var request = MessageFactory.CreateMessageFetchRequestConsumer(10, _streamAndTopicFixture.StreamId!,_streamAndTopicFixture.ConsumerTopicId!, _partitionId, consumerId);
        //     var result = await _messageStream.FetchMessagesAsync(request);
        //     consumerPolledMessages.Add(result);
        // }
        // _scenarioContext.Add("ConsumersPollResults", consumerPolledMessages);
    }

    [Then(@"Each consumer gets equal amount of messages")]
    public void ThenEachConsumerGetsEqualAmountOfMessages()
    {
        // var consumerPolledMessages = _scenarioContext.Get<List<PolledMessages>>("ConsumersPollResults");
        // foreach (var polledMessage in consumerPolledMessages)
        // {
        //     polledMessage.Messages.Count.Should().Be(10);
        // }
    }

    [Given(@"Messages are available in topic on multiple partitions")]
    public async Task GivenMessagesAreAvailableOnConsumergroupTopicAndConsumergroupExists()
    {
        // var messages = MessageFactory.GenerateMessages(10);
        // var messageConsumerGroupSendRequestPt1 =
        //     MessageFactory.CreateMessageSendRequest(_streamAndTopicFixture.StreamId!, _streamAndTopicFixture.ConsumerGroupTopicId!,
        //         _partitionId, messages);
        // var messageConsumerGroupSendRequestPt2 =
        //     MessageFactory.CreateMessageSendRequest(_streamAndTopicFixture.StreamId, _streamAndTopicFixture.ConsumerGroupTopicId,
        //         _partitionId + 1, messages);
        // var messageConsumerGroupSendRequestPt3 =
        //     MessageFactory.CreateMessageSendRequest(_streamAndTopicFixture.StreamId, _streamAndTopicFixture.ConsumerGroupTopicId,
        //         _partitionId + 2, messages);
        // var messageConsumerGroupSendRequestPt4 =
        //     MessageFactory.CreateMessageSendRequest(_streamAndTopicFixture.StreamId, _streamAndTopicFixture.ConsumerGroupTopicId,
        //         _partitionId + 3, messages);
        //
        // await _messageStream.CreateConsumerGroupAsync(new CreateConsumerGroupRequest
        // {
        //     Name = "test_cg",
        //     StreamId = Identifier.Numeric(_streamAndTopicFixture.StreamId),
        //     TopicId = Identifier.Numeric(_streamAndTopicFixture.ConsumerGroupTopicId),
        //     ConsumerGroupId = _consumerGroupId
        // });
        // await _messageStream.SendMessagesAsync(messageConsumerGroupSendRequestPt1);
        // await _messageStream.SendMessagesAsync(messageConsumerGroupSendRequestPt2);
        // await _messageStream.SendMessagesAsync(messageConsumerGroupSendRequestPt3);
        // await _messageStream.SendMessagesAsync(messageConsumerGroupSendRequestPt4);
        // //waiting for the message dispatcher to batch the messages and send them to server
        // await Task.Delay(1500);
    }

    [When(@"Consumer group polls messages")]
    public async Task WhenConsumerGroupPollMessages()
    {
        // var consumerPolledMessages = new List<PolledMessages>();
        // foreach (var client in _clients)
        // {
        //     await client.JoinConsumerGroupAsync(new JoinConsumerGroupRequest
        //     {
        //         StreamId = Identifier.Numeric(_streamAndTopicFixture.StreamId),
        //         TopicId = Identifier.Numeric(_streamAndTopicFixture.ConsumerGroupTopicId),
        //         ConsumerGroupId = Identifier.Numeric(_consumerGroupId)
        //     });
        // }
        //
        // foreach (var client in _clients)
        // {
        //     for (int i = 0; i < 2; i++)
        //     {
        //         var request = MessageFactory.CreateMessageFetchRequestConsumerGroup(10, _streamAndTopicFixture.StreamId, _streamAndTopicFixture.ConsumerGroupTopicId, 0, _consumerGroupId);
        //         var result = await client.FetchMessagesAsync(request);
        //         consumerPolledMessages.Add(result);
        //     }
        // }
        //
        // _scenarioContext.Add("ConsumerGroupPollResults", consumerPolledMessages);
    }

    [Then(@"Each consumer gets messages from server-side calculated partitions")]
    public async Task ThenEachConsumerGetsSameAmountOfMessages()
    {
        // var consumerPolledMessages = _scenarioContext.Get<List<PolledMessages>>("ConsumerGroupPollResults");
        //
        // foreach (var polledMessage in consumerPolledMessages)
        // {
        //     polledMessage.Messages.Count.Should().Be(10);
        // }
        //
        // await _messageStream.DeleteConsumerGroupAsync(new DeleteConsumerGroupRequest
        // {
        //     StreamId = Identifier.Numeric(_streamAndTopicFixture.StreamId),
        //     TopicId = Identifier.Numeric(_streamAndTopicFixture.ConsumerGroupTopicId),
        //     ConsumerGroupId = Identifier.Numeric(_consumerGroupId)
        // });
    }
}