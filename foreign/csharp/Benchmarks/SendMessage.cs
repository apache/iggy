using Iggy_SDK;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.IggyClient;
using Iggy_SDK.Kinds;
using Iggy_SDK.Messages;
using System.Diagnostics;
using System.Text;

namespace Benchmarks;


public static class SendMessage
{
    public static async Task Create(IIggyClient bus, int producerNumber, int producerCount,
        int messagesBatch, int messagesCount, int messageSize, Identifier streamId, Identifier topicId)
    {
        long totalMessages = messagesBatch * messagesCount;
        long totalMessagesBytes = totalMessages * messageSize;
        Console.WriteLine(
            $"Executing Send Messages command for producer {producerNumber}, stream id {streamId}, messages count {totalMessages}, with size {totalMessagesBytes}");
        Message[] messages = CreateMessages(messagesCount, messageSize);
        List<TimeSpan> latencies = new();

        for (int i = 0; i < messagesBatch; i++)
        {
            var startTime = Stopwatch.GetTimestamp();
            await bus.SendMessagesAsync(new MessageSendRequest
            {
                StreamId = streamId,
                TopicId = topicId,
                Partitioning = Partitioning.PartitionId(1),
                Messages = messages,
            });
            var diff = Stopwatch.GetElapsedTime(startTime);
            latencies.Add(diff);
        }

        var totalLatencies = latencies.Sum(x => x.TotalSeconds);
        var avgLatency = Math.Round(latencies.Sum(x => x.TotalMilliseconds) / (producerCount * latencies.Count), 2);
        var duration = totalLatencies / producerCount;
        var avgThroughput = Math.Round(totalMessagesBytes / duration / 1024.0 / 1024.0, 2);

        Console.WriteLine($"Total message bytes: {totalMessagesBytes}, average latency: {avgLatency} ms.");
        Console.WriteLine(
            $"Producer number: {producerNumber} send Messages: {messagesCount} in {messagesBatch} batches, with average throughput {avgThroughput} MB/s");
    }

    private static Message[] CreateMessages(int messagesCount, int messageSize)
    {
        var messages = new Message[messagesCount];
        for (int i = 0; i < messagesCount; i++)
        {
            messages[i] = new Message
            {
                Id = Guid.NewGuid(),
                Payload = CreatePayload(messageSize)
            };
        }

        return messages;
    }

    private static byte[] CreatePayload(int size)
    {
        StringBuilder payloadBuilder = new StringBuilder(size);
        for (uint i = 0; i < size; i++)
        {
            char character = (char)((i % 26) + 97);
            payloadBuilder.Append(character);
        }

        string payloadString = payloadBuilder.ToString();
        return Encoding.UTF8.GetBytes(payloadString);
    }
}