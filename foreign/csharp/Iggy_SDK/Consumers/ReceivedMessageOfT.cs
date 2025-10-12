namespace Apache.Iggy.Consumers;

public class ReceivedMessage<T> : ReceivedMessage
{
    public T? Data { get; init; }
}
