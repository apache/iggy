namespace Apache.Iggy.Exceptions;

public class InvalidConsumerGroupNameException : Exception
{
    public InvalidConsumerGroupNameException(string message) : base(message)
    {
    }
}