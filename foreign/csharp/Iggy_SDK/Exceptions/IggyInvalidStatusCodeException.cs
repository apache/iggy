namespace Apache.Iggy.Exceptions;

public sealed class IggyInvalidStatusCodeException : Exception
{
    public int StatusCode { get; init; }
    
    internal IggyInvalidStatusCodeException(int statusCode, string message) : base(message)
    {
        StatusCode = statusCode;
    }
}