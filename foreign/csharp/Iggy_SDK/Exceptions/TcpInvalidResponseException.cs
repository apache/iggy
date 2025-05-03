namespace Iggy_SDK.Exceptions;

public sealed class TcpInvalidResponseException : Exception
{

    public TcpInvalidResponseException() : base("Received an Invalid Response")
    {

    }
}