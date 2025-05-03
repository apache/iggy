namespace Iggy_SDK.Exceptions;

internal sealed class TcpInvalidStatus : Exception
{
    internal TcpInvalidStatus() : base("Received an Invalid Response Status")
    {

    }
}