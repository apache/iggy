namespace Iggy_SDK.Exceptions;

internal sealed class InvalidBaseAdressException : Exception
{

    internal InvalidBaseAdressException() : base("Invalid Base Adress, use ':' only to describe the port")
    {

    }
}