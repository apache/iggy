namespace Iggy_SDK.Exceptions;

public sealed class FeatureUnavailableException : Exception
{
    public FeatureUnavailableException() : base("This feature is not available.")
    {

    }
}