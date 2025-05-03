using Iggy_SDK.Errors;

namespace Iggy_SDK_Tests.Utils.Errors;

public static class ErrorModelFactory
{
    public static ErrorModel CreateErrorModelBadRequest()
    {
        return new ErrorModel(69, "bad_request", "Bad Request");
    }

    public static ErrorModel CreateErrorModelNotFound()
    {
        return new ErrorModel(69, "not_found", "Not Found");
    }
}