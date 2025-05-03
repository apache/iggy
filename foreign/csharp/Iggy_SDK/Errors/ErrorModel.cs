namespace Iggy_SDK.Errors;

public sealed class ErrorModel
{
    public int Id { get; init; }
    public string Code { get; init; }
    public string Reason { get; init; }

    internal ErrorModel(int id, string code, string reason)
    {
        Id = id;
        Code = code;
        Reason = reason;
    }

}