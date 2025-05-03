using Iggy_SDK.Extensions;
using System.Text.Json;

namespace Iggy_SDK.JsonConfiguration;

internal sealed class ToSnakeCaseNamingPolicy : JsonNamingPolicy
{
    public override string ConvertName(string name) => name.ToSnakeCase();
}