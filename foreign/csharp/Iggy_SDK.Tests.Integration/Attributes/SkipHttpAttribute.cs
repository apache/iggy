using Apache.Iggy.Enums;

namespace Apache.Iggy.Tests.Integrations.Attributes;

internal class SkipHttpAttribute() : SkipAttribute("This test is skipped for HTTP protocol")
{
    public override Task<bool> ShouldSkip(BeforeTestContext context)
    {
        foreach (var argument in context.TestDetails.TestClassArguments)
            if (argument is Protocol.Http)
                return Task.FromResult(true);

        return Task.FromResult(false);
    }
}