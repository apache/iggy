using Apache.Iggy.Enums;

namespace Apache.Iggy.Tests.Integrations.Attributes;

internal class SkipTcpAttribute() : SkipAttribute("This test is skipped for TCP protocol")
{
    public override Task<bool> ShouldSkip(BeforeTestContext context)
    {
        foreach (var argument in context.TestDetails.TestClassArguments)
            if (argument is Protocol.Tcp)
                return Task.FromResult(true);

        return Task.FromResult(false);
    }
}