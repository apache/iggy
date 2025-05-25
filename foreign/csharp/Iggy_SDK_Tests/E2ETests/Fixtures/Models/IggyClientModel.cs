using Iggy_SDK.IggyClient;

namespace Iggy_SDK_Tests.E2ETests.Fixtures.Models;

public class IggyClientModel
{
    public string Name { get; set; }
    public IIggyClient Client { get; set; }
    public int HttpPort { get; set; }
    public int TcpPort { get; set; }
}