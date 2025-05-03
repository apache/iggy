using FluentAssertions;
using Iggy_SDK_Tests.E2ETests.Fixtures;
using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;
using Iggy_SDK_Tests.Utils;

namespace Iggy_SDK_Tests.E2ETests;

//TODO(numinex): The clients query doesn't work for http in this test case, but works in general, figure that shit out.
[TestCaseOrderer("Iggy_SDK_Tests.Utils.PriorityOrderer", "Iggy_SDK_Tests")]
public sealed class ClientsE2E : IClassFixture<IggyClientsFixture>
{
    private const string SkipMessage = "TCP implementation needs to be aligned with Iggyrs core changes";
    
    private const int sutByTcp = 0;
    
    private readonly IggyClientsFixture _fixture;

    public ClientsE2E(IggyClientsFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact(Skip = SkipMessage), TestPriority(1)]
    public async Task GetClients_Should_Return_CorrectClientsCount()
    {
        var sut = _fixture.SubjectsUnderTest[sutByTcp];
        var clients = await sut.GetClientsAsync();
        clients.Count.Should().Be(ClientsFixtureBootstrap.TotalClientsCount);
    }
        
    [Fact(Skip = SkipMessage), TestPriority(2)]
    public async Task GetClient_Should_Return_CorrectClient()
    {
        var sut = _fixture.SubjectsUnderTest[sutByTcp];
        var clients = await sut.GetClientsAsync();
        clients.Count.Should().Be(ClientsFixtureBootstrap.TotalClientsCount);
        uint id = clients[0].ClientId;
        var response = await sut.GetClientByIdAsync(id);
        response!.ClientId.Should().Be(id);
    }
    
    // [Fact, TestPriority(3)]
    // public async Task HTTPGetClient_Should_Return_CorrectClient()
    // {
    //     var sut = _fixture.SubjectsUnderTest[sutByHttp];
    //     var clients = await sut.GetClientsAsync();
    //     clients.Count.Should().Be(ClientsFixtureBootstrap.TotalClientsCount);
    //     uint id = clients[0].ClientId;
    //     var response = await sut.GetClientByIdAsync(id);
    //     response!.ClientId.Should().Be(id);
    // }
}