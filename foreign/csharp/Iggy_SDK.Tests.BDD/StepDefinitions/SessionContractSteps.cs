// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using Apache.Iggy.Configuration;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Factory;
using Reqnroll;
using Shouldly;
using TestContext = Apache.Iggy.Tests.BDD.Context.TestContext;
using TestEnvironment = Apache.Iggy.Tests.BDD.Context.TestEnvironment;

namespace Apache.Iggy.Tests.BDD.StepDefinitions;

[Binding]
public class SessionContractSteps
{
    private readonly TestContext _context;

    public SessionContractSteps(TestContext context)
    {
        _context = context;
    }

    [Given(@"I have a cluster node that cannot reach quorum")]
    public async Task GivenIHaveAClusterNodeThatCannotReachQuorum()
    {
        _context.IggyClient = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = _context.NoQuorumTcpUrl,
            Protocol = Protocol.Tcp
        });

        await _context.IggyClient.ConnectAsync();
    }

    [When(@"I sign out")]
    public async Task WhenISignOut()
    {
        await _context.IggyClient.LogoutUserAsync();
    }

    [When(@"I request the stream with ID (\d+)")]
    public async Task WhenIRequestTheStreamWithId(uint streamId)
    {
        await RecordCallError(() => _context.IggyClient.GetStreamByIdAsync(Identifier.Numeric(streamId)));
    }

    [When(@"I sign in to that node as the root user")]
    public async Task WhenISignInToThatNodeAsTheRootUser()
    {
        await RecordCallError(() =>
            _context.IggyClient.LoginUserAsync(TestEnvironment.RootUsername, TestEnvironment.RootPassword));
    }

    [Then(@"the call should fail with error code (\d+)")]
    public void ThenTheCallShouldFailWithErrorCode(int code)
    {
        var error = _context.LastCallError.ShouldBeOfType<IggyInvalidStatusCodeException>();
        error.StatusCode.ShouldBe(code);
    }

    private async Task RecordCallError(Func<Task> call)
    {
        try
        {
            await call();
            _context.LastCallError = null;
        }
        catch (Exception error)
        {
            _context.LastCallError = error;
        }
    }
}
