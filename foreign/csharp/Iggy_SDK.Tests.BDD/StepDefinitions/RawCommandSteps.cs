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

using Apache.Iggy.Exceptions;
using Reqnroll;
using Shouldly;
using TestContext = Apache.Iggy.Tests.BDD.Context.TestContext;

namespace Apache.Iggy.Tests.BDD.StepDefinitions;

[Binding]
public class RawCommandSteps
{
    private readonly TestContext _context;

    public RawCommandSteps(TestContext context)
    {
        _context = context;
    }

    [When(@"I send a raw command with code (\d+) and an empty payload")]
    public async Task WhenISendARawCommand(uint code)
    {
        try
        {
            _context.LastRawResponse = await _context.IggyClient.SendBinaryRequestAsync(code, []);
            _context.LastRawError = null;
        }
        catch (Exception error)
        {
            _context.LastRawResponse = null;
            _context.LastRawError = error;
        }
    }

    [Then(@"the raw command should succeed with an empty response")]
    public void ThenTheRawCommandShouldSucceedWithAnEmptyResponse()
    {
        _context.LastRawError.ShouldBeNull();
        _context.LastRawResponse.ShouldBeEmpty();
    }

    [Then(@"the raw command should succeed with a non-empty response")]
    public void ThenTheRawCommandShouldSucceedWithANonEmptyResponse()
    {
        _context.LastRawError.ShouldBeNull();
        _context.LastRawResponse.ShouldNotBeEmpty();
    }

    [Then(@"the raw command should fail with an invalid command error")]
    public void ThenTheRawCommandShouldFailWithAnInvalidCommandError()
    {
        _context.LastRawResponse.ShouldBeNull();
        var error = _context.LastRawError.ShouldBeOfType<IggyInvalidStatusCodeException>();
        error.StatusCode.ShouldBe(3);
    }
}
