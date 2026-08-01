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

using Apache.Iggy.Tests.Integrations.Fixtures;

namespace Apache.Iggy.Tests.Integrations.Attributes;

/// <summary>
///     The operation is not served by iggy-server-ng yet, so the test only runs against the classic server.
///     The reason names the gap.
/// </summary>
internal class SkipServerNgAttribute(string reason) : SkipAttribute($"Skipped for server-ng: {reason}")
{
    public override Task<bool> ShouldSkip(TestRegisteredContext context)
    {
        return Task.FromResult(IggyServerFixture.IsServerNg);
    }
}

/// <summary>Pins a test to iggy-server-ng: a classic run has no cluster to exercise.</summary>
internal class RequiresServerNgAttribute() : SkipAttribute("Requires IGGY_TEST_SERVER=ng")
{
    public override Task<bool> ShouldSkip(TestRegisteredContext context)
    {
        return Task.FromResult(!IggyServerFixture.IsServerNg);
    }
}

/// <summary>Pins a test to the classic server, which owns the only image it can start.</summary>
internal class RequiresClassicServerAttribute() : SkipAttribute("Requires IGGY_TEST_SERVER=classic")
{
    public override Task<bool> ShouldSkip(TestRegisteredContext context)
    {
        return Task.FromResult(IggyServerFixture.IsServerNg);
    }
}
