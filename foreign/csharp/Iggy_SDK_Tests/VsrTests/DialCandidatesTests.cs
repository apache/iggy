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

using Apache.Iggy.IggyClient.Implementations;

namespace Apache.Iggy.Tests.VsrTests;

/// <summary>
///     Mirrors the Rust SDK's <c>dial_candidates</c>: a client that loses the node it is on has to dial the rest
///     of the cluster, and the two SDKs have to agree on which endpoints those are and in what order.
/// </summary>
public sealed class DialCandidatesTests
{
    [Fact]
    public void LeadsWithTheCurrentEndpointThenNamesEachOtherOneOnce()
    {
        var candidates = TcpMessageStream.DialCandidates(
            "127.0.0.1:8090",
            "localhost:8090",
            ["127.0.0.1:8090", "127.0.0.1:8091", "127.0.0.1:8092"]);

        // Neither the roster's copy of the current endpoint nor a configured address that only spells the same
        // endpoint differently earns a second dial.
        Assert.Equal(["127.0.0.1:8090", "127.0.0.1:8091", "127.0.0.1:8092"], candidates);
    }

    [Fact]
    public void KeepsTheConfiguredAddressWhenNoRosterWasLearned()
    {
        var candidates = TcpMessageStream.DialCandidates("127.0.0.1:8091", "127.0.0.1:8090", []);

        Assert.Equal(["127.0.0.1:8091", "127.0.0.1:8090"], candidates);
    }

    [Fact]
    public void FallsBackToTheConfiguredAddressBeforeTheFirstConnect()
    {
        var candidates = TcpMessageStream.DialCandidates(string.Empty, "127.0.0.1:8090", []);

        Assert.Equal(["127.0.0.1:8090"], candidates);
    }

    [Fact]
    public void DialsOneEndpointWhenNothingElseIsKnown()
    {
        var candidates = TcpMessageStream.DialCandidates("127.0.0.1:8090", "127.0.0.1:8090", []);

        Assert.Equal(["127.0.0.1:8090"], candidates);
    }
}
