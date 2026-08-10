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

using Apache.Iggy.Utils;

namespace Apache.Iggy.Tests.VsrTests;

/// <summary>
///     Mirrors <c>core/sdk/src/leader_aware.rs</c> <c>test_is_same_address</c> and
///     <c>test_normalize_address</c>: the leader redirect check has to agree with the Rust SDK, or the two
///     disagree on whether a roster entry names the node this connection is already on.
/// </summary>
public sealed class ServerAddressTests
{
    [Theory]
    [InlineData("127.0.0.1:8090", "127.0.0.1:8090")]
    [InlineData("localhost:8090", "127.0.0.1:8090")]
    [InlineData("LOCALHOST:8090", "127.0.0.1:8090")]
    [InlineData("[::1]:8090", "[::1]:8090")]
    public void IsSame_MatchesEquivalentEndpoints(string first, string second)
    {
        Assert.True(ServerAddress.IsSame(first, second));
        Assert.True(ServerAddress.IsSame(second, first));
    }

    [Theory]
    [InlineData("127.0.0.1:8090", "127.0.0.1:8091")]
    [InlineData("192.168.1.1:8090", "127.0.0.1:8090")]
    [InlineData("localhost:8090", "127.0.0.1:8091")]
    [InlineData("iggy-1:8090", "iggy-2:8090")]
    [InlineData("127.0.0.1:8090", "")]
    public void IsSame_SeparatesDistinctEndpoints(string first, string second)
    {
        Assert.False(ServerAddress.IsSame(first, second));
        Assert.False(ServerAddress.IsSame(second, first));
    }

    [Theory]
    [InlineData("localhost:8090", "127.0.0.1:8090")]
    [InlineData("LOCALHOST:8090", "127.0.0.1:8090")]
    [InlineData("[::]:8090", "[::1]:8090")]
    [InlineData("0.0.0.0:8090", "127.0.0.1:8090")]
    [InlineData("my-localhost-1:8090", "my-localhost-1:8090")]
    public void Normalize_ResolvesHostAliases(string address, string expected)
    {
        Assert.Equal(expected, ServerAddress.Normalize(address));
    }

    /// <summary>
    ///     A host name that merely contains an alias is a different node, and a server bound to the unspecified
    ///     address answers on the loopback one.
    /// </summary>
    [Theory]
    [InlineData("my-localhost-1:8090", "127.0.0.1:8090", false)]
    [InlineData("localhost.example.com:8090", "127.0.0.1:8090", false)]
    [InlineData("0.0.0.0:8090", "127.0.0.1:8090", true)]
    [InlineData("[::]:8090", "[::1]:8090", true)]
    [InlineData("0.0.0.0:8090", "[::1]:8090", false)]
    public void IsSame_ResolvesHostAliasesWithoutSubstringMatching(string first, string second, bool same)
    {
        Assert.Equal(same, ServerAddress.IsSame(first, second));
        Assert.Equal(same, ServerAddress.IsSame(second, first));
    }

    [Fact]
    public void IsSame_FallsBackToNormalizedStringsForUnparsableAddresses()
    {
        Assert.True(ServerAddress.IsSame("Iggy-Node:8090", "iggy-node:8090"));
        Assert.False(ServerAddress.IsSame("iggy-node:8090", "iggy-node:8091"));
    }
}
