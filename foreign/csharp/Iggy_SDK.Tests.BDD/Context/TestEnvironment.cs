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

namespace Apache.Iggy.Tests.BDD.Context;

/// <summary>
/// Endpoints and credentials the BDD suite runs against. A default here would turn a dropped
/// compose variable into a run against whatever happens to listen on the fallback address, so a
/// missing value aborts the suite instead.
/// </summary>
public static class TestEnvironment
{
    public static string TcpAddress => Require("IGGY_TCP_ADDRESS");
    public static string LeaderTcpAddress => Require("IGGY_TCP_ADDRESS_LEADER");
    public static string FollowerTcpAddress => Require("IGGY_TCP_ADDRESS_FOLLOWER");
    public static string RootUsername => Require("IGGY_ROOT_USERNAME");
    public static string RootPassword => Require("IGGY_ROOT_PASSWORD");

    private static string Require(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new InvalidOperationException(
                $"{name} must be set; run the suite via scripts/run-bdd-tests.sh");
        }

        return value;
    }
}
