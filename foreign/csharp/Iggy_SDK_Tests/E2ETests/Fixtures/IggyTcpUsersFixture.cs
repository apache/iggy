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

using DotNet.Testcontainers.Builders;
using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;
using Iggy_SDK_Tests.E2ETests.Fixtures.Configs;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK_Tests.Utils.Users;
using Iggy_SDK.Configuration;
using Iggy_SDK.Enums;
using Iggy_SDK.Factory;
using Iggy_SDK.IggyClient;
using Microsoft.Extensions.Logging.Abstractions;
using IContainer = DotNet.Testcontainers.Containers.IContainer;

namespace Iggy_SDK_Tests.E2ETests.Fixtures.Tcp;

public sealed class IggyTcpUsersFixture : IggyBaseFixture
{
    public IIggyClient TcpSubjectUnderTest => SubjectsUnderTest[0]; 
    
    public IggyTcpUsersFixture() : base(new UsersFixtureBootstrap(),
        IggyFixtureClientMessagingSettings.PollingSettings,
        IggyFixtureClientMessagingSettings.BatchingSettings)
    {
    }
}