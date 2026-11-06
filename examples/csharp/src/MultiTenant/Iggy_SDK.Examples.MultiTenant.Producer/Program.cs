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

using Iggy_SDK.Examples.MultiTenant.Producer;
using Iggy_SDK.Examples.Shared;
using Microsoft.Extensions.Logging;

using var loggerFactory = LoggerFactory.Create(builder => { builder.AddConsole(); });
var logger = loggerFactory.CreateLogger<Program>();

var address = ExampleHelpers.GetTcpServerAddr(args, logger);
var tenantsCount = MultiTenantHelpers.ReadPositiveInt("TENANTS_COUNT", 3);
logger.LogInformation("Multi-tenant producer has started, tenants: {TenantsCount}.", tenantsCount);

logger.LogInformation("Connecting as root to create a stream and a user for each tenant.");
using (var root = await MultiTenantHelpers.Connect(address, "iggy", "iggy", loggerFactory))
{
    for (var tenantId = 1; tenantId <= tenantsCount; tenantId++)
    {
        await Utils.CreateStreamAndUser(root, tenantId, logger);
    }
}

for (var tenantId = 1; tenantId <= tenantsCount; tenantId++)
{
    logger.LogInformation("Connecting as tenant {TenantId}.", tenantId);
    using var client = await MultiTenantHelpers.ConnectAsTenant(
        address,
        Utils.ProducerUsername(tenantId),
        loggerFactory
    );

    await MultiTenantHelpers.EnsureStreamAccess(client, tenantId, tenantsCount, logger);
    await Utils.ProduceMessages(client, tenantId, logger);
}

logger.LogInformation("Multi-tenant producer has finished.");
