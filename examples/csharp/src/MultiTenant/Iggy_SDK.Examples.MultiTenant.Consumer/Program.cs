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

using Iggy_SDK.Examples.MultiTenant.Consumer;
using Iggy_SDK.Examples.Shared;
using Microsoft.Extensions.Logging;

using var loggerFactory = LoggerFactory.Create(builder => { builder.AddConsole(); });
var logger = loggerFactory.CreateLogger<Program>();

var address = ExampleHelpers.GetTcpServerAddr(args, logger);
var tenantsCount = MultiTenantHelpers.ReadPositiveInt("TENANTS_COUNT", 3);
var tenantId = MultiTenantHelpers.ReadPositiveInt("TENANT_ID", 1);
if (tenantId > tenantsCount)
{
    throw new FormatException($"TENANT_ID {tenantId} is above TENANTS_COUNT {tenantsCount}.");
}

logger.LogInformation(
    "Multi-tenant consumer has started, tenant: {TenantId}, tenants: {TenantsCount}.",
    tenantId,
    tenantsCount
);

logger.LogInformation("Connecting as root to create a read-only user for the tenant.");
using (var root = await MultiTenantHelpers.Connect(address, "iggy", "iggy", loggerFactory))
{
    await Utils.CreateConsumerUser(root, tenantId, logger);
}

using var client = await MultiTenantHelpers.ConnectAsTenant(
    address,
    Utils.ConsumerUsername(tenantId),
    loggerFactory
);

await MultiTenantHelpers.EnsureStreamAccess(client, tenantId, tenantsCount, logger);
await Utils.ConsumeMessages(client, tenantId, logger);

logger.LogInformation("Multi-tenant consumer has finished.");
