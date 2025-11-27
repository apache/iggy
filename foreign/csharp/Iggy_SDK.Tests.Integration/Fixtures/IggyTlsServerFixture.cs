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

using Apache.Iggy.Tests.Integrations.Helpers;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

/// <summary>
///     Iggy server fixture configured with TLS enabled.
///     Requires certificates to be mounted in the container.
/// </summary>
public class IggyTlsServerFixture : IggyServerFixture
{
    /// <summary>
    ///     Environment variables with TLS configuration enabled.
    /// </summary>
    protected override Dictionary<string, string> EnvironmentVariables => new(base.EnvironmentVariables)
    {
        { "IGGY_TCP_TLS_ENABLED", "true" },
        { "IGGY_TCP_TLS_CERT_FILE", "/app/certs/iggy_cert.pem" },
        { "IGGY_TCP_TLS_KEY_FILE", "/app/certs/iggy_key.pem" }
    };

    /// <summary>
    ///     Resource mappings for TLS certificates.
    /// </summary>
    protected override ResourceMapping[] ResourceMappings =>
    [
        new("Certs", "/app/certs/")
    ];

    public override async Task InitializeAsync()
    {
        await IggyContainer!.StartAsync();
    }
}
