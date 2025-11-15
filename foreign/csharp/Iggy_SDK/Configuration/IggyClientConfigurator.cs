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

using Apache.Iggy.Enums;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Iggy.Configuration;

/// <summary>
///     Configuration for the Iggy client
/// </summary>
public sealed class IggyClientConfigurator
{
    /// <summary>
    ///     The base address of the Iggy server.
    /// </summary>
    public required string BaseAddress { get; set; }

    /// <summary>
    ///     The transport protocol to use.
    /// </summary>
    public required Protocol Protocol { get; set; }

    /// <summary>
    ///     The size of the receive buffer in bytes. Default is 4096.
    /// </summary>
    public int ReceiveBufferSize { get; set; } = 4096;

    /// <summary>
    ///     The size of the send buffer in bytes. Default is 4096.
    /// </summary>
    public int SendBufferSize { get; set; } = 4096;

    /// <summary>
    ///     TLS settings
    /// </summary>
    public TlsSettings TlsSettings { get; set; } = new();

    /// <summary>
    ///     Reconnection settings
    /// </summary>
    public ReconnectionSettings ReconnectionSettings { get; set; } = new();

    /// <summary>
    ///     Auto-login settings used after successful connection.
    /// </summary>
    public AutoLoginSettings AutoLoginSettings { get; set; } = new();

    /// <summary>
    ///     The logger factory to use.
    /// </summary>
    public ILoggerFactory LoggerFactory { get; set; } = NullLoggerFactory.Instance;
}
