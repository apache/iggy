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

namespace Apache.Iggy.Configuration;

/// <summary>
///     Settings for automatic reconnection
/// </summary>
public sealed class ReconnectionSettings
{
    /// <summary>
    ///     Enable automatic reconnection when connection is lost
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    ///     Maximum number of reconnection attempts (0 = infinite)
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    ///     Initial delay before first reconnection attempt
    /// </summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    ///     Maximum delay between reconnection attempts
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    ///     Specifies the duration to wait after a successful reconnection before resuming normal operations. Default is 1
    ///     second.
    /// </summary>
    /// <remarks>
    ///     This can help rejoin to e.g., a consumer group.
    /// </remarks>
    public TimeSpan WaitAfterReconnect { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    ///     Use exponential backoff for reconnection delays
    /// </summary>
    public bool UseExponentialBackoff { get; set; } = true;

    /// <summary>
    ///     Multiplier for exponential backoff (default: 2.0)
    /// </summary>
    public double BackoffMultiplier { get; set; } = 2.0;
}
