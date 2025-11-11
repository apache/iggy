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

namespace Apache.Iggy.Enums;

/// <summary>
///     Represents connection state to iggy server.
/// </summary>
public enum ConnectionState
{
    /// <summary>
    ///     Represents a state where the connection to the target server has been lost or terminated.
    /// </summary>
    Disconnected,

    /// <summary>
    ///     Indicates a state where the connection to the server is in the process of being established.
    /// </summary>
    Connecting,

    /// <summary>
    ///     Represents a state where the connection to the server has been successfully established.
    /// </summary>
    Connected,

    /// <summary>
    ///     Represents a state where the client is in the process of authenticating with the server.
    /// </summary>
    Authenticating,

    /// <summary>
    ///     Represents a state where the client has successfully authenticated with the target server after establishing a
    ///     connection.
    /// </summary>
    Authenticated
}
