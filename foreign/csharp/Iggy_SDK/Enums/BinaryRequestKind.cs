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
///     Declares how a raw binary request executes on the server.
/// </summary>
/// <remarks>
///     Classic framing carries the length, the command code, then the payload, with no operation
///     field, so this SDK encodes both kinds identically and the server decides how a command runs.
///     The declaration exists so the API matches every other Iggy SDK and is already in place when
///     the C# client gains VSR framing.
/// </remarks>
public enum BinaryRequestKind
{
    /// <summary>
    ///     Runs on the receiving node only, outside consensus.
    /// </summary>
    NonReplicated = 1,

    /// <summary>
    ///     Replicated through consensus before it takes effect.
    /// </summary>
    Replicated = 2
}
