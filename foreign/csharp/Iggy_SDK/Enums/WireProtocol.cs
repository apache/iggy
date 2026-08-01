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
///     The wire framing used on top of the transport. Independent of <see cref="Protocol" />, which selects
///     the transport itself.
/// </summary>
public enum WireProtocol
{
    /// <summary>
    ///     Classic framing: <c>[length u32][command code u32][body]</c>.
    /// </summary>
    Classic,

    /// <summary>
    ///     Viewstamped Replication framing: a 256-byte consensus header followed by the body. TCP only.
    /// </summary>
    Vsr
}
