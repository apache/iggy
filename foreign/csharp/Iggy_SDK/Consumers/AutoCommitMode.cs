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

namespace Apache.Iggy.Consumers;

/// <summary>
///     Auto commit modes
/// </summary>
public enum AutoCommitMode
{
    /// <summary>
    ///     Set auto commit to true on polling messages
    /// </summary>
    Auto,

    /// <summary>
    ///     Stores the offset of a message once the receive loop asks for the next message. Neither this store nor the
    ///     final checkpoint on dispose covers a message whose loop body threw, broke or was still running, so that
    ///     message is delivered again.
    /// </summary>
    AfterReceive,

    /// <summary>
    ///     Offset will not be stored automatically
    /// </summary>
    Disabled
}
