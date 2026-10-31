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
///     Outcome of a metadata changes query.
/// </summary>
public enum MetadataChangeStatus : byte
{
    /// <summary>Records or items after the requested position.</summary>
    Ok = 0,

    /// <summary>The answering node has not applied the requested position yet. Ask again later or ask another node.</summary>
    Behind = 1,

    /// <summary>The node no longer holds every record after the position. Rebuild from current state.</summary>
    Expired = 2,

    /// <summary>The position belongs to another history. Rebuild from current state.</summary>
    Reset = 3
}
