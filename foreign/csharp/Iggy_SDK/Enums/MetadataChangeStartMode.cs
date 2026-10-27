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
///     Where a metadata changes query starts. The values are the wire codes.
/// </summary>
public enum MetadataChangeStartMode : byte
{
    /// <summary>The oldest operation the requested groups still hold completely.</summary>
    Earliest = 0,

    /// <summary>No records, only a cursor at the newest retained record.</summary>
    Latest = 1,

    /// <summary>Records after a change cursor.</summary>
    After = 2,

    /// <summary>Current state pages, the first one without a page token.</summary>
    Current = 3
}
