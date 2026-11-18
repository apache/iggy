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
///     The order of a list page read. The values are the wire codes.
/// </summary>
public enum PageOrder : byte
{
    /// <summary>The numeric id.</summary>
    Id = 0,

    /// <summary>
    ///     The creation time, then the id. Partitions and consumer groups get their ids in creation order, so the
    ///     server reads them by id.
    /// </summary>
    Created = 1
}
