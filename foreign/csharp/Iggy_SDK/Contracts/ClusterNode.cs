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

namespace Apache.Iggy.Contracts;

/// <summary>
///     Cluster node metadata
/// </summary>
public class ClusterNode
{
    /// <summary>
    ///     Node identifier
    /// </summary>
    public required uint Id { get; set; }

    /// <summary>
    ///     Node name
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    ///     Node address
    /// </summary>
    public required string Address { get; set; }

    /// <summary>
    ///     Node role within the cluster
    /// </summary>
    public required ClusterNodeRole Role { get; set; }

    /// <summary>
    ///     Node status
    /// </summary>
    public required ClusterNodeStatus Status { get; set; }

    internal int GetSize()
    {
        // id, name length, name, address length, address, role, status
        return 4 + 4 + Name.Length + 4 + Address.Length + 1 + 1;
    }
}
