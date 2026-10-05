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

using System.Text.Json.Serialization;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Immutable authority captured before a partition operation is sent. The incarnation changes only when a
///     partition id is reused after a delete, so offsets and retry receipts never cross into the new partition.
/// </summary>
public readonly record struct PartitionContext(
    [property: JsonPropertyName("incarnation")] ulong Incarnation,
    [property: JsonPropertyName("owner_generation")] ulong OwnerGeneration,
    [property: JsonPropertyName("metadata_op")] ulong MetadataOp)
{
    internal const int ENCODED_SIZE = 24;
}
