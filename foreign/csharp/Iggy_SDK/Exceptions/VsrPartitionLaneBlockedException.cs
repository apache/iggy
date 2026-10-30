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

using Apache.Iggy.Vsr;

namespace Apache.Iggy.Exceptions;

/// <summary>
///     An uncertain committing fetch blocks the partition until the application abandons its operation. Polls,
///     committing fetches, offset stores and deletes of the partition fail with this until then.
/// </summary>
public sealed class VsrPartitionLaneBlockedException(ulong operationId, uint partitionId)
    : VsrLaneBlockedException(operationId, partitionId,
        $"Lane of partition {partitionId} blocked by operation {operationId} with an unknown outcome.")
{
    /// <summary>The SDK-local partition lane blocked status.</summary>
    public new const int STATUS_CODE = VsrError.PARTITION_LANE_BLOCKED;

    /// <inheritdoc />
    public override bool RequiresExplicitRecovery => true;

    /// <inheritdoc />
    public override int StatusCode => STATUS_CODE;
}
