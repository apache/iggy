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

/// <summary>A retained operation blocks this mutation lane until it is resolved or abandoned.</summary>
public class VsrLaneBlockedException : Exception
{
    /// <summary>The SDK-local lane blocked status.</summary>
    public const int STATUS_CODE = VsrError.LANE_BLOCKED;

    /// <summary>A retained operation blocks the lane.</summary>
    public VsrLaneBlockedException(ulong operationId)
        : this(operationId, null, $"Lane blocked by operation {operationId} with an unknown outcome.")
    {
    }

    /// <summary>A retained operation blocks a particular fetch target.</summary>
    public VsrLaneBlockedException(ulong operationId, uint partitionId)
        : this(operationId, partitionId, $"Lane blocked by operation {operationId} with an unknown outcome.")
    {
    }

    private protected VsrLaneBlockedException(ulong operationId, uint? partitionId, string message) : base(message)
    {
        OperationId = operationId;
        PartitionId = partitionId;
    }

    /// <summary>The request id of the operation.</summary>
    public ulong OperationId { get; }

    /// <summary>The blocked target of a fetch, otherwise null.</summary>
    public uint? PartitionId { get; }

    /// <summary>The blocker is a committing fetch without a replayable server receipt.</summary>
    public virtual bool RequiresExplicitRecovery => false;

    /// <summary>The SDK-local status of the block.</summary>
    public virtual int StatusCode => STATUS_CODE;
}
