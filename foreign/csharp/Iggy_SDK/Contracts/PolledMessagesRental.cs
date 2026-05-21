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

using System.Buffers;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Represents a rented poll result whose payload and raw header memory remain valid until disposed.
/// </summary>
public sealed class PolledMessagesRental : IDisposable
{
    private readonly IMemoryOwner<byte> _owner;
    private int _disposed;

    /// <summary>
    ///     Partition identifier for the messages.
    /// </summary>
    public required int PartitionId { get; init; }

    /// <summary>
    ///     Current offset for the partition.
    /// </summary>
    public required ulong CurrentOffset { get; init; }

    /// <summary>
    ///     Rented messages.
    /// </summary>
    public required IReadOnlyList<RentedMessageResponse> Messages { get; init; }

    /// <summary>
    ///    Initializes a new instance of the <see cref="PolledMessagesRental" /> class with the specified memory owner.
    /// </summary>
    /// <param name="owner"></param>
    public PolledMessagesRental(IMemoryOwner<byte> owner)
    {
        _owner = owner;
    }

    /// <summary>
    ///     Disposes the rental and returns the underlying buffer to the pool.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _owner.Dispose();
    }
}
