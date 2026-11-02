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
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;

namespace Apache.Iggy.Tests.ConsumerTests;

public class RentedBatchTests
{
    [Fact]
    public void RentedBatchHandle_AcquireRelease_Balanced_DisposesOnce()
    {
        var owner = new TrackingMemoryOwner(16);
        var rental = new PolledMessagesRental(owner)
        {
            PartitionId = 1,
            CurrentOffset = 0,
            Messages = Array.Empty<RentedMessageResponse>()
        };
        var handle = new RentedBatchHandle(rental); // refCount=1 (self-ref)

        handle.Acquire(); // 2
        handle.Acquire(); // 3
        Assert.Equal(0, owner.DisposeCount);

        handle.Release(); // 2
        handle.Release(); // 1 (self-ref still alive)
        Assert.Equal(0, owner.DisposeCount);

        handle.Release(); // 0 -> disposed
        Assert.Equal(1, owner.DisposeCount);
    }

    [Fact]
    public void PolledMessagesRental_Dispose_Idempotent()
    {
        var owner = new TrackingMemoryOwner(16);
        var rental = new PolledMessagesRental(owner)
        {
            PartitionId = 1,
            CurrentOffset = 0,
            Messages = Array.Empty<RentedMessageResponse>()
        };

        rental.Dispose();
        rental.Dispose();
        rental.Dispose();

        Assert.Equal(1, owner.DisposeCount);
    }

    [Fact]
    public async Task PolledMessagesRental_ConcurrentDispose_ReturnsBufferOnce()
    {
        var owner = new TrackingMemoryOwner(16);
        var rental = new PolledMessagesRental(owner)
        {
            PartitionId = 1,
            CurrentOffset = 0,
            Messages = Array.Empty<RentedMessageResponse>()
        };

        using var start = new ManualResetEventSlim(false);
        Task[] tasks = Enumerable.Range(0, 64).Select(_ => Task.Run(() =>
        {
            start.Wait();
            rental.Dispose();
        })).ToArray();

        start.Set();
        await Task.WhenAll(tasks);

        Assert.Equal(1, owner.DisposeCount);
    }

    internal sealed class TrackingMemoryOwner : IMemoryOwner<byte>
    {
        private readonly byte[] _buffer;

        public int DisposeCount { get; private set; }

        public TrackingMemoryOwner(int size)
        {
            _buffer = new byte[size];
        }

        public Memory<byte> Memory => _buffer;

        public void Dispose()
        {
            DisposeCount++;
        }
    }
}
