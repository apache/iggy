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

using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Exceptions;

namespace Apache.Iggy.Vsr;

internal readonly record struct MutationLane(ulong Incarnation, uint PartitionId)
{
    internal static MutationLane Metadata => default;
}

/// <summary>
///     One client-wide admission budget and FIFO lane table. Retention keeps the original reservation;
///     queue references keep bytes charged until the writer releases them, even after abandon or reset.
/// </summary>
internal sealed class MutationLanes(ConsensusSession session, IggyClientConfigurator configuration)
{
    private sealed class Waiting(MutationLane lane)
    {
        internal readonly MutationLane Lane = lane;
    }
    private readonly int _maxOperations = configuration.MaxPendingOperations > 0 ? configuration.MaxPendingOperations
        : throw new ArgumentOutOfRangeException(nameof(configuration.MaxPendingOperations));
    private readonly long _maxBytes = configuration.MaxPendingOperationBytes >= VsrHeader.HEADER_SIZE
        ? configuration.MaxPendingOperationBytes : throw new ArgumentOutOfRangeException(nameof(configuration.MaxPendingOperationBytes));
    private readonly object _gate = new();
    private readonly List<Waiting> _waiting = [];
    private readonly Dictionary<MutationLane, ClientOperation> _lanes = [];
    private readonly Dictionary<ulong, ClientOperation> _operations = [];
    private TaskCompletionSource _changed = NewSignal();
    private int _admitted;
    private long _bytes;

    internal async Task<ClientOperation> EnterAsync(MutationLane lane, int code, ReadOnlyMemory<byte> body,
        PartitionContext context, bool retainable, CancellationToken token)
    {
        var size = VsrHeader.HEADER_SIZE + (long)body.Length;
        if (size > int.MaxValue || size > _maxBytes)
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND, "Request frame exceeds the configured operation byte limit.");
        }
        var generation = session.Generation;
        var waiter = new Waiting(lane);
        lock (_gate)
        {
            _waiting.Add(waiter);
        }
        try
        {
            while (true)
            {
                Task changed;
                lock (_gate)
                {
                    token.ThrowIfCancellationRequested();
                    if (generation != session.Generation)
                    {
                        throw new NotConnectedException();
                    }
                    if (_lanes.TryGetValue(lane, out var active) && active.Retained)
                    {
                        throw new VsrLaneBlockedException(active.Identity.RequestId);
                    }
                    if (active is null && ReferenceEquals(_waiting.First(waiting => waiting.Lane == lane), waiter)
                        && _admitted < _maxOperations
                        && size <= _maxBytes - _bytes)
                    {
                        _admitted++;
                        _bytes += size;
                        try
                        {
                            var bytes = new byte[(int)size];
                            var identity = session.Resolve(VsrOperations.ForCode(code));
                            VsrHeader.EncodeRequestHeader(bytes, session, code, body.Span, context, identity);
                            body.CopyTo(bytes.AsMemory(VsrHeader.HEADER_SIZE));
                            var frame = new SharedRequestFrame(bytes, () => ReleaseBudget(size));
                            var operation = new ClientOperation(lane, code, identity, context, generation, frame, retainable);
                            _lanes.Add(lane, operation);
                            _operations.Add(identity.RequestId, operation);
                            return operation;
                        }
                        catch
                        {
                            _admitted--;
                            _bytes -= size;
                            Pulse();
                            throw;
                        }
                    }
                    changed = _changed.Task;
                }
                await changed.WaitAsync(token);
            }
        }
        finally
        {
            lock (_gate)
            {
                _waiting.Remove(waiter);
                Pulse();
            }
        }
    }

    internal void ThrowIfRetained(MutationLane lane)
    {
        lock (_gate)
        {
            if (_lanes.TryGetValue(lane, out var operation) && operation.Retained)
            {
                throw new VsrLaneBlockedException(operation.Identity.RequestId);
            }
        }
    }

    internal ClientOperation Retained(ulong operationId)
    {
        lock (_gate)
        {
            return _operations.TryGetValue(operationId, out var operation) && operation.Retained
                ? operation : throw new VsrOperationNotRetainedException(operationId);
        }
    }

    internal ClientOperation AcquireRetained(ulong operationId)
    {
        lock (_gate)
        {
            var operation = Retained(operationId);
            operation.Frame.Acquire();
            return operation;
        }
    }

    internal void Acquire(ClientOperation operation)
    {
        lock (_gate)
        {
            if (!Owns(operation))
            {
                throw new VsrOperationNotRetainedException(operation.Identity.RequestId);
            }
            operation.Frame.Acquire();
        }
    }

    internal bool Owns(ClientOperation operation)
    {
        lock (_gate)
        {
            return _operations.TryGetValue(operation.Identity.RequestId, out var active) && ReferenceEquals(active, operation);
        }
    }

    internal void Retain(ClientOperation operation)
    {
        lock (_gate)
        {
            if (operation.Retainable && Owns(operation))
            {
                operation.Retained = true;
                Pulse();
            }
        }
    }

    internal void Complete(ClientOperation operation)
    {
        lock (_gate)
        {
            if (!Owns(operation))
            {
                return;
            }
            _operations.Remove(operation.Identity.RequestId);
            _lanes.Remove(operation.Lane);
            _admitted--;
            Pulse();
        }
        operation.Lifetime.Cancel();
        operation.Frame.Release();
    }

    internal void Reset()
    {
        ClientOperation[] operations;
        lock (_gate)
        {
            operations = _operations.Values.ToArray();
        }
        foreach (var operation in operations)
        {
            Complete(operation);
        }
    }

    private void ReleaseBudget(long size)
    {
        lock (_gate)
        {
            _bytes -= size;
            Pulse();
        }
    }

    private void Pulse()
    {
        var changed = _changed;
        _changed = NewSignal();
        changed.TrySetResult();
    }

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}

internal sealed class ClientOperation(MutationLane lane, int code, SessionFrame identity, PartitionContext context,
    ulong generation, SharedRequestFrame frame, bool retainable)
{
    internal readonly MutationLane Lane = lane;
    internal readonly int Code = code;
    internal readonly SessionFrame Identity = identity;
    internal readonly PartitionContext Context = context;
    internal readonly ulong Generation = generation;
    internal readonly SharedRequestFrame Frame = frame;
    internal readonly bool Retainable = retainable;
    internal readonly SemaphoreSlim Resolution = new(1, 1);
    internal readonly CancellationTokenSource Lifetime = new();
    internal bool Retained;
    internal bool Uncertain;
    internal ReadOnlyMemory<byte> Body => Frame.Bytes.AsMemory(VsrHeader.HEADER_SIZE);
}

internal sealed class SharedRequestFrame(byte[] bytes, Action released)
{
    private int _references = 1;
    internal byte[] Bytes => bytes;

    internal void Acquire()
    {
        while (true)
        {
            var references = Volatile.Read(ref _references);
            ObjectDisposedException.ThrowIf(references == 0, this);
            if (Interlocked.CompareExchange(ref _references, references + 1, references) == references)
            {
                return;
            }
        }
    }

    internal void Release()
    {
        if (Interlocked.Decrement(ref _references) == 0)
        {
            Array.Clear(bytes);
            released();
        }
    }
}
