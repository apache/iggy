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

using System.Runtime.ExceptionServices;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Exceptions;

namespace Apache.Iggy.Vsr;

internal readonly record struct MutationLane(ulong Incarnation, uint PartitionId)
{
    internal static MutationLane Metadata => default;
}

/// <summary>
///     One client-wide mutation budget and FIFO lane table. Retention keeps the original reservation;
///     queue references keep bytes charged until the writer releases them, even after abandon or reset.
///     The outcomes that the last replays proved stay readable, also after a reset, because request ids never
///     repeat within a client.
/// </summary>
internal sealed class MutationLanes(ConsensusSession session, IggyClientConfigurator configuration)
{
    private sealed class Waiting(MutationLane lane)
    {
        internal readonly MutationLane Lane = lane;
    }
    private readonly int _maxOperations = configuration.MaxPendingOperations > 0 ? configuration.MaxPendingOperations
        : throw new ArgumentOutOfRangeException(nameof(configuration.MaxPendingOperations));
    private readonly long _maxBytes = configuration.MaxPendingOperationBytes > 0
        ? configuration.MaxPendingOperationBytes : throw new ArgumentOutOfRangeException(nameof(configuration.MaxPendingOperationBytes));
    private readonly object _gate = new();
    private readonly List<Waiting> _waiting = [];
    private readonly Dictionary<MutationLane, ClientOperation> _lanes = [];
    private readonly Dictionary<ulong, ClientOperation> _operations = [];
    private readonly Queue<(ulong OperationId, IggyInvalidStatusCodeException? Rejection)> _proved = new();
    private TaskCompletionSource _changed = NewSignal();
    private int _admitted;
    private long _bytes;

    internal async Task<ClientOperation> EnterAsync(MutationLane lane, int code, ReadOnlyMemory<byte> body,
        PartitionContext context, bool retainable, CancellationToken token)
    {
        var size = FrameSize(body, retainable);
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
                        && (!retainable || _admitted < _maxOperations && size <= _maxBytes - _bytes))
                    {
                        var operation = CreateOperation(lane, code, body, context, generation, retainable, size);
                        _lanes.Add(lane, operation);
                        _operations.Add(operation.Identity.RequestId, operation);
                        if (retainable)
                        {
                            _admitted++;
                        }
                        return operation;
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

    // The caller detaches its lifetime registration and frame alias, and supplies a body independent of the old frame.
    // Queued writers keep the old bytes charged until their aliases are released.
    internal async Task<ClientOperation> RestartAsync(ClientOperation previous, ReadOnlyMemory<byte> body,
        CancellationToken token)
    {
        var size = FrameSize(body, previous.Retainable);
        lock (_gate)
        {
            if (!Owns(previous))
            {
                throw new VsrOperationNotRetainedException(previous.Identity.RequestId);
            }
            if (previous.Uncertain || previous.Retained)
            {
                throw new InvalidOperationException("An uncertain operation must keep its original request identity.");
            }
        }
        previous.Lifetime.Cancel();
        previous.ReleaseRegistryFrame();
        while (true)
        {
            Task changed;
            lock (_gate)
            {
                token.ThrowIfCancellationRequested();
                if (previous.Generation != session.Generation)
                {
                    throw new NotConnectedException();
                }
                if (!Owns(previous))
                {
                    throw new VsrOperationNotRetainedException(previous.Identity.RequestId);
                }
                if (!previous.Retainable || size <= _maxBytes - _bytes)
                {
                    var operation = CreateOperation(previous.Lane, previous.Code, body, previous.Context,
                        previous.Generation, previous.Retainable, size);
                    _operations.Remove(previous.Identity.RequestId);
                    _operations.Add(operation.Identity.RequestId, operation);
                    _lanes[previous.Lane] = operation;
                    return operation;
                }
                changed = _changed.Task;
            }
            await changed.WaitAsync(token);
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

    internal ClientOperation? AcquireRetained(ulong operationId)
    {
        lock (_gate)
        {
            if (!_operations.TryGetValue(operationId, out var operation) || !operation.Retained)
            {
                return null;
            }
            operation.Frame.Acquire();
            return operation;
        }
    }

    /// <summary>
    ///     Returns when a replay proved that the operation committed and throws the committed rejection it proved.
    ///     Without a kept outcome it throws <see cref="VsrOperationNotRetainedException" />.
    /// </summary>
    internal void RepeatProvedOutcome(ulong operationId)
    {
        lock (_gate)
        {
            foreach (var (proved, rejection) in _proved)
            {
                if (proved == operationId)
                {
                    if (rejection is not null)
                    {
                        ExceptionDispatchInfo.Throw(rejection);
                    }
                    return;
                }
            }
        }
        throw new VsrOperationNotRetainedException(operationId);
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
            // A replay keeps the state it has, which an abandon can have dropped.
            if (operation.Retainable && !operation.Replaying && Owns(operation))
            {
                operation.Retained = true;
                Pulse();
            }
        }
    }

    /// <summary>
    ///     Starts the replay of a retained operation. Returns false when it is no longer retained: another replay
    ///     proved it, or an abandon or a reset dropped it.
    /// </summary>
    internal bool BeginReplay(ClientOperation operation)
    {
        lock (_gate)
        {
            if (!operation.Retained || !Owns(operation))
            {
                return false;
            }
            operation.Replaying = true;
            return true;
        }
    }

    /// <summary>Ends a replay. An abandon during the replay left the lane to it until now.</summary>
    internal void EndReplay(ClientOperation operation)
    {
        bool abandoned;
        lock (_gate)
        {
            operation.Replaying = false;
            abandoned = !operation.Retained;
        }
        if (abandoned)
        {
            Complete(operation);
        }
    }

    /// <summary>
    ///     Drops the retained operation, whose outcome stays unknown, and frees its operation slot. A running replay
    ///     keeps the lane and the frame until it ends, so no later mutation of the lane is sent while the replay runs.
    /// </summary>
    internal void Abandon(ulong operationId)
    {
        ClientOperation operation;
        lock (_gate)
        {
            operation = Retained(operationId);
            operation.Retained = false;
            if (operation.Replaying)
            {
                ReleaseSlot(operation);
                Pulse();
                return;
            }
        }
        Complete(operation);
    }

    /// <summary>
    ///     Completes a replay that proved the outcome. The outcome is kept before the release, so a resolve that no
    ///     longer finds the operation finds the outcome. It is kept also when an abandon came during the replay, or
    ///     a reset already released the operation.
    /// </summary>
    internal void Prove(ClientOperation operation, IggyInvalidStatusCodeException? rejection)
    {
        lock (_gate)
        {
            if (_proved.Count == _maxOperations)
            {
                _proved.Dequeue();
            }
            _proved.Enqueue((operation.Identity.RequestId, rejection));
        }
        Complete(operation);
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
            ReleaseSlot(operation);
            Pulse();
        }
        operation.Lifetime.Cancel();
        operation.ReleaseRegistryFrame();
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

    private int FrameSize(ReadOnlyMemory<byte> body, bool retainable)
    {
        var size = VsrHeader.HEADER_SIZE + (long)body.Length;
        if (size > int.MaxValue || retainable && size > _maxBytes)
        {
            throw VsrError.Exception(VsrError.INVALID_SIZE_BYTES, "Request frame exceeds the configured operation byte limit.");
        }
        return (int)size;
    }

    private ClientOperation CreateOperation(MutationLane lane, int code, ReadOnlyMemory<byte> body,
        PartitionContext context, ulong generation, bool retainable, int size)
    {
        if (retainable)
        {
            _bytes += size;
        }
        try
        {
            var bytes = new byte[size];
            var identity = session.Resolve(VsrOperations.ForCode(code));
            VsrHeader.EncodeRequestHeader(bytes, session, code, body.Span, context, identity);
            body.CopyTo(bytes.AsMemory(VsrHeader.HEADER_SIZE));
            var frame = new SharedRequestFrame(bytes, retainable ? () => ReleaseBudget(size) : null);
            return new ClientOperation(lane, code, identity, context, generation, frame, retainable);
        }
        catch
        {
            if (retainable)
            {
                _bytes -= size;
                Pulse();
            }
            throw;
        }
    }

    // An abandon frees the slot of a running replay, so the completion of that replay must not free it again.
    private void ReleaseSlot(ClientOperation operation)
    {
        if (operation.Retainable && !operation.SlotReleased)
        {
            operation.SlotReleased = true;
            _admitted--;
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
    private int _registryReference = 1;
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
    internal bool Replaying;
    internal bool SlotReleased;
    internal bool Uncertain;
    internal ReadOnlyMemory<byte> Body => Frame.Bytes.AsMemory(VsrHeader.HEADER_SIZE);

    internal void ReleaseRegistryFrame()
    {
        if (Interlocked.Exchange(ref _registryReference, 0) == 1)
        {
            Frame.Release();
        }
    }
}

internal sealed class SharedRequestFrame(byte[] bytes, Action? released)
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
            released?.Invoke();
        }
    }
}
