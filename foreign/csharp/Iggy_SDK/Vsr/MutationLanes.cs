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
///     repeat within a client. A waiter keeps its place while <paramref name="signInRecovery" /> returns the
///     running recovery that signs in again, and enters under the session that continues its own.
/// </summary>
/// <remarks>
///     Only the first waiter of a free lane takes budget, and the waiters take it in the order they queued for it,
///     so a release wakes only a waiter it can admit. Restarts take bytes ahead of the enters, because a restart
///     keeps its slot and an enter can wait for exactly that slot. An admission reserves its budget and request id
///     under the gate and builds its frame outside it. A failed build, a cancellation or a session change in
///     between gives the reservation back, so no request id is kept without a frame.
/// </remarks>
internal sealed class MutationLanes(ConsensusSession session, IggyClientConfigurator configuration,
    Func<Task?>? signInRecovery = null)
{
    private sealed class Waiter(MutationLane lane, int size, bool retainable, bool restart)
    {
        internal readonly MutationLane Lane = lane;
        internal readonly int Size = size;
        internal readonly bool Retainable = retainable;
        internal readonly bool Restart = restart;
        internal LinkedListNode<Waiter>? LaneNode;
        internal LinkedListNode<Waiter>? BudgetNode;
        private TaskCompletionSource? _signal;

        // Armed under the gate after a failed admission check, so a wake that comes later reaches the wait.
        internal Task Arm()
        {
            _signal = NewSignal();
            return _signal.Task;
        }

        internal void Wake() => _signal?.TrySetResult();
    }

    private readonly int _maxOperations = ValidateLimits(configuration).MaxPendingOperations;
    private readonly long _maxBytes = configuration.MaxPendingOperationBytes;
    private readonly object _gate = new();
    private readonly Dictionary<MutationLane, LinkedList<Waiter>> _laneWaiters = [];
    private readonly LinkedList<Waiter> _restartWaiters = new();
    private readonly LinkedList<Waiter> _budgetWaiters = new();
    private readonly Dictionary<MutationLane, ClientOperation> _lanes = [];
    private readonly Dictionary<ulong, ClientOperation> _operations = [];
    private readonly Queue<(ulong OperationId, IggyInvalidStatusCodeException? Rejection)> _proved = new();
    private int _admitted;
    private long _bytes;

    /// <summary>Rejects pending limits that admit no mutation, so the client factory can fail before construction.</summary>
    internal static IggyClientConfigurator ValidateLimits(IggyClientConfigurator configuration)
    {
        if (configuration.MaxPendingOperations <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(IggyClientConfigurator.MaxPendingOperations),
                configuration.MaxPendingOperations, "MaxPendingOperations must be greater than 0.");
        }

        if (configuration.MaxPendingOperationBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(IggyClientConfigurator.MaxPendingOperationBytes),
                configuration.MaxPendingOperationBytes, "MaxPendingOperationBytes must be greater than 0.");
        }

        return configuration;
    }

    /// <summary>The waiters whose lane is free and that wait for an operation slot or for bytes.</summary>
    internal int BudgetWaiterCount
    {
        get
        {
            lock (_gate)
            {
                return _budgetWaiters.Count;
            }
        }
    }

    /// <summary>The operations that hold a request identity, sent or not.</summary>
    internal int OperationCount
    {
        get
        {
            lock (_gate)
            {
                return _operations.Count;
            }
        }
    }

    internal async Task<ClientOperation> EnterAsync(MutationLane lane, int code, ReadOnlyMemory<byte> body,
        PartitionContext context, bool retainable, ulong generation, CancellationToken token)
    {
        var waiter = new Waiter(lane, FrameSize(body, retainable), retainable, restart: false);
        try
        {
            while (true)
            {
                Task changed;
                Task? recovery;
                SessionFrame? identity = null;
                lock (_gate)
                {
                    waiter.LaneNode ??= JoinLane(waiter);
                    token.ThrowIfCancellationRequested();
                    if (AdmissionGeneration(generation, out recovery) is { } admission)
                    {
                        generation = admission;
                        if (_lanes.TryGetValue(lane, out var active) && active.Retained)
                        {
                            throw new VsrLaneBlockedException(active.Identity.RequestId);
                        }
                        if (active is null && waiter.LaneNode.Previous is null)
                        {
                            identity = TryReserve(waiter, code);
                        }
                    }
                    changed = identity is null ? waiter.Arm() : Task.CompletedTask;
                }
                if (identity is { } reserved)
                {
                    var operation = Build(waiter, code, body, context, generation, reserved);
                    lock (_gate)
                    {
                        if (Keep(waiter, operation, token))
                        {
                            _lanes.Add(lane, operation);
                            _operations.Add(reserved.RequestId, operation);
                            Leave(waiter);
                            return operation;
                        }
                    }
                    continue;
                }
                await WaitForChangeAsync(changed, recovery, token);
            }
        }
        catch
        {
            lock (_gate)
            {
                Leave(waiter);
            }
            throw;
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
    // Queued writers keep the old bytes charged until their aliases are released. The new identity comes from the
    // session that continues the one the previous operation was created under.
    internal async Task<ClientOperation> RestartAsync(ClientOperation previous, ReadOnlyMemory<byte> body,
        CancellationToken token)
    {
        var waiter = new Waiter(previous.Lane, FrameSize(body, previous.Retainable), previous.Retainable,
            restart: true);
        lock (_gate)
        {
            if (!Owns(previous))
            {
                throw session.Ended(previous.Generation)
                    ?? new VsrOperationNotRetainedException(previous.Identity.RequestId);
            }
            if (previous.Uncertain || previous.Retained)
            {
                throw new InvalidOperationException("An uncertain operation must keep its original request identity.");
            }
        }
        previous.Lifetime.Cancel();
        previous.ReleaseRegistryFrame();
        try
        {
            while (true)
            {
                Task changed;
                Task? recovery;
                ulong? admission;
                SessionFrame? identity = null;
                lock (_gate)
                {
                    token.ThrowIfCancellationRequested();
                    admission = AdmissionGeneration(previous.Generation, out recovery);
                    if (!Owns(previous))
                    {
                        throw new VsrOperationNotRetainedException(previous.Identity.RequestId);
                    }
                    if (admission is not null)
                    {
                        identity = TryReserve(waiter, previous.Code);
                    }
                    changed = identity is null ? waiter.Arm() : Task.CompletedTask;
                }
                if (identity is { } reserved && admission is { } generation)
                {
                    var operation = Build(waiter, previous.Code, body, previous.Context, generation, reserved);
                    lock (_gate)
                    {
                        if (Keep(waiter, operation, token))
                        {
                            if (!Owns(previous))
                            {
                                Unreserve(waiter);
                                throw new VsrOperationNotRetainedException(previous.Identity.RequestId);
                            }
                            _operations.Remove(previous.Identity.RequestId);
                            _operations.Add(reserved.RequestId, operation);
                            _lanes[previous.Lane] = operation;
                            return operation;
                        }
                    }
                    continue;
                }
                await WaitForChangeAsync(changed, recovery, token);
            }
        }
        catch
        {
            lock (_gate)
            {
                Leave(waiter);
            }
            throw;
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
            // A replay keeps the state it has, which an abandon can have dropped. An operation of a replaced session
            // can never replay, so it must not block the lane of the new one.
            if (operation.Retainable && !operation.Replaying && Owns(operation)
                && operation.Generation == session.Generation)
            {
                operation.Retained = true;
                WakeLane(operation.Lane);
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
            WakeLane(operation.Lane);
        }
        operation.Lifetime.Cancel();
        operation.ReleaseRegistryFrame();
    }

    internal void Reset() => CompleteWhere(_ => true);

    /// <summary>
    ///     Drops the retained operations of a replaced session, which can never replay. The live operations and the
    ///     waiters keep their lanes and slots: their calls go on under the session the client signs in again, or
    ///     end uncertain.
    /// </summary>
    internal void DropRetained() => CompleteWhere(operation => operation.Retained);

    private void CompleteWhere(Func<ClientOperation, bool> selected)
    {
        ClientOperation[] operations;
        lock (_gate)
        {
            operations = _operations.Values.Where(selected).ToArray();
            WakeAll();
        }
        foreach (var operation in operations)
        {
            Complete(operation);
        }
    }

    /// <summary>
    ///     The generation that an operation made under <paramref name="generation" /> is admitted under, as
    ///     <see cref="ConsensusSession.Successor" /> gives it. Null while <paramref name="recovery" /> signs in again.
    /// </summary>
    /// <exception cref="IggyInvalidStatusCodeException">
    ///     The caller signed in or out since, or the server refused a sign-in, as <see cref="ConsensusSession.Ended" />
    ///     gives it.
    /// </exception>
    /// <exception cref="NotConnectedException">
    ///     No session that continues the operation's own is bound or coming, or the client stopped.
    /// </exception>
    private ulong? AdmissionGeneration(ulong generation, out Task? recovery)
    {
        recovery = null;
        if (session.Successor(generation) is { } admission)
        {
            return admission;
        }
        recovery = signInRecovery?.Invoke();
        return recovery is null ? throw session.Ended(generation) ?? new NotConnectedException() : null;
    }

    private static Task WaitForChangeAsync(Task changed, Task? recovery, CancellationToken token) =>
        (recovery is null ? changed : Task.WhenAny(changed, recovery)).WaitAsync(token);

    private int FrameSize(ReadOnlyMemory<byte> body, bool retainable)
    {
        var size = VsrHeader.HEADER_SIZE + (long)body.Length;
        if (size > int.MaxValue || retainable && size > _maxBytes)
        {
            throw VsrError.Exception(VsrError.INVALID_SIZE_BYTES, "Request frame exceeds the configured operation byte limit.");
        }
        return (int)size;
    }

    private LinkedListNode<Waiter> JoinLane(Waiter waiter)
    {
        if (!_laneWaiters.TryGetValue(waiter.Lane, out var queue))
        {
            queue = new LinkedList<Waiter>();
            _laneWaiters.Add(waiter.Lane, queue);
        }
        return queue.AddLast(waiter);
    }

    /// <summary>
    ///     Reserves the budget of a waiter whose lane is ready and takes its request id, or queues the waiter for the
    ///     budget. A waiter reserves only at the head of its queue, and an enter only while no restart waits.
    /// </summary>
    /// <remarks>
    ///     The strict order is on purpose. A small frame that passed a larger head could starve it, because small
    ///     frames can keep taking the bytes that each release frees. Restarts queue ahead of the enters, because an
    ///     enter at the head can wait for the slot that a waiting restart keeps, and the restart could never pass it.
    /// </remarks>
    private SessionFrame? TryReserve(Waiter waiter, int code)
    {
        if (waiter.Retainable)
        {
            var queue = waiter.Restart ? _restartWaiters : _budgetWaiters;
            var turn = queue.First is null || queue.First == waiter.BudgetNode;
            if (!turn || !waiter.Restart && _restartWaiters.Count > 0 || !Fits(waiter))
            {
                waiter.BudgetNode ??= queue.AddLast(waiter);
                return null;
            }
            if (waiter.BudgetNode is { } node)
            {
                queue.Remove(node);
                waiter.BudgetNode = null;
            }
            _bytes += waiter.Size;
            if (!waiter.Restart)
            {
                _admitted++;
            }
            WakeBudget();
        }
        try
        {
            return session.Resolve(VsrOperations.ForCode(code));
        }
        catch
        {
            Unreserve(waiter);
            throw;
        }
    }

    // Outside the gate, so a large body never holds back the admissions of the other lanes.
    private ClientOperation Build(Waiter waiter, int code, ReadOnlyMemory<byte> body, PartitionContext context,
        ulong generation, SessionFrame identity)
    {
        try
        {
            var size = waiter.Size;
            var bytes = new byte[size];
            VsrHeader.EncodeRequestHeader(bytes, session, code, body.Span, context, identity);
            body.CopyTo(bytes.AsMemory(VsrHeader.HEADER_SIZE));
            var frame = new SharedRequestFrame(bytes, waiter.Retainable ? () => ReleaseBudget(size) : null);
            return new ClientOperation(waiter.Lane, code, identity, context, generation, frame, waiter.Retainable);
        }
        catch
        {
            lock (_gate)
            {
                Unreserve(waiter);
            }
            throw;
        }
    }

    /// <summary>
    ///     Whether a built operation keeps its reservation. A cancellation or a session change during the build gives
    ///     it back, and after a session change the waiter checks its admission again.
    /// </summary>
    private bool Keep(Waiter waiter, ClientOperation operation, CancellationToken token)
    {
        if (!token.IsCancellationRequested && operation.Generation == session.Generation)
        {
            return true;
        }
        Unreserve(waiter);
        token.ThrowIfCancellationRequested();
        return false;
    }

    private void Unreserve(Waiter waiter)
    {
        if (!waiter.Retainable)
        {
            return;
        }
        _bytes -= waiter.Size;
        if (!waiter.Restart)
        {
            _admitted--;
        }
        WakeBudget();
    }

    private void Leave(Waiter waiter)
    {
        if (waiter.BudgetNode is { } budget)
        {
            var first = budget.Previous is null;
            (waiter.Restart ? _restartWaiters : _budgetWaiters).Remove(budget);
            waiter.BudgetNode = null;
            if (first)
            {
                WakeBudget();
            }
        }
        if (waiter.LaneNode is { } node)
        {
            var queue = _laneWaiters[waiter.Lane];
            var first = node.Previous is null;
            queue.Remove(node);
            waiter.LaneNode = null;
            if (queue.Count == 0)
            {
                _laneWaiters.Remove(waiter.Lane);
            }
            else if (first)
            {
                WakeLane(waiter.Lane);
            }
        }
    }

    // The first waiter of a lane enters once the lane is free, and fails at once when the lane is retained.
    private void WakeLane(MutationLane lane)
    {
        if (_laneWaiters.TryGetValue(lane, out var queue)
            && (!_lanes.TryGetValue(lane, out var active) || active.Retained))
        {
            queue.First?.Value.Wake();
        }
    }

    private void WakeBudget()
    {
        if ((_restartWaiters.First ?? _budgetWaiters.First)?.Value is { } next && Fits(next))
        {
            next.Wake();
        }
    }

    // A reset ends or replaces the session of every waiter, so each one checks its admission again.
    private void WakeAll()
    {
        foreach (var queue in _laneWaiters.Values)
        {
            foreach (var waiter in queue)
            {
                waiter.Wake();
            }
        }
        foreach (var waiter in _restartWaiters)
        {
            waiter.Wake();
        }
    }

    private bool Fits(Waiter waiter) =>
        waiter.Size <= _maxBytes - _bytes && (waiter.Restart || _admitted < _maxOperations);

    // An abandon frees the slot of a running replay, so the completion of that replay must not free it again.
    private void ReleaseSlot(ClientOperation operation)
    {
        if (operation.Retainable && !operation.SlotReleased)
        {
            operation.SlotReleased = true;
            _admitted--;
            WakeBudget();
        }
    }

    private void ReleaseBudget(long size)
    {
        lock (_gate)
        {
            _bytes -= size;
            WakeBudget();
        }
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
