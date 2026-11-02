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

import NIOConcurrencyHelpers

/// A FIFO counting semaphore whose permits can be held across suspension
/// points. Waiters are served in arrival order, so a large request is not
/// starved by a stream of small ones.
final class AsyncSemaphore: Sendable {
    private struct Waiter {
        let id: UInt64
        let count: Int
        let continuation: CheckedContinuation<Bool, Error>
    }

    private struct State {
        var available: Int
        var waiters: [Waiter] = []
        var nextID: UInt64 = 0
    }

    private let state: NIOLockedValueBox<State>

    init(permits: Int) {
        precondition(permits >= 0, "permits must not be negative")
        state = NIOLockedValueBox(State(available: permits))
    }

    var availablePermits: Int {
        state.withLockedValue { $0.available }
    }

    /// Takes `count` permits without waiting; false when they are not free or
    /// an earlier waiter is still queued.
    func tryAcquire(_ count: Int) -> Bool {
        state.withLockedValue { state in
            guard state.waiters.isEmpty, state.available >= count else {
                return false
            }
            state.available -= count
            return true
        }
    }

    /// Takes `count` permits, waiting for them until `deadline` when one is
    /// given. Returns false at the deadline, holding nothing. A cancelled
    /// caller leaves the queue with `CancellationError` and holds nothing; one
    /// the permits were already handed to keeps them.
    func acquire(_ count: Int, deadline: ContinuousClock.Instant? = nil) async throws -> Bool {
        let id = state.withLockedValue { state -> UInt64 in
            state.nextID &+= 1
            return state.nextID
        }
        let expiry = deadline.map { deadline in
            Task { [self] in
                try await Task.sleep(until: deadline, clock: .continuous)
                leave(id, with: .success(false))
            }
        }
        defer { expiry?.cancel() }
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Bool, Error>) in
                let granted: Result<Bool, Error>? = state.withLockedValue { state in
                    if Task.isCancelled {
                        return .failure(CancellationError())
                    }
                    if state.waiters.isEmpty, state.available >= count {
                        state.available -= count
                        return .success(true)
                    }
                    if let deadline, ContinuousClock.now >= deadline {
                        // The expiry may already have run and found no waiter.
                        return .success(false)
                    }
                    state.waiters.append(Waiter(id: id, count: count, continuation: continuation))
                    return nil
                }
                if let granted {
                    continuation.resume(with: granted)
                }
            }
        } onCancel: {
            leave(id, with: .failure(CancellationError()))
        }
    }

    /// Gives `count` permits back and serves waiters in order while they fit.
    func release(_ count: Int) {
        let served = state.withLockedValue { state -> [CheckedContinuation<Bool, Error>] in
            state.available += count
            return Self.serve(&state)
        }
        for continuation in served {
            continuation.resume(returning: true)
        }
    }

    /// Takes a waiter that is still queued out with `outcome`. One that was
    /// already served keeps its permits.
    private func leave(_ id: UInt64, with outcome: Result<Bool, Error>) {
        let (left, served) = state.withLockedValue { state -> (CheckedContinuation<Bool, Error>?, [CheckedContinuation<Bool, Error>]) in
            guard let index = state.waiters.firstIndex(where: { $0.id == id }) else {
                return (nil, [])
            }
            let waiter = state.waiters.remove(at: index)
            // The waiter that left may have been holding smaller ones back.
            return (waiter.continuation, Self.serve(&state))
        }
        left?.resume(with: outcome)
        for continuation in served {
            continuation.resume(returning: true)
        }
    }

    private static func serve(_ state: inout State) -> [CheckedContinuation<Bool, Error>] {
        var served: [CheckedContinuation<Bool, Error>] = []
        for waiter in state.waiters {
            guard state.available >= waiter.count else {
                break
            }
            state.available -= waiter.count
            served.append(waiter.continuation)
        }
        state.waiters.removeFirst(served.count)
        return served
    }
}
