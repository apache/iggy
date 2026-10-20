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

/// A FIFO mutex that can be held across suspension points.
///
/// Actors release their isolation at every `await`, so an actor method that
/// writes a request and then awaits the reply would let a second caller
/// interleave on the same connection. The lockstep exchange holds this
/// instead.
final class AsyncMutex: Sendable {
    private struct State {
        var locked = false
        var waiters: [(id: UInt64, continuation: CheckedContinuation<Void, Error>)] = []
        var nextID: UInt64 = 0
    }

    private let state = NIOLockedValueBox(State())

    /// Waits for the lock. A caller cancelled while it waits leaves the queue
    /// with `CancellationError` and does not hold the lock.
    func lock() async throws {
        let id = state.withLockedValue { state -> UInt64 in
            state.nextID &+= 1
            return state.nextID
        }
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                let outcome: Result<Void, Error>? = state.withLockedValue { state in
                    if Task.isCancelled {
                        return .failure(CancellationError())
                    }
                    if !state.locked {
                        state.locked = true
                        return .success(())
                    }
                    state.waiters.append((id, continuation))
                    return nil
                }
                if let outcome {
                    continuation.resume(with: outcome)
                }
            }
        } onCancel: {
            // A waiter the lock was already handed to is no longer queued and
            // keeps it; only a caller still waiting is taken out.
            let waiter = state.withLockedValue { state -> CheckedContinuation<Void, Error>? in
                guard let index = state.waiters.firstIndex(where: { $0.id == id }) else {
                    return nil
                }
                return state.waiters.remove(at: index).continuation
            }
            waiter?.resume(throwing: CancellationError())
        }
    }

    /// Hands the lock to the longest waiter, or frees it.
    func unlock() {
        let next = state.withLockedValue { state -> CheckedContinuation<Void, Error>? in
            if state.waiters.isEmpty {
                state.locked = false
                return nil
            }
            return state.waiters.removeFirst().continuation
        }
        next?.resume()
    }
}

/// A single-flight gate: the first caller starts the work, every caller that
/// arrives meanwhile awaits the same outcome.
///
/// A cancelled caller leaves at once with `CancellationError`. The work is
/// cancelled when its last caller has left, and callers that arrive while it
/// unwinds start a new flight once it has ended.
final class SingleFlight: Sendable {
    typealias Work = @Sendable () async throws -> Void
    private typealias Caller = CheckedContinuation<Void, Error>

    private struct State {
        var flight: Task<Void, Never>?
        /// The flight lost its last caller and is unwinding.
        var abandoned = false
        var callers: [UInt64: Caller] = [:]
        var queued: [UInt64: (caller: Caller, work: Work)] = [:]
        var nextID: UInt64 = 0
    }

    private let state = NIOLockedValueBox(State())

    var isActive: Bool {
        state.withLockedValue { $0.flight != nil }
    }

    func run(_ work: @escaping Work) async throws {
        let id = state.withLockedValue { state -> UInt64 in
            state.nextID &+= 1
            return state.nextID
        }
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (caller: Caller) in
                let cancelled = state.withLockedValue { state -> Bool in
                    if Task.isCancelled {
                        return true
                    }
                    if state.flight == nil {
                        state.callers[id] = caller
                        state.flight = Task { await self.fly(work) }
                    } else if state.abandoned {
                        state.queued[id] = (caller, work)
                    } else {
                        state.callers[id] = caller
                    }
                    return false
                }
                if cancelled {
                    caller.resume(throwing: CancellationError())
                }
            }
        } onCancel: {
            leave(id)
        }
    }

    private func fly(_ work: Work) async {
        let result: Result<Void, Error>
        do {
            try await work()
            result = .success(())
        } catch {
            result = .failure(error)
        }
        let finished = state.withLockedValue { state -> [Caller] in
            let finished = Array(state.callers.values)
            state.callers.removeAll()
            state.abandoned = false
            state.flight = nil
            // Callers that arrived while an abandoned flight was unwinding
            // share the next one, started by the earliest of them.
            if let first = state.queued.min(by: { $0.key < $1.key }) {
                for (id, entry) in state.queued {
                    state.callers[id] = entry.caller
                }
                state.queued.removeAll()
                let next = first.value.work
                state.flight = Task { await self.fly(next) }
            }
            return finished
        }
        for caller in finished {
            caller.resume(with: result)
        }
    }

    private func leave(_ id: UInt64) {
        let (caller, abandonedFlight) = state.withLockedValue { state -> (Caller?, Task<Void, Never>?) in
            if let queued = state.queued.removeValue(forKey: id) {
                return (queued.caller, nil)
            }
            guard let caller = state.callers.removeValue(forKey: id) else {
                return (nil, nil)
            }
            guard state.callers.isEmpty, !state.abandoned else {
                return (caller, nil)
            }
            state.abandoned = true
            return (caller, state.flight)
        }
        caller?.resume(throwing: CancellationError())
        abandonedFlight?.cancel()
    }
}

/// Runs `body` until `deadline` and returns nil when the deadline passes
/// first. `body` is cancelled then, and the wait ends once it honours that.
func withDeadline<T: Sendable>(_ deadline: ContinuousClock.Instant, _ body: @escaping @Sendable () async throws -> T) async throws -> T? {
    try await withThrowingTaskGroup(of: T?.self) { group in
        defer { group.cancelAll() }
        group.addTask { try await body() }
        group.addTask {
            try await Task.sleep(until: deadline, clock: .continuous)
            return nil
        }
        return try await group.next() ?? nil
    }
}
