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

/// A FIFO mutex that can be held across suspension points.
///
/// Actors release their isolation at every `await`, so an actor method that
/// writes a request and then awaits the reply would let a second caller
/// interleave on the same connection. The lockstep exchange holds this
/// instead.
actor AsyncMutex {
    private var locked = false
    private var waiters: [(id: UInt64, continuation: CheckedContinuation<Void, Never>)] = []
    private var nextWaiter: UInt64 = 0

    func lock() async {
        if !locked {
            locked = true
            return
        }
        let id = nextWaiter
        nextWaiter += 1
        await withCheckedContinuation { continuation in
            waiters.append((id, continuation))
        }
    }

    func unlock() {
        if waiters.isEmpty {
            locked = false
            return
        }
        let next = waiters.removeFirst()
        next.continuation.resume()
    }

    /// Runs `body` while holding the lock.
    func withLock<T: Sendable>(_ body: @Sendable () async throws -> T) async rethrows -> T {
        await lock()
        defer { unlock() }
        return try await body()
    }
}

/// A single-flight gate: the first caller runs the work, every caller that
/// arrives meanwhile awaits the same outcome.
actor SingleFlight {
    private var inFlight: Task<Void, Error>?

    var isActive: Bool { inFlight != nil }

    func run(_ work: @Sendable @escaping () async throws -> Void) async throws {
        if let inFlight {
            try await inFlight.value
            return
        }
        let task = Task { try await work() }
        inFlight = task
        defer { inFlight = nil }
        try await task.value
    }
}
