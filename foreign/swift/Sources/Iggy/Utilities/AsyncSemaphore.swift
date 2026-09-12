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

/// A FIFO counting semaphore whose permits can be held across suspension
/// points. Waiters are served in arrival order, so a large request is not
/// starved by a stream of small ones.
actor AsyncSemaphore {
    private struct Waiter {
        let id: UInt64
        let count: Int
        let continuation: CheckedContinuation<Bool, Never>
    }

    private var available: Int
    private var waiters: [Waiter] = []
    private var nextWaiter: UInt64 = 0

    init(permits: Int) {
        precondition(permits >= 0, "permits must not be negative")
        available = permits
    }

    var availablePermits: Int { available }

    /// Takes `count` permits without waiting; false when they are not free.
    func tryAcquire(_ count: Int = 1) -> Bool {
        guard waiters.isEmpty, available >= count else {
            return false
        }
        available -= count
        return true
    }

    /// Takes `count` permits, waiting for them. Returns false when the wait
    /// was cancelled, in which case nothing is held.
    func acquire(_ count: Int = 1) async -> Bool {
        if waiters.isEmpty, available >= count {
            available -= count
            return true
        }
        let id = nextWaiter
        nextWaiter += 1
        return await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                if Task.isCancelled {
                    continuation.resume(returning: false)
                    return
                }
                waiters.append(Waiter(id: id, count: count, continuation: continuation))
            }
        } onCancel: {
            Task { await self.cancelWaiter(id) }
        }
    }

    /// Takes `count` permits, giving up after `timeout`. Returns false when
    /// the permits could not be taken in time or the wait was cancelled.
    func acquire(_ count: Int = 1, timeout: Duration) async -> Bool {
        if waiters.isEmpty, available >= count {
            available -= count
            return true
        }
        let id = nextWaiter
        nextWaiter += 1
        let timer = Task { [weak self] in
            try? await Task.sleep(for: timeout)
            guard !Task.isCancelled else { return }
            await self?.cancelWaiter(id)
        }
        defer { timer.cancel() }
        return await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                if Task.isCancelled {
                    continuation.resume(returning: false)
                    return
                }
                waiters.append(Waiter(id: id, count: count, continuation: continuation))
            }
        } onCancel: {
            Task { await self.cancelWaiter(id) }
        }
    }

    /// Gives `count` permits back and serves waiters in order while they fit.
    func release(_ count: Int = 1) {
        available += count
        serveWaiters()
    }

    private func cancelWaiter(_ id: UInt64) {
        guard let index = waiters.firstIndex(where: { $0.id == id }) else {
            return
        }
        let waiter = waiters.remove(at: index)
        waiter.continuation.resume(returning: false)
        // The removed waiter may have been blocking smaller ones behind it.
        serveWaiters()
    }

    private func serveWaiters() {
        while let first = waiters.first, available >= first.count {
            waiters.removeFirst()
            available -= first.count
            first.continuation.resume(returning: true)
        }
    }
}

/// A signal that wakes at most one waiter per `notify`. A notification sent
/// while nobody waits is remembered, so a waiter that arrives later does not
/// miss it. A cancelled wait returns at once and consumes no notification.
actor AsyncNotify {
    private var pending = false
    private var waiters: [(id: UInt64, continuation: CheckedContinuation<Void, Never>)] = []
    private var nextWaiter: UInt64 = 0

    func notify() {
        if waiters.isEmpty {
            pending = true
            return
        }
        waiters.removeFirst().continuation.resume()
    }

    func wait() async {
        if pending {
            pending = false
            return
        }
        let id = nextWaiter
        nextWaiter += 1
        await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                if Task.isCancelled {
                    continuation.resume()
                    return
                }
                waiters.append((id, continuation))
            }
        } onCancel: {
            Task { await self.cancelWaiter(id) }
        }
    }

    private func cancelWaiter(_ id: UInt64) {
        guard let index = waiters.firstIndex(where: { $0.id == id }) else {
            return
        }
        waiters.remove(at: index).continuation.resume()
    }
}
