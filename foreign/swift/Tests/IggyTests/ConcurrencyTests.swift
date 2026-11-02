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

import Foundation
import NIOCore
import NIOPosix
import Testing

@testable import Iggy

@Suite("Concurrency helpers")
struct ConcurrencyTests {
    @Test func cancelledWaiterLeavesTheQueueWithoutTheLock() async throws {
        let mutex = AsyncMutex()
        try await mutex.lock()
        let cancelled = Task { try await mutex.lock() }
        let waiting = Task {
            try await mutex.lock()
            return true
        }
        cancelled.cancel()
        // Returns although the lock is still held.
        await #expect(throws: CancellationError.self) { try await cancelled.value }
        mutex.unlock()
        #expect(try await waiting.value)
        mutex.unlock()
        // Released twice by its two holders, so it is free again.
        try await mutex.lock()
        mutex.unlock()
    }

    @Test func mutexSerializesItsHolders() async throws {
        let mutex = AsyncMutex()
        let inside = Locked(0)
        let overlaps = Locked(0)
        try await withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0..<50 {
                group.addTask {
                    try await mutex.lock()
                    defer { mutex.unlock() }
                    let holders = inside.withLock {
                        $0 += 1
                        return $0
                    }
                    if holders != 1 {
                        overlaps.withLock { $0 += 1 }
                    }
                    await Task.yield()
                    inside.withLock { $0 -= 1 }
                }
            }
            try await group.waitForAll()
        }
        #expect(overlaps.withLock { $0 } == 0)
    }

    @Test func callersShareOneFlight() async throws {
        let gate = SingleFlight()
        let runs = Locked(0)
        let release = Locked(false)
        try await withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0..<5 {
                group.addTask {
                    try await gate.run {
                        runs.withLock { $0 += 1 }
                        while !release.withLock({ $0 }) {
                            try await Task.sleep(for: .milliseconds(2))
                        }
                    }
                }
            }
            #expect(await eventually { gate.isActive })
            try await Task.sleep(for: .milliseconds(20))
            release.withLock { $0 = true }
            try await group.waitForAll()
        }
        #expect(runs.withLock { $0 } == 1)
        #expect(!gate.isActive)
    }

    @Test func flightSurvivesWhileACallerRemains() async throws {
        let gate = SingleFlight()
        let release = Locked(false)
        let work: SingleFlight.Work = {
            while !release.withLock({ $0 }) {
                try await Task.sleep(for: .milliseconds(2))
            }
        }
        let leaving = Task { try await gate.run(work) }
        #expect(await eventually { gate.isActive })
        let staying = Task { try await gate.run(work) }
        try await Task.sleep(for: .milliseconds(20))
        leaving.cancel()
        await #expect(throws: CancellationError.self) { try await leaving.value }
        // The flight was not cancelled with the caller that left.
        release.withLock { $0 = true }
        try await staying.value
    }

    @Test func flightIsCancelledWhenItsLastCallerLeaves() async throws {
        let gate = SingleFlight()
        let sawCancellation = Locked(false)
        let caller = Task {
            try await gate.run {
                do {
                    try await Task.sleep(for: .seconds(600))
                } catch {
                    sawCancellation.withLock { $0 = true }
                    throw error
                }
            }
        }
        #expect(await eventually { gate.isActive })
        caller.cancel()
        await #expect(throws: CancellationError.self) { try await caller.value }
        #expect(await eventually { sawCancellation.withLock { $0 } })
        #expect(await eventually { !gate.isActive })
    }

    /// A caller that arrives while an abandoned flight unwinds must not share
    /// its cancellation: it gets a flight of its own once that one has ended.
    @Test func callerArrivingDuringUnwindingStartsANewFlight() async throws {
        let gate = SingleFlight()
        let unwound = Locked(false)
        let runs = Locked(0)
        let first = Task {
            try await gate.run {
                runs.withLock { $0 += 1 }
                // Ignores cancellation until it is told to unwind.
                while !unwound.withLock({ $0 }) {
                    try? await Task.sleep(for: .milliseconds(2))
                    await Task.yield()
                }
            }
        }
        #expect(await eventually { gate.isActive })
        first.cancel()
        await #expect(throws: CancellationError.self) { try await first.value }
        let second = Task {
            try await gate.run { runs.withLock { $0 += 1 } }
        }
        try await Task.sleep(for: .milliseconds(20))
        #expect(runs.withLock { $0 } == 1)
        unwound.withLock { $0 = true }
        try await second.value
        #expect(runs.withLock { $0 } == 2)
    }

    @Test func deadlineEndsAWaitThatHonoursCancellation() async throws {
        let late = try await withDeadline(ContinuousClock.now + .milliseconds(50)) {
            try await Task.sleep(for: .seconds(600))
            return 1
        }
        #expect(late == nil)
        let inTime = try await withDeadline(ContinuousClock.now + .seconds(600)) { 2 }
        #expect(inTime == 2)
        await #expect(throws: IggyError(.invalidCommand)) {
            _ = try await withDeadline(ContinuousClock.now + .seconds(600)) { () async throws -> Int in throw IggyError(.invalidCommand) }
        }
    }

    /// A peer that accepts the connection and never answers the ClientHello
    /// must not hold the dial open past the connect timeout.
    @Test func stalledTLSHandshakeEndsAtTheConnectTimeout() async throws {
        let silent = try await ServerBootstrap(group: MultiThreadedEventLoopGroup.singleton)
            .serverChannelOption(.socketOption(.so_reuseaddr), value: 1)
            .bind(host: "127.0.0.1", port: 0).get()
        defer { silent.close(promise: nil) }
        let port = try #require(silent.localAddress?.port)
        let endpoint = try Endpoint(parsing: "127.0.0.1:\(port)")
        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            await expectCode(.cannotEstablishConnection) {
                _ = try await TCPConnection.connect(to: endpoint, tls: TLSOptions(validateCertificate: false), timeout: .milliseconds(300), noDelay: true)
            }
        }
        #expect(elapsed >= .milliseconds(250) && elapsed < .seconds(10))
    }

    @Test func retryOutcomeTurnsLaterRefusalsIntoUnknown() {
        var outcome = RetryOutcome()
        #expect(outcome.observe(IggyError(.transientNotAccepted)).code == .transientNotAccepted)
        #expect(outcome.observe(IggyError(.unauthenticated)).code == .unauthenticated)
        #expect(outcome.observe(IggyError(.transientNotCommitted)).code == .transientNotCommitted)
        #expect(outcome.isUncertain)
        for refusal: IggyErrorCode in [
            .transientNotAccepted, .historyUnavailable, .unauthorized, .unauthenticated, .staleClient, .notConnected, .cannotEstablishConnection,
            .topicIdNotFound, .streamNameNotFound, .disconnected, .streamNameAlreadyExists,
        ] {
            #expect(outcome.observe(IggyError(refusal)).code == .transientNotCommitted)
        }
        #expect(outcome.observe(IggyError(.streamNameAlreadyExists), committed: true).code == .streamNameAlreadyExists)
        #expect(!outcome.isUncertain)
    }

    @Test func aSemaphoreServesWaitersInOrder() async throws {
        let semaphore = AsyncSemaphore(permits: 5)
        #expect(try await semaphore.acquire(5))
        let large = Task { try await semaphore.acquire(3) }
        #expect(await eventually { !semaphore.tryAcquire(0) })
        let small = Task { try await semaphore.acquire(1) }
        semaphore.release(1)
        // The small waiter fits but queues behind the large one.
        try await Task.sleep(for: .milliseconds(30))
        #expect(semaphore.availablePermits == 1)
        semaphore.release(3)
        #expect(try await large.value)
        #expect(try await small.value)
        #expect(semaphore.availablePermits == 0)
    }

    @Test func aSemaphoreWaitEndsAtItsDeadlineOrCancellation() async throws {
        let semaphore = AsyncSemaphore(permits: 1)
        #expect(semaphore.tryAcquire(1))
        #expect(try await !semaphore.acquire(1, deadline: .now + .milliseconds(30)))
        #expect(try await !semaphore.acquire(1, deadline: .now - .milliseconds(1)))
        let cancelled = Task { try await semaphore.acquire(1) }
        try await Task.sleep(for: .milliseconds(20))
        cancelled.cancel()
        await #expect(throws: CancellationError.self) { try await cancelled.value }
        semaphore.release(1)
        // Nobody who gave up holds a permit.
        #expect(semaphore.availablePermits == 1)
    }

    /// The server took a later request of the session first, so an earlier
    /// attempt of this one may have committed.
    @Test func retryOutcomeReportsARequestTooOldAsUnknown() {
        var outcome = RetryOutcome()
        #expect(outcome.observe(IggyError(.requestTooOld)).code == .transientNotCommitted)
        #expect(outcome.isUncertain)
    }
}
