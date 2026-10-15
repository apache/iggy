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
import Logging
import Testing

@testable import Iggy

let testLogger = Logger(label: "org.apache.iggy.tests")

/// A value behind a lock, for counters shared with `@Sendable` closures.
final class Locked<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var value: Value

    init(_ value: Value) {
        self.value = value
    }

    func withLock<T>(_ body: (inout Value) throws -> T) rethrows -> T {
        lock.lock()
        defer { lock.unlock() }
        return try body(&value)
    }
}

/// Polls `condition` until it holds or `timeout` passes.
func eventually(timeout: Duration = .seconds(5), _ condition: @Sendable () async -> Bool) async -> Bool {
    let deadline = ContinuousClock.now + timeout
    while ContinuousClock.now < deadline {
        if await condition() {
            return true
        }
        try? await Task.sleep(for: .milliseconds(5))
    }
    return await condition()
}

/// Asserts that `body` throws an ``IggyError`` with `code`.
func expectCode(_ code: IggyErrorCode, sourceLocation: SourceLocation = #_sourceLocation, _ body: () async throws -> Void) async {
    do {
        try await body()
        Issue.record("expected \(code) but nothing was thrown", sourceLocation: sourceLocation)
    } catch let error as IggyError {
        #expect(error.code == code, "expected \(code), got \(error)", sourceLocation: sourceLocation)
    } catch {
        Issue.record("expected \(code), got \(error)", sourceLocation: sourceLocation)
    }
}
