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

/// The conformance agent of the Swift SDK. It reads JSON requests from stdin,
/// calls the SDK and writes one JSON answer per request to stdout. It never
/// judges a result. See conformance/README.md for the protocol.
@main
struct IggyAgent {
    static func main() async {
        // Stdout carries the answers, so every log, the SDK's included, goes
        // to stderr.
        LoggingSystem.bootstrap { label in
            var handler = StreamLogHandler.standardError(label: label)
            handler.logLevel = .warning
            return handler
        }
        let output = Output()
        await output.write(Hello.line)
        let agent = Agent()
        for await line in standardInputLines() {
            guard let request = Request(line: line) else {
                continue
            }
            // Each request runs on a task of its own, so a long call never
            // delays a reset or a cancellation behind it.
            Task.detached {
                let reply = await agent.handle(request)
                await output.write(reply.line(id: request.id))
            }
        }
        // The container of the agent stops only when this process exits, so
        // the closing of the entities gets a few seconds and no more.
        Task.detached {
            await agent.reset()
            exit(0)
        }
        try? await Task.sleep(for: .seconds(5))
        exit(0)
    }

    /// The lines of stdin. A thread of its own reads them, so a blocking read
    /// never holds a thread of the concurrency pool.
    private static func standardInputLines() -> AsyncStream<String> {
        let (lines, continuation) = AsyncStream.makeStream(of: String.self)
        Thread.detachNewThread {
            while let line = readLine() {
                continuation.yield(line)
            }
            continuation.finish()
        }
        return lines
    }
}
