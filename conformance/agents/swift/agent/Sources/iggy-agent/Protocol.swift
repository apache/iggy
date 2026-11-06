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
import Iggy
import Logging

/// The log of the agent. Logs go to stderr, as stdout carries the answers.
let log = Logger(label: "org.apache.iggy.conformance")

/// The first line of the agent: who it is and what it implements beyond the
/// methods every agent has. The SDK has no call that resolves an operation,
/// so `resolve_operation` is not among the capabilities.
enum Hello {
    static let protocolVersion: UInt64 = 1
    static let capabilities = [
        "reader", "consumer", "abandon_operation", "raw_command", "raw_command_replica", "describe_options", "metadata_changes",
        "connection_info",
    ]

    static var line: JSON {
        .object([
            "hello": .object([
                "sdk": .string("swift"),
                "sdk_version": .string(IggyVersion.sdkVersion),
                "protocol": .number(protocolVersion),
                "transports": .array([.string("tcp")]),
                "capabilities": .array(capabilities.map(JSON.string)),
            ])
        ])
    }
}

/// One request line of the orchestrator.
struct Request: Decodable, Sendable {
    let id: UInt64
    let op: String
    let kind: String?
    let entity: String?
    let method: String?
    let args: JSON?
    let saveAs: String?
    let task: String?

    enum CodingKeys: String, CodingKey {
        case id, op, kind, entity, method, args, task
        case saveAs = "save_as"
    }

    /// The request of one line, or nil for a blank line or one that is not
    /// a request, which the log reports.
    init?(line: String) {
        guard !line.allSatisfy(\.isWhitespace) else {
            return nil
        }
        do {
            self = try JSONDecoder().decode(Request.self, from: Data(line.utf8))
        } catch {
            log.error("Cannot read the request \(line): \(error)")
            return nil
        }
    }
}

/// A fault of the agent itself, such as an argument that it cannot read.
struct Fault: Error, CustomStringConvertible {
    let reason: String

    init(_ reason: String) {
        self.reason = reason
    }

    var description: String { reason }
}

/// A method, an argument value, a transport or an entity kind that the SDK
/// does not have.
struct Unsupported: Error {
    let reason: String

    init(_ reason: String) {
        self.reason = reason
    }
}

/// The answer to one request: exactly one of a result, an SDK error, an
/// unsupported call and a failure of the agent itself.
enum Reply: Sendable {
    case result(JSON)
    case error(JSON)
    case unsupported(String)
    case failure(String)

    /// The answer for an error that a call threw. An error that carries no
    /// Iggy code is a failure of the agent, not an answer of the SDK.
    init(_ error: any Error) {
        switch error {
        case let fault as Fault:
            self = .failure(fault.reason)
        case let unsupported as Unsupported:
            self = .unsupported(unsupported.reason)
        case let error as IggyError:
            self = .error(Self.sdkError(error, readPartition: nil))
        case let failure as ReadError:
            self = .error(Self.sdkError(failure.error, readPartition: failure.partitionID))
        case is CancellationError:
            self = .failure("the call was cancelled")
        default:
            self = .failure("an error without an Iggy code: \(error)")
        }
    }

    func line(id: UInt64) -> JSON {
        switch self {
        case .result(let value): .object(["id": .number(id), "result": value])
        case .error(let error): .object(["id": .number(id), "error": error])
        case .unsupported(let reason): .object(["id": .number(id), "unsupported": .string(reason)])
        case .failure(let reason): .object(["id": .number(id), "failure": .string(reason)])
        }
    }

    /// The code that arrived on the wire, the operation of the errors 14006
    /// to 14010, and the partition of `partition_lane_blocked` or of a read
    /// that failed.
    private static func sdkError(_ error: IggyError, readPartition: UInt32?) -> JSON {
        var fields: [String: JSON] = ["code": JSON(error.rawCode), "message": .string(error.description)]
        // The SDK names an operation with a random 128-bit handle, and the
        // protocol carries a u64, so the answer holds the low half. The agent
        // keeps the whole handle for abandon_operation, see Agent.remember.
        if let operation = error.operationID {
            fields["operation_id"] = .number(operation.low)
        }
        let blockedPartition = error.code == .partitionLaneBlocked ? error.partitionID : nil
        if let partition = blockedPartition ?? readPartition {
            fields["partition_id"] = JSON(partition)
        }
        return .object(fields)
    }
}

/// Writes the answer lines. Answers come from many tasks, so one actor
/// writes them whole and one at a time.
actor Output {
    private let encoder = JSONEncoder.compact

    func write(_ line: JSON) {
        do {
            try FileHandle.standardOutput.write(contentsOf: encoder.encode(line) + [0x0A])
        } catch {
            log.error("Cannot write an answer: \(error)")
        }
    }
}
