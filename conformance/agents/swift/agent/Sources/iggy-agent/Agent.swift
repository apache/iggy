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

import Iggy

/// The entities and tasks of the agent. A call gives the actor up at each
/// SDK call it awaits, so a long call never delays the requests after it.
actor Agent {
    var clients: [String: IggyClient] = [:]
    var readers: [String: PartitionReader] = [:]
    var consumers: [String: IggyConsumer] = [:]
    private var tasks: [String: Task<Reply, Never>] = [:]
    /// The 128-bit operation handles that errors named, by the low half that
    /// the answers carry.
    private var operations: [UInt64: UInt128Value] = [:]

    func handle(_ request: Request) async -> Reply {
        switch request.op {
        case "create":
            guard request.kind == "client" else {
                return .unsupported("creating a \(request.kind ?? "nameless kind") with create")
            }
            guard let entity = request.entity else {
                return .failure("create needs an entity")
            }
            return await createClient(entity, request.args)
        case "call":
            guard let entity = request.entity, let method = request.method else {
                return .failure("a call needs an entity and a method")
            }
            return await call(entity, method, request.args, saveAs: request.saveAs)
        case "start":
            guard let name = request.task, let entity = request.entity, let method = request.method else {
                return .failure("start needs a task, an entity and a method")
            }
            tasks[name]?.cancel()
            tasks[name] = Task { await self.call(entity, method, request.args, saveAs: nil) }
            return .result(.null)
        case "await":
            guard let name = request.task, let task = tasks.removeValue(forKey: name) else {
                return .failure("no task \(request.task ?? "")")
            }
            return await task.value
        case "cancel":
            guard let name = request.task, let task = tasks.removeValue(forKey: name) else {
                return .failure("no task \(request.task ?? "")")
            }
            task.cancel()
            return .result(.null)
        case "close":
            guard let entity = request.entity else {
                return .failure("close needs an entity")
            }
            await close(entity)
            return .result(.null)
        case "reset":
            await reset()
            return .result(.null)
        default:
            return .failure("unknown op \(request.op)")
        }
    }

    /// Cancels every task and closes every entity, the readers and consumers
    /// before the clients that they read with.
    func reset() async {
        for task in tasks.values {
            task.cancel()
        }
        tasks = [:]
        for entity in Array(readers.keys) + Array(consumers.keys) + Array(clients.keys) {
            await close(entity)
        }
        operations = [:]
    }

    /// Keeps the operation handle of an SDK error, whose answer carries only
    /// its low half.
    func remember(_ error: any Error) {
        if let operation = ((error as? IggyError) ?? (error as? ReadError)?.error)?.operationID {
            operations[operation.low] = operation
        }
    }

    /// The handle of an operation id that an answer carried. An id that no
    /// answer carried goes to the SDK as it is.
    func operation(_ id: UInt64) -> UInt128Value {
        operations[id] ?? UInt128Value(id)
    }

    private func createClient(_ entity: String, _ raw: JSON?) async -> Reply {
        do {
            var args = try Arguments(raw)
            let transport: String = try args.required("transport")
            let address: String = try args.required("address")
            let login: Login? = try args.optional("login")
            try args.finish()
            guard transport == "tcp" else {
                throw Unsupported("the \(transport) transport")
            }
            let client = IggyClient(configuration: ClientConfiguration(address: address))
            do {
                try await client.connect()
                if let login {
                    _ = try await client.login(username: login.username, password: login.password)
                }
            } catch {
                await client.shutdown()
                throw error
            }
            clients[entity] = client
            return .result(.null)
        } catch {
            return Reply(error)
        }
    }

    private func close(_ entity: String) async {
        if let reader = readers.removeValue(forKey: entity) {
            do {
                try await reader.close()
            } catch {
                log.warning("Cannot close the reader \(entity): \(error)")
            }
        }
        if let consumer = consumers.removeValue(forKey: entity) {
            await consumer.shutdown()
        }
        if let client = clients.removeValue(forKey: entity) {
            await client.shutdown()
        }
    }
}

/// The credentials of a client that signs in when it is created.
struct Login: ArgumentForm {
    let username: String
    let password: String

    init(argument: JSON) throws {
        var fields = try Arguments(argument)
        username = try fields.required("username")
        password = try fields.required("password")
        try fields.finish()
    }
}
