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

// The conformance agent of the Node.js SDK. It reads JSON requests from stdin,
// calls the SDK and writes one JSON answer per request to stdout. It never
// judges a result. See conformance/README.md for the protocol.

import { createInterface } from 'node:readline';
import { fields, optional, text } from './args.js';
import { call } from './calls.js';
import { Failure, answerFor, type Answer } from './canonical.js';
import {
  Client, SDK_VERSION, type ClientConfig, type ConsumerMessage, type IggyConsumer, type PartitionReader
} from './iggy.js';

declare global {
  interface JSON {
    // Node 21 and later, which no TypeScript lib declares yet.
    rawJSON(text: string): unknown;
    parse(text: string, reviver: (key: string, value: unknown, context: { source?: string }) => unknown): unknown;
  }
}

const PROTOCOL_VERSION = 1;

const CAPABILITIES = [
  'reader', 'consumer', 'resolve_operation', 'abandon_operation', 'raw_command',
  'raw_command_replica', 'describe_options', 'metadata_changes'
];

const NULL_RESULT: Answer = { result: null };

type Request = {
  id: number,
  op: string,
  kind?: string,
  entity?: string,
  method?: string,
  args?: unknown,
  save_as?: string,
  task?: string
};

type Task = { cancel: AbortController, answer: Promise<Answer> };

type ConsumerEntity = { consumer: IggyConsumer, messages: AsyncIterator<ConsumerMessage> };

/** Runs the calls of one entity one after another, as a reader takes one caller at a time. */
class Exclusive<T> {
  private last: Promise<unknown> = Promise.resolve();

  constructor(readonly value: T) {}

  run<R>(work: (value: T) => Promise<R>): Promise<R> {
    const result = this.last.then(() => work(this.value));
    this.last = result.catch(() => undefined);
    return result;
  }
}

export class Agent {
  readonly clients = new Map<string, Client>();
  readonly readers = new Map<string, Exclusive<PartitionReader>>();
  readonly consumers = new Map<string, Exclusive<ConsumerEntity>>();
  private readonly tasks = new Map<string, Task>();

  async handle(request: Request): Promise<Answer> {
    try {
      return await this.dispatch(request);
    } catch (error) {
      return answerFor(error);
    }
  }

  keepReader(entity: string, reader: PartitionReader): void {
    this.readers.set(entity, new Exclusive(reader));
  }

  keepConsumer(entity: string, consumer: IggyConsumer): void {
    this.consumers.set(entity, new Exclusive({ consumer, messages: consumer[Symbol.asyncIterator]() }));
  }

  /** Cancels every task and closes every entity, the readers and consumers before the clients under them. */
  async reset(): Promise<void> {
    for (const task of this.tasks.values()) task.cancel.abort();
    this.tasks.clear();
    await Promise.all([...this.readers.keys(), ...this.consumers.keys()].map((entity) => this.close(entity)));
    await Promise.all([...this.clients.keys()].map((entity) => this.close(entity)));
  }

  private async dispatch(request: Request): Promise<Answer> {
    switch (request.op) {
      case 'create':
        if (request.kind !== 'client') return { unsupported: `creating a ${request.kind} with create` };
        return this.createClient(text(request.entity), request.args);
      case 'call':
        return call(this, text(request.entity), text(request.method), request.args, request.save_as);
      case 'start': {
        const name = text(request.task);
        this.tasks.get(name)?.cancel.abort();
        const cancel = new AbortController();
        const answer = call(this, text(request.entity), text(request.method), request.args, undefined, cancel.signal);
        this.tasks.set(name, { cancel, answer });
        return NULL_RESULT;
      }
      case 'await': {
        const task = this.take(text(request.task));
        return task === undefined ? { failure: `no task ${request.task}` } : await task.answer;
      }
      case 'cancel': {
        const task = this.take(text(request.task));
        if (task === undefined) return { failure: `no task ${request.task}` };
        task.cancel.abort();
        return NULL_RESULT;
      }
      case 'close':
        await this.close(text(request.entity));
        return NULL_RESULT;
      case 'reset':
        await this.reset();
        return NULL_RESULT;
      default:
        return { failure: `unknown op ${request.op}` };
    }
  }

  private async createClient(entity: string, args: unknown): Promise<Answer> {
    const { transport, address, login } = fields(args, ['transport', 'address', 'login']);
    if (transport !== 'tcp') return { unsupported: `the ${String(transport)} transport` };
    const credentials = optional(login, (value) => {
      const { username, password } = fields(value, ['username', 'password']);
      return { username: text(username), password: text(password) };
    });
    // Without configured credentials the client signs in only when the test
    // signs it in, so a client without login stays signed out.
    const client = new Client({ transport: 'TCP', options: endpoint(text(address)) } as ClientConfig);
    if (credentials !== undefined) {
      try {
        await client.session.login(credentials);
      } catch (error) {
        await client.destroy();
        return answerFor(error);
      }
    }
    this.clients.set(entity, client);
    return NULL_RESULT;
  }

  private take(name: string): Task | undefined {
    const task = this.tasks.get(name);
    this.tasks.delete(name);
    return task;
  }

  private async close(entity: string): Promise<void> {
    const consumer = this.consumers.get(entity);
    const reader = this.readers.get(entity);
    const client = this.clients.get(entity);
    this.consumers.delete(entity);
    this.readers.delete(entity);
    this.clients.delete(entity);
    try {
      await consumer?.value.consumer.shutdown();
      await reader?.value.close();
      await client?.destroy();
    } catch (error) {
      console.error(`cannot close ${entity}:`, error);
    }
  }
}

/** `host:port`, with an IPv6 host in brackets. */
const endpoint = (address: string): { host: string, port: number } => {
  const match = /^(?:\[([^\]]+)\]|([^:]+)):([0-9]+)$/.exec(address);
  if (match === null) throw new Failure(`an address is host:port: ${address}`);
  return { host: match[1] ?? match[2], port: Number(match[3]) };
};

/** Reads a line, with an integer beyond 2^53 as an exact bigint. */
const parseLine = (line: string): unknown => JSON.parse(line, (_key, value, context) =>
  typeof value === 'number' && !Number.isSafeInteger(value) && /^-?[0-9]+$/.test(context.source ?? '')
    ? BigInt(context.source!) : value);

/** Writes a line, with each bigint as an exact JSON number. */
const writeLine = (line: object): void => {
  process.stdout.write(`${JSON.stringify(line, (_key, value) =>
    typeof value === 'bigint' ? JSON.rawJSON(value.toString()) : value)}\n`);
};

// A stray rejection inside the SDK must not end the agent, and with it every
// test that follows.
process.on('unhandledRejection', (reason) => console.error('unhandled rejection:', reason));

writeLine({
  hello: {
    sdk: 'node', sdk_version: SDK_VERSION, protocol: PROTOCOL_VERSION, transports: ['tcp'], capabilities: CAPABILITIES
  }
});

const agent = new Agent();
const answering = new Set<Promise<void>>();
for await (const line of createInterface({ input: process.stdin, crlfDelay: Infinity })) {
  if (line.trim() === '') continue;
  let request: Request;
  try {
    request = parseLine(line) as Request;
  } catch (error) {
    console.error(`cannot read the request ${line}:`, error);
    continue;
  }
  // Each request runs on its own, so a long call never delays a reset or a
  // cancellation behind it.
  const answered = agent.handle(request).then((answer) => writeLine({ id: request.id, ...answer }));
  answering.add(answered);
  void answered.finally(() => answering.delete(answered));
}
await agent.reset();
await Promise.all(answering);
process.exit(0);
