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

// The methods of the agent: read the arguments, call the SDK and return the
// canonical form of the result.

import {
  commit, consumer, expiry, fields, hex, identifier, messages, metadataStart, millis, optional,
  partitioning, permissions, scope, strategy, text, texts, topicSize, u32, u32s, u64, userStatus,
  type Args
} from './args.js';
import {
  Failure, Unsupported, answerFor, groupView, identityView, optionSpecView, polledView, rawView,
  receivedView, streamDetailsView, streamView, topicDetailsView, topicView, userView, type Answer
} from './canonical.js';
import { StreamEncryptionKey, type Client, type MetadataChangeGroup, type PollOptions } from './iggy.js';
import { changesView } from './metadata.js';
import type { Agent } from './main.js';

type Call = { agent: Agent, client: Client, args: Args, saveAs?: string, signal?: AbortSignal };

type Method = { args: readonly string[], run: (call: Call) => Promise<unknown> };

/** Calls a method of an entity. The answer of the SDK, or of the agent, never a rejection. */
export const call = async (
  agent: Agent, entity: string, name: string, args: unknown, saveAs?: string, signal?: AbortSignal
): Promise<Answer> => {
  try {
    if (name === 'next') return { result: await next(agent, entity, args, signal) };
    const client = agent.clients.get(entity);
    if (client === undefined) throw new Failure(`no client ${entity}`);
    if (!Object.hasOwn(METHODS, name)) throw new Unsupported(`the method ${name}`);
    const method = METHODS[name];
    return { result: await method.run({ agent, client, args: fields(args, method.args), saveAs, signal }) };
  } catch (error) {
    return answerFor(error);
  }
};

const next = async (agent: Agent, entity: string, args: unknown, signal?: AbortSignal): Promise<unknown> => {
  fields(args, []);
  const reader = agent.readers.get(entity);
  if (reader !== undefined) return polledView(await reader.run((partitionReader) => partitionReader.next({ signal })));
  const consumer = agent.consumers.get(entity);
  if (consumer === undefined) throw new Failure(`no reader or consumer ${entity}`);
  const received = await consumer.run(({ messages }) => messages.next());
  if (received.done) throw new Failure('the consumer ended');
  return receivedView(received.value);
};

const nullOr = <T, R>(value: T | null, view: (value: T) => R): R | null => value === null ? null : view(value);

/** The options that a call sets. The SDK spreads options over its defaults, where an undefined value would win. */
const pollOptions = (options: Partial<PollOptions>): Partial<PollOptions> =>
  Object.fromEntries(Object.entries(options).filter(([, value]) => value !== undefined));

const saveAsOf = ({ saveAs }: Call): string => {
  if (saveAs === undefined) throw new Failure('a call that builds an entity needs save_as');
  return saveAs;
};

const METHODS: Record<string, Method> = {
  connect: {
    args: [],
    run: async () => { throw new Unsupported('the SDK has no connect, a client dials when a call needs it'); }
  },
  disconnect: {
    args: [],
    run: async () => { throw new Unsupported('the SDK has no disconnect that keeps the client usable'); }
  },
  ping: {
    args: [],
    run: async ({ client }) => { await client.system.ping(); return null; }
  },
  login_user: {
    args: ['username', 'password'],
    run: async ({ client, args }) =>
      identityView(await client.session.login({ username: text(args.username), password: text(args.password) }))
  },
  logout_user: {
    args: [],
    run: async ({ client }) => { await client.session.logout(); return null; }
  },
  get_stream: {
    args: ['stream_id'],
    run: async ({ client, args }) =>
      nullOr(await client.stream.get({ streamId: identifier(args.stream_id) }), streamDetailsView)
  },
  get_streams: {
    args: [],
    run: async ({ client }) => (await client.stream.list()).map(streamView)
  },
  create_stream: {
    args: ['name', 'encryption_key_hex'],
    run: async ({ client, args }) => streamDetailsView(await client.stream.create({
      name: text(args.name),
      encryptionKey: optional(args.encryption_key_hex, (key) => StreamEncryptionKey.from(hex(key)))
    }))
  },
  update_stream: {
    args: ['stream_id', 'name', 'reset'],
    run: async ({ client, args }) => {
      await client.stream.update({
        streamId: identifier(args.stream_id), name: optional(args.name, text), rawReset: optional(args.reset, texts)
      });
      return null;
    }
  },
  delete_stream: {
    args: ['stream_id'],
    run: async ({ client, args }) => { await client.stream.delete({ streamId: identifier(args.stream_id) }); return null; }
  },
  get_topic: {
    args: ['stream_id', 'topic_id'],
    run: async ({ client, args }) => nullOr(
      await client.topic.get({ streamId: identifier(args.stream_id), topicId: identifier(args.topic_id) }),
      topicDetailsView
    )
  },
  get_topics: {
    args: ['stream_id'],
    run: async ({ client, args }) => (await client.topic.list({ streamId: identifier(args.stream_id) })).map(topicView)
  },
  create_topic: {
    args: ['stream_id', 'name', 'partitions_count', 'message_expiry', 'max_topic_size'],
    run: async ({ client, args }) => {
      const partitionCount = optional(args.partitions_count, u32);
      if (partitionCount === undefined)
        throw new Unsupported('a topic without partitions_count, the SDK has no default count');
      return topicDetailsView(await client.topic.create({
        streamId: identifier(args.stream_id),
        name: text(args.name),
        partitionCount,
        messageExpiry: optional(args.message_expiry, expiry),
        maxTopicSize: optional(args.max_topic_size, topicSize)
      }));
    }
  },
  update_topic: {
    args: ['stream_id', 'topic_id', 'name', 'message_expiry', 'max_topic_size', 'reset'],
    run: async ({ client, args }) => {
      await client.topic.update({
        streamId: identifier(args.stream_id),
        topicId: identifier(args.topic_id),
        name: optional(args.name, text),
        messageExpiry: optional(args.message_expiry, expiry),
        maxTopicSize: optional(args.max_topic_size, topicSize),
        rawReset: optional(args.reset, texts)
      });
      return null;
    }
  },
  delete_topic: {
    args: ['stream_id', 'topic_id'],
    run: async ({ client, args }) => {
      // The SDK sends a partitions count that the server does not read, and
      // its own end-to-end test sends 0.
      await client.topic.delete({
        streamId: identifier(args.stream_id), topicId: identifier(args.topic_id), partitionsCount: 0
      });
      return null;
    }
  },
  send_messages: {
    args: ['stream_id', 'topic_id', 'partitioning', 'messages'],
    run: async ({ client, args }) => {
      await client.message.send({
        streamId: identifier(args.stream_id),
        topicId: identifier(args.topic_id),
        partition: partitioning(args.partitioning),
        messages: messages(args.messages)
      });
      return null;
    }
  },
  poll_messages: {
    args: ['stream_id', 'topic_id', 'partition_id', 'consumer', 'strategy', 'count', 'commit', 'max_wait_ms', 'min_count'],
    run: async ({ client, args, signal }) => polledView(await client.message.poll({
      streamId: identifier(args.stream_id),
      topicId: identifier(args.topic_id),
      partitionId: u32(args.partition_id),
      consumer: consumer(args.consumer),
      strategy: strategy(args.strategy),
      options: pollOptions({
        count: u32(args.count),
        commit: commit(args.commit),
        maxWait: optional(args.max_wait_ms, millis),
        minCount: optional(args.min_count, u32)
      }),
      signal
    }))
  },
  create_consumer_group: {
    args: ['stream_id', 'topic_id', 'name'],
    run: async ({ client, args }) => groupView(await client.group.create({
      streamId: identifier(args.stream_id), topicId: identifier(args.topic_id), name: text(args.name)
    }))
  },
  get_consumer_group: {
    args: ['stream_id', 'topic_id', 'group_id'],
    run: async ({ client, args }) => nullOr(await client.group.get({
      streamId: identifier(args.stream_id), topicId: identifier(args.topic_id), groupId: identifier(args.group_id)
    }), groupView)
  },
  delete_consumer_group: {
    args: ['stream_id', 'topic_id', 'group_id'],
    run: async ({ client, args }) => {
      await client.group.delete({
        streamId: identifier(args.stream_id), topicId: identifier(args.topic_id), groupId: identifier(args.group_id)
      });
      return null;
    }
  },
  build_reader: {
    args: ['stream_id', 'topic_id', 'consumer', 'partitions', 'start', 'max_wait_ms', 'commit'],
    run: async (call) => {
      const { agent, client, args } = call;
      const entity = saveAsOf(call);
      agent.keepReader(entity, await client.message.reader({
        streamId: identifier(args.stream_id),
        topicId: identifier(args.topic_id),
        consumer: consumer(args.consumer),
        partitions: optional(args.partitions, u32s),
        start: optional(args.start, strategy),
        options: pollOptions({ commit: commit(args.commit), maxWait: optional(args.max_wait_ms, millis) })
      }));
      return null;
    }
  },
  build_consumer: {
    args: ['stream_id', 'topic_id', 'group', 'max_wait_ms', 'count', 'commit'],
    run: async (call) => {
      const { agent, client, args } = call;
      const entity = saveAsOf(call);
      agent.keepConsumer(entity, await client.consumerGroup({
        name: text(args.group),
        streamId: identifier(args.stream_id),
        topicId: identifier(args.topic_id),
        options: pollOptions({
          commit: commit(args.commit), maxWait: optional(args.max_wait_ms, millis), count: optional(args.count, u32)
        })
      }));
      return null;
    }
  },
  resolve_operation: {
    args: ['operation_id', 'deadline_ms'],
    run: async ({ client, args }) => {
      const budget = optional(args.deadline_ms, millis);
      await client.resolveOperation(
        u64(args.operation_id), budget === undefined ? undefined : { deadline: Date.now() + budget }
      );
      return null;
    }
  },
  abandon_operation: {
    args: ['operation_id'],
    run: async ({ client, args }) => { await client.abandonOperation(u64(args.operation_id)); return null; }
  },
  send_binary_request: {
    args: ['code', 'payload_hex'],
    run: async ({ client, args }) =>
      rawView(await client.sendBinaryRequest(u32(args.code), optional(args.payload_hex, hex) ?? Buffer.alloc(0)))
  },
  send_binary_request_to_replica: {
    args: ['code', 'payload_hex', 'replica'],
    run: async ({ client, args }) => rawView(await client.sendBinaryRequestToReplica(
      u32(args.code), optional(args.payload_hex, hex) ?? Buffer.alloc(0), u32(args.replica)
    ))
  },
  describe_options: {
    args: ['scope'],
    run: async ({ client, args }) =>
      (await client.system.describeOptions({ scope: scope(args.scope) })).map(optionSpecView)
  },
  create_user: {
    args: ['username', 'password', 'status', 'permissions'],
    run: async ({ client, args }) => userView(await client.user.create({
      username: text(args.username),
      password: text(args.password),
      status: userStatus(args.status),
      permissions: optional(args.permissions, permissions)
    }))
  },
  get_user: {
    args: ['user_id'],
    run: async ({ client, args }) => nullOr(await client.user.get({ userId: identifier(args.user_id) }), userView)
  },
  update_user: {
    args: ['user_id', 'username', 'status'],
    run: async ({ client, args }) => {
      await client.user.update({
        userId: identifier(args.user_id),
        username: optional(args.username, text),
        status: optional(args.status, userStatus)
      });
      return null;
    }
  },
  delete_user: {
    args: ['user_id'],
    run: async ({ client, args }) => { await client.user.delete({ userId: identifier(args.user_id) }); return null; }
  },
  get_metadata_changes: {
    args: ['start', 'groups', 'limit'],
    run: async ({ client, args }) => {
      const limit = optional(args.limit, u32);
      return changesView(await client.system.getMetadataChanges({
        start: metadataStart(args.start),
        groups: (optional(args.groups, texts) ?? []) as MetadataChangeGroup[],
        ...(limit === undefined ? {} : { limit })
      }));
    }
  },
  get_connection_info: {
    args: [],
    run: async () => { throw new Unsupported('the SDK has no connection info'); }
  }
};
