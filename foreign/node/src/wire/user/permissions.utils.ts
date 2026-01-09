/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import { uint8ToBuf, uint32ToBuf } from '../number.utils.js';

/**
 * Global permissions for server-wide operations.
 */
export type GlobalPermissions = {
  /** Can manage server configuration */
  ManageServers: boolean,
  /** Can read server information */
  ReadServers: boolean,
  /** Can manage users */
  ManageUsers: boolean,
  /** Can read user information */
  ReadUsers: boolean,
  /** Can manage streams */
  ManageStreams: boolean,
  /** Can read stream information */
  ReadStreams: boolean,
  /** Can manage topics */
  ManageTopics: boolean,
  /** Can read topic information */
  ReadTopics: boolean,
  /** Can poll messages */
  PollMessages: boolean,
  /** Can send messages */
  SendMessages: boolean
};

/**
 * Result of deserializing global permissions.
 */
type GlobalPermissionsDeserialized = {
  /** Number of bytes consumed */
  bytesRead: number,
  /** Deserialized permissions */
  data: GlobalPermissions
};

/**
 * Permissions for a specific topic.
 */
export type TopicPermissions = {
  /** Can manage the topic */
  manage: boolean,
  /** Can read the topic */
  read: boolean,
  /** Can poll messages from the topic */
  pollMessages: boolean,
  /** Can send messages to the topic */
  sendMessages: boolean
};

/**
 * Topic permissions with topic identifier.
 */
export type TopicPerms = {
  /** Topic ID */
  topicId: number,
  /** Topic permissions */
  permissions: TopicPermissions
};

/** Result of deserializing topic permissions */
type TopicPermissionsDeserialized = { bytesRead: number } & TopicPerms;

/**
 * Permissions for a specific stream.
 */
export type StreamPermissions = {
  /** Can manage the stream */
  manageStream: boolean,
  /** Can read the stream */
  readStream: boolean,
  /** Can manage topics within the stream */
  manageTopics: boolean,
  /** Can read topics within the stream */
  readTopics: boolean,
  /** Can poll messages from the stream */
  pollMessages: boolean,
  /** Can send messages to the stream */
  sendMessages: boolean,
};

/**
 * Stream permissions with stream identifier and topic permissions.
 */
export type StreamPerms = {
  /** Stream ID */
  streamId: number,
  /** Stream-level permissions */
  permissions: StreamPermissions,
  /** Topic-specific permissions within the stream */
  topics: TopicPerms[]
};

/** Result of deserializing stream permissions */
type StreamPermissionsDeserialized = { bytesRead: number } & StreamPerms;


/**
 * Complete user permissions including global and stream-level permissions.
 */
export type UserPermissions = {
  /** Global permissions */
  global: GlobalPermissions,
  /** Stream-specific permissions */
  streams: StreamPerms[]
};

/**
 * Converts a number to a boolean (1 = true).
 *
 * @param u - Number to convert
 * @returns True if u equals 1
 */
const toBool = (u: number) => u === 1;

/**
 * Converts a boolean to a byte value.
 *
 * @param b - Boolean value
 * @returns 1 if true, 0 if false
 */
const boolToByte = (b: boolean) => b ? 1 : 0;

/**
 * Deserializes global permissions from a buffer.
 *
 * @param p - Buffer containing serialized permissions
 * @param pos - Starting position in the buffer
 * @returns Object with bytes read and deserialized permissions
 */
export const deserializeGlobalPermissions =
  (p: Buffer, pos = 0): GlobalPermissionsDeserialized => {
    return {
      bytesRead: 10,
      data: {
        ManageServers: toBool(p.readUInt8(pos)),
        ReadServers: toBool(p.readUInt8(pos + 1)),
        ManageUsers: toBool(p.readUInt8(pos + 2)),
        ReadUsers: toBool(p.readUInt8(pos + 3)),
        ManageStreams: toBool(p.readUInt8(pos + 4)),
        ReadStreams: toBool(p.readUInt8(pos + 5)),
        ManageTopics: toBool(p.readUInt8(pos + 6)),
        ReadTopics: toBool(p.readUInt8(pos + 7)),
        PollMessages: toBool(p.readUInt8(pos + 8)),
        SendMessages: toBool(p.readUInt8(pos + 9)),
      }
    };
  };

/**
 * Serializes global permissions to a buffer.
 *
 * @param p - Global permissions to serialize
 * @returns Serialized permissions buffer (10 bytes)
 */
export const serializeGlobalPermission = (p: GlobalPermissions) => {
  return Buffer.concat([
    uint8ToBuf(boolToByte(p.ManageServers)),
    uint8ToBuf(boolToByte(p.ReadServers)),
    uint8ToBuf(boolToByte(p.ManageUsers)),
    uint8ToBuf(boolToByte(p.ReadUsers)),
    uint8ToBuf(boolToByte(p.ManageStreams)),
    uint8ToBuf(boolToByte(p.ReadStreams)),
    uint8ToBuf(boolToByte(p.ManageTopics)),
    uint8ToBuf(boolToByte(p.ReadTopics)),
    uint8ToBuf(boolToByte(p.PollMessages)),
    uint8ToBuf(boolToByte(p.SendMessages)),
  ]);
}

/**
 * Deserializes topic permissions from a buffer.
 *
 * @param p - Buffer containing serialized permissions
 * @param pos - Starting position in the buffer
 * @returns Object with bytes read, topic ID, and permissions
 */
export const deserializeTopicPermissions =
  (p: Buffer, pos = 0): TopicPermissionsDeserialized => {
    const topicId = p.readUInt32LE(pos);
    const permissions = {
      manage: toBool(p.readUInt8(pos + 4)),
      read: toBool(p.readUInt8(pos + 5)),
      pollMessages: toBool(p.readUInt8(pos + 6)),
      sendMessages: toBool(p.readUInt8(pos + 7)),
    };

    return {
      bytesRead: 8,
      topicId,
      permissions
    }
  };

/**
 * Serializes topic permissions to a buffer.
 *
 * @param p - Topic permissions to serialize
 * @returns Serialized permissions buffer (8 bytes)
 */
export const serializeTopicPermissions = (p: TopicPerms) => {
  return Buffer.concat([
    uint32ToBuf(p.topicId),
    uint8ToBuf(boolToByte(p.permissions.manage)),
    uint8ToBuf(boolToByte(p.permissions.read)),
    uint8ToBuf(boolToByte(p.permissions.pollMessages)),
    uint8ToBuf(boolToByte(p.permissions.sendMessages)),
  ]);
};

/**
 * Deserializes stream permissions from a buffer.
 * Includes nested topic permissions if present.
 *
 * @param p - Buffer containing serialized permissions
 * @param pos - Starting position in the buffer
 * @returns Object with bytes read, stream ID, permissions, and topics
 */
export const deserializeStreamPermissions =
  (p: Buffer, pos = 0): StreamPermissionsDeserialized => {
    const start = pos;
    const streamId = p.readUInt32LE(pos);
    const permissions = {
      manageStream: toBool(p.readUInt8(pos + 4)),
      readStream: toBool(p.readUInt8(pos + 5)),
      manageTopics: toBool(p.readUInt8(pos + 6)),
      readTopics: toBool(p.readUInt8(pos + 7)),
      pollMessages: toBool(p.readUInt8(pos + 8)),
      sendMessages: toBool(p.readUInt8(pos + 9)),
    }

    pos += 10;
    const topics = [];
    const hasTopics = toBool(p.readUInt8(pos));

    if (hasTopics) {
      let read = true;
      pos += 1;
      while (read) {
        const { bytesRead, topicId, permissions } = deserializeTopicPermissions(p, pos);
        pos += bytesRead;
        topics.push({ topicId, permissions });

        if (p.readUInt8(pos) === 0)
          read = false; // break
      }
    }

    return {
      bytesRead: pos - start,
      streamId,
      permissions,
      topics
    }
  };

/**
 * Serializes stream permissions to a buffer.
 * Includes nested topic permissions if present.
 *
 * @param p - Stream permissions to serialize
 * @returns Serialized permissions buffer
 */
export const serializeStreamPermissions = (p: StreamPerms) => {
  const bStream = Buffer.concat([
    uint32ToBuf(p.streamId),
    uint8ToBuf(boolToByte(p.permissions.manageStream)),
    uint8ToBuf(boolToByte(p.permissions.readStream)),
    uint8ToBuf(boolToByte(p.permissions.manageTopics)),
    uint8ToBuf(boolToByte(p.permissions.readTopics)),
    uint8ToBuf(boolToByte(p.permissions.pollMessages)),
    uint8ToBuf(boolToByte(p.permissions.sendMessages)),
  ]);

  const hasTopic = p.topics.length > 0;
  const bHasTopic = uint8ToBuf(boolToByte(hasTopic));
  const bHead = Buffer.concat([bStream, bHasTopic]);

  if (!hasTopic)
    return bHead;

  return p.topics.reduce((ac, c) => Buffer.concat([
    ac, serializeTopicPermissions(c)
  ]), bHead);
}

/**
 * Deserializes complete user permissions from a buffer.
 * Includes global permissions and stream-level permissions.
 *
 * @param p - Buffer containing serialized permissions
 * @param pos - Starting position in the buffer
 * @returns Deserialized user permissions
 */
export const deserializePermissions = (p: Buffer, pos = 0): UserPermissions => {
  const { bytesRead, data } = deserializeGlobalPermissions(p, pos);
  pos += bytesRead;

  const streams = [];
  const hasStream = toBool(p.readUInt8(pos));

  if (hasStream) {
    let readStream = true;
    pos += 1;
    while (readStream) {
      const {
        bytesRead, streamId, permissions, topics
      } = deserializeStreamPermissions(p, pos);
      streams.push({ streamId, permissions, topics });
      pos += bytesRead;
      if (p.readUInt8(pos) === 0)
        readStream = false; // break
    }
  }
  return {
    global: data,
    streams
  }
};

/**
 * Serializes user permissions to a buffer.
 * Returns a single zero byte if no permissions provided.
 *
 * @param p - Optional user permissions to serialize
 * @returns Serialized permissions buffer
 */
export const serializePermissions = (p?: UserPermissions) => {
  if (!p)
    return uint8ToBuf(0);

  const bGlobal = serializeGlobalPermission(p.global);
  const hasStream = p.streams.length > 0;
  const bHasStream = uint8ToBuf(boolToByte(hasStream));
  const bHead = Buffer.concat([bGlobal, bHasStream]);

  if (!hasStream)
    return bHead;

  return p.streams.reduce((ac, c) => Buffer.concat([
    ac, serializeStreamPermissions(c)
  ]), bHead);
};
