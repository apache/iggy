
import { uint8ToBuf, uint32ToBuf } from '../number.utils.js';

export type GlobalPermissions = {
  ManageServers: boolean,
  ReadServers: boolean,
  ManageUsers: boolean,
  ReadUsers: boolean,
  ManageStreams: boolean,
  ReadStreams: boolean,
  ManageTopics: boolean,
  ReadTopics: boolean,
  PollMessages: boolean,
  SendMessages: boolean
};

type GlobalPermissionsDeserialized = {
  bytesRead: number,
  data: GlobalPermissions
};

export type TopicPermissions = {
  manage: boolean,
  read: boolean,
  pollMessages: boolean,
  sendMessages: boolean
};

export type TopicPerms = {
  topicId: number,
  permissions: TopicPermissions
};

type TopicPermissionsDeserialized = { bytesRead: number } & TopicPerms;

export type StreamPermissions = {
  manageStream: boolean,
  readStream: boolean,
  manageTopics: boolean,
  readTopics: boolean,
  pollMessages: boolean,
  sendMessages: boolean,
};

export type StreamPerms = {
  streamId: number,
  permissions: StreamPermissions,
  topics: TopicPerms[]
};

type StreamPermissionsDeserialized = { bytesRead: number } & StreamPerms;


export type UserPermissions = {
  global: GlobalPermissions,
  streams: StreamPerms[]
};

const toBool = (u: number) => u === 1;
const boolToByte = (b: boolean) => b ? 1 : 0;

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

export const serializeTopicPermissions = (p: TopicPerms) => {
  return Buffer.concat([
    uint32ToBuf(p.topicId),
    uint8ToBuf(boolToByte(p.permissions.manage)),
    uint8ToBuf(boolToByte(p.permissions.read)),
    uint8ToBuf(boolToByte(p.permissions.pollMessages)),
    uint8ToBuf(boolToByte(p.permissions.sendMessages)),
  ]);
};

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
