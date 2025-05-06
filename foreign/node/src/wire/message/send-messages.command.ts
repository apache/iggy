
import { type Id } from '../identifier.utils.js';
import { serializeSendMessages, type CreateMessage } from './message.utils.js';
import type { Partitioning } from './partitioning.utils.js';
import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';

export type SendMessages = {
  streamId: Id,
  topicId: Id,
  messages: CreateMessage[],
  partition?: Partitioning,
};

export const SEND_MESSAGES = {
  code: 101,

  serialize: ({ streamId, topicId, messages, partition }: SendMessages) => {
    return serializeSendMessages(streamId, topicId, messages, partition);
  },

  deserialize: deserializeVoidResponse
};

export const sendMessages = wrapCommand<SendMessages, boolean>(SEND_MESSAGES);
