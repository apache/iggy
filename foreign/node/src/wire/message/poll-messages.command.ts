
import type { Id } from '../identifier.utils.js';
import type { CommandResponse } from '../../client/client.type.js';
import type { Consumer } from '../offset/offset.utils.js';
import { wrapCommand } from '../command.utils.js';
import {
  serializePollMessages, deserializePollMessages,
  type PollingStrategy, type PollMessagesResponse
} from './poll.utils.js';


export type PollMessages = {
  streamId: Id,
  topicId: Id,
  consumer: Consumer,
  partitionId: number,
  pollingStrategy: PollingStrategy,
  count: number,
  autocommit: boolean
};

export const POLL_MESSAGES = {
  code: 100,

  serialize: ({
    streamId, topicId, consumer, partitionId, pollingStrategy, count, autocommit
  }: PollMessages) => {
    return serializePollMessages(
      streamId, topicId, consumer, partitionId, pollingStrategy, count, autocommit
    );
  },

  deserialize: (r: CommandResponse) => {
    return deserializePollMessages(r.data);
  }
};

export const pollMessages = wrapCommand<PollMessages, PollMessagesResponse>(POLL_MESSAGES);
