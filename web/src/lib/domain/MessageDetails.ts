import { type Message, type MessagePartition, messagePartitionMapper } from './Message';

type Payload = {
  partition_id: number;
  current_offset: number;
  messages: Array<Message>;
};

export function partitionMessagesDetailsMapper(payload: Payload): MessagePartition {
  return messagePartitionMapper(payload);
}
