import { bytesFormatter } from '$lib/utils/formatters/bytesFormatter';
import { formatDate } from '$lib/utils/formatters/dateFormatter';

export type Partition = {
  id: number;
  segmentsCount: number;
  currentOffset: number;
  sizeBytes: number;
  sizeFormatted: string;
  messagesCount: number;
  createdAt: string;
};

export function partitionMapper(item: any): Partition {
  return {
    id: item.id,
    segmentsCount: item.segments_count,
    currentOffset: item.current_offset,
    sizeBytes: item.size,
    sizeFormatted: item.size,
    messagesCount: item.messages_count,
    createdAt: formatDate(item.created_at)
  };
}
