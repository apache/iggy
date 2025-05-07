import { bytesFormatter } from '$lib/utils/formatters/bytesFormatter';

export type Stream = {
  id: number;
  name: string;
  sizeBytes: number;
  sizeFormatted: string;
  messagesCount: number;
  topicsCount: number;
  createdAt: number;
};

export function streamMapper(item: any): Stream {
  return {
    id: item.id,
    name: item.name,
    sizeBytes: item.size,
    sizeFormatted: item.size,
    messagesCount: item.messages_count,
    topicsCount: item.topics_count,
    createdAt: item.created_at
  };
}

export function streamListMapper(data: any): Stream[] {
  const streams = data.map(streamMapper) as Stream[];
  const sortedStream = streams.sort((a, b) => b.createdAt - a.createdAt);

  return sortedStream;
}
