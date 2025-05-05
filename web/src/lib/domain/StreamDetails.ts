import { streamMapper, type Stream } from './Stream';
import { topicMapper, type Topic } from './Topic';

export type StreamDetails = Stream & { topics: Topic[] };

export function streamDetailsMapper(item: any): StreamDetails {
  return {
    ...streamMapper(item),
    topics: item.topics.map(topicMapper)
  };
}
