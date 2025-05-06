import { partitionMapper, type Partition } from './Partition';
import { topicMapper, type Topic } from './Topic';

export type TopicDetails = Topic & { partitions: Partition[] };

export function topicDetailsMapper(item: any): TopicDetails {
  return {
    ...topicMapper(item),
    partitions: item.partitions.map(partitionMapper)
  };
}
