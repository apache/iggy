import crypto from 'crypto';
import { Client, Consumer, PollingStrategy } from 'apache-iggy';
import debug from 'debug';

export const log = debug('iggy:getting-started:consumer');

const PARTITION_COUNT = 5;


export async function initSystem(client: Client) {
  log('Creating stream with random name...');
  console.table(await client.stream.list());
  const stream = await client.stream.create({
    name: `sample-stream-${crypto.randomBytes(8).toString('hex')}`,
  });

  log('Stream was created successfully. Stream ID: %s', stream?.id);

  log('Creating topic in stream ID %s...', stream.id);

  const topic = await client.topic.create({
    streamId: stream.id,
    name: `sample-topic-${crypto.randomBytes(4).toString('hex')}`,
    partitionCount: PARTITION_COUNT,
    compressionAlgorithm: 1, // None
    replicationFactor: 1,
  });

  log('Topic was created successfully.', 'Topic ID: %s', topic?.id);
  return {
    stream,
    topic,
  };
}

export async function cleanup(client: Client, streamId: number | string, topicId: number | string) {
  log('Cleaning up: deleting topic ID %d and stream ID %d...', topicId, streamId);
  try {
    await client.topic.delete({
      streamId: streamId,
      topicId: topicId,
      partitionsCount: PARTITION_COUNT,
    });
    log('Topic deleted successfully.');
  } catch (error) {
    log('Error deleting topic: %o', error);
  }

  try {
    await client.stream.delete({ streamId });
    log('Stream deleted successfully.');
  } catch (error) {
    log('Error deleting stream: %o', error);
  }
}

export const sleep = (interval: number) => new Promise(resolve => setTimeout(resolve, interval));



export function parseArgs() {
  console.log = (...args) => process.stdout.write(args.join(' ') + '\n');
  const args = process.argv.slice(2);
  const connectionString = args[0] || 'iggy+tcp://iggy:iggy@127.0.0.1:8090';

  if (args.length > 0 && (args[0] === '-h' || args[0] === '--help')) {
    log('Usage: node producer.ts [connection_string]');
    log('Example: node producer.ts iggy+tcp://iggy:iggy@127.0.0.1:8090');
    process.exit(0);
  }

  return { connectionString };
}