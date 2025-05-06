import { formatDate, formatRuntime } from '$lib/utils/formatters/dateFormatter';

type StatsStringItem = { name: string; value: string; rawValue: string | number };
type StatsNumberItem = { name: string; value: number };

export type Stats = {
  processId: StatsNumberItem;
  cpuUsage: StatsStringItem;
  memoryUsage: StatsStringItem;
  totalMemory: StatsStringItem;
  availableMemory: StatsStringItem;
  runTime: StatsStringItem;
  startTime: StatsStringItem;
  readBytes: StatsStringItem;
  writtenBytes: StatsStringItem;
  messagesSizeBytes: StatsStringItem;
  streamsCount: StatsNumberItem;
  topicsCount: StatsNumberItem;
  partitionsCount: StatsNumberItem;
  segmentsCount: StatsNumberItem;
  messagesCount: StatsNumberItem;
  clientsCount: StatsNumberItem;
  consumerGroupsCount: StatsNumberItem;
  hostName: StatsStringItem;
  osName: StatsStringItem;
  osVersion: StatsStringItem;
  kernelVersion: StatsStringItem;
  iggyServerVersion: StatsStringItem;
};

export function statsMapper(item: any): Stats {
  const formattedRuntime = formatRuntime(item.run_time);

  const formattedStartTime = formatDate(item.start_time);

  return {
    processId: {
      name: 'Process ID',
      value: item.process_id
    },
    cpuUsage: {
      name: 'CPU usage',
      value: `${item.cpu_usage.toFixed(2)} %`,
      rawValue: item.cpu_usage
    },
    memoryUsage: {
      value: item.memory_usage,
      name: 'Memory usage',
      rawValue: item.memory_usage
    },
    totalMemory: {
      name: 'Total memory',
      value: item.total_memory,
      rawValue: item.total_memory
    },
    availableMemory: {
      name: 'Available memory',
      value: item.available_memory,
      rawValue: item.available_memory
    },
    runTime: {
      name: 'Run time',
      value: formattedRuntime,
      rawValue: item.run_time
    },
    startTime: {
      name: 'Start time',
      value: formattedStartTime,
      rawValue: item.start_time
    },

    readBytes: {
      name: 'Read',
      value: item.read_bytes,
      rawValue: item.read_bytes
    },

    writtenBytes: {
      name: 'Written',
      value: item.written_bytes,
      rawValue: item.written_bytes
    },
    messagesSizeBytes: {
      name: 'Messages Size',
      value: item.messages_size_bytes,
      rawValue: item.messages_size_bytes
    },
    streamsCount: {
      name: 'Streams',
      value: item.streams_count
    },

    topicsCount: {
      name: 'Topics',
      value: item.topics_count
    },

    partitionsCount: {
      name: 'Partitions',
      value: item.partitions_count
    },

    segmentsCount: {
      name: 'Segments',
      value: item.segments_count
    },

    messagesCount: {
      name: 'Messages',
      value: item.messages_count
    },

    clientsCount: {
      name: 'Clients',
      value: item.clients_count
    },
    consumerGroupsCount: {
      name: 'Consumer Groups',
      value: item.consumer_groups_count
    },

    hostName: {
      name: 'Host',
      value: item.hostname,
      rawValue: item.hostName
    },
    osName: {
      name: 'System',
      value: item.os_name,
      rawValue: item.os_name
    },

    osVersion: {
      name: 'System version',
      value: item.os_version,
      rawValue: item.os_version
    },
    kernelVersion: {
      name: 'Kernel version',
      value: item.kernel_version,
      rawValue: item.kernel_version
    },
    iggyServerVersion: {
      name: 'Iggy server version',
      value: item.iggy_server_version,
      rawValue: item.iggy_server_version
    }
  };
}
