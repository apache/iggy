// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

export const translateErrorCode = (code: number): string => {
  switch (code.toString()) {
    case '1': return "Error";
    case '2': return "Invalid configuration";
    case '3': return "Invalid command";
    case '4': return "Invalid format";
    case '5': return "Feature is unavailable";
    case '6': return "Invalid identifier";
    case '7': return "Invalid version: {0}";
    case '8': return "Disconnected";
    case '9': return "Cannot establish connection";
    case '10': return "Cannot create base directory, Path: {0}";
    case '11': return "Cannot create runtime directory, Path: {0}";
    case '12': return "Cannot remove runtime directory, Path: {0}";
    case '13': return "Cannot create state directory, Path: {0}";
    case '14': return "State file not found";
    case '15': return "State file corrupted";
    case '16': return "Invalid state entry checksum: {0}, expected: {1}, for index: {2}";
    case '19': return "Cannot open database, Path: {0}";
    case '20': return "Resource with key: {0} was not found.";
    case '21': return "Cannot close WebSocket connection";
    case '30': return "Stale client";
    case '31': return "TCP error";
    case '32': return "QUIC error";
    case '33': return "Invalid server address";
    case '34': return "Invalid client address";
    case '35': return "Invalid IP address: {0}:{1}";
    case '36': return "Http error {0}";
    case '37': return "Invalid API URL: {0}";
    case '40': return "Unauthenticated";
    case '41': return "Unauthorized";
    case '42': return "Invalid credentials";
    case '43': return "Invalid username";
    case '44': return "Invalid password";
    case '45': return "Invalid user status";
    case '46': return "User already exists";
    case '47': return "User inactive";
    case '48': return "Cannot delete user with ID: {0}";
    case '49': return "Cannot change permissions for user with ID: {0}";
    case '50': return "Invalid personal access token name";
    case '51': return "Personal access token: {0} for user with ID: {1} already exists";
    case '52': return "User with ID: {0} has reached the maximum number of personal access tokens: {1}";
    case '53': return "Invalid personal access token";
    case '54': return "Personal access token: {0} for user with ID: {1} has expired.";
    case '55': return "Users limit reached.";
    case '56': return "Invalid personal access token expiry";
    case '57': return "Request transiently not committed; retry";
    case '58': return "Request transiently not accepted; retry, on any replica";
    case '59': return "Request already applied; its reply is no longer available";
    case '61': return "Not connected";
    case '63': return "Client shutdown";
    case '64': return "Invalid TLS domain";
    case '65': return "Invalid TLS certificate path";
    case '66': return "Invalid TLS certificate";
    case '67': return "Failed to add certificate";
    case '70': return "Invalid encryption key";
    case '71': return "Cannot encrypt data";
    case '72': return "Cannot decrypt data";
    case '73': return "Invalid JWT algorithm: {0}";
    case '74': return "Invalid JWT secret";
    case '75': return "JWT is missing";
    case '76': return "Cannot generate JWT";
    case '77': return "Access token is missing";
    case '78': return "Invalid access token";
    case '79': return "Cannot fetch JWKS from URL: {0}";
    case '80': return "Invalid size bytes";
    case '81': return "Invalid UTF-8";
    case '82': return "Invalid number encoding";
    case '83': return "Invalid boolean value";
    case '84': return "Invalid number value";

    case '100': return "Client with ID: {0} was not found.";
    case '101': return "Invalid client ID";

    case '206': return "Connection closed";
    case '209': return "Cannot parse header kind from {0}";
    case '300': return "HTTP response error, status: {0}, body: {1}";
    case '301': return "Invalid HTTP request";
    case '302': return "Invalid JSON response";
    case '303': return "Invalid bytes response";
    case '304': return "Empty response";
    case '305': return "Cannot create endpoint";
    case '306': return "Cannot parse URL";
    case '400': return "WebSocket error";
    case '401': return "WebSocket connection error";
    case '402': return "WebSocket close error";
    case '403': return "WebSocket receive error";
    case '404': return "WebSocket send error";

    // STREAM
    case '1000': return "Cannot create streams directory, Path: {0}";
    case '1001': return "Cannot create stream with ID: {0} directory, Path: {1}";
    case '1002': return "Failed to create stream info file for stream with ID: {0}";
    case '1003': return "Failed to update stream info for stream with ID: {0}";
    case '1004': return "Failed to open stream info file for stream with ID: {0}";
    case '1005': return "Failed to read stream info file for stream with ID: {0}";
    case '1006': return "Failed to create stream with ID: {0}";
    case '1007': return "Failed to delete stream with ID: {0}";
    case '1008': return "Failed to delete stream directory with ID: {0}";
    case '1009': return "Stream with ID: {0} was not found.";
    case '1010': return "Stream with name: {0} was not found.";
    case '1011': return "Stream with directory {0} was not found.";
    case '1012': return "Stream with name: {0} already exists.";
    case '1013': return "Invalid stream name";
    case '1014': return "Invalid stream ID";
    case '1015': return "Cannot read streams";
    case '1019': return "Max topic size cannot be lower than segment size. Max topic size: {0} < segment size: {1}.";
    case '1020': return "Too many streams";


    case '2000': return "Cannot create topics directory for stream with ID: {0}, Path: {1}";
    case '2001': return "Failed to create directory for topic with ID: {0} for stream with ID: {1}, Path: {2}";
    case '2002': return "Failed to create topic info file for topic with ID: {0} for stream with ID: {1}.";
    case '2003': return "Failed to update topic info for topic with ID: {0} for stream with ID: {1}.";
    case '2004': return "Failed to open topic info file for topic with ID: {0} for stream with ID: {1}.";
    case '2005': return "Failed to read topic info file for topic with ID: {0} for stream with ID: {1}.";
    case '2006': return "Failed to create topic with ID: {0} for stream with ID: {1}.";
    case '2007': return "Failed to delete topic with ID: {0} for stream with ID: {1}.";
    case '2008': return "Failed to delete topic directory with ID: {0} for stream with ID: {1}, Path: {2}";
    case '2009': return "Cannot poll topic";
    case '2010': return "Topic with ID: {0} for stream with ID: {1} was not found.";
    case '2011': return "Topic with name: {0} for stream with ID: {1} was not found.";
    case '2013': return "Topic with name: {0} for stream with ID: {1} already exists.";
    case '2014': return "Invalid topic name";
    case '2015': return "Too many partitions";
    case '2016': return "Invalid topic ID";
    case '2017': return "Cannot read topics for stream with ID: {0}";
    case '2018': return "Invalid replication factor";
    case '2019': return "Invalid partitions count";
    case '2020': return "Topic directory: {0} not found";
    case '2021': return "Too many topics";

    // TOPIC
    case '3000': return "Cannot create partition with ID: {0} for stream with ID: {1} and topic with ID: {2}";
    case '3001': return "Failed to create directory for partitions for stream with ID: {0} and topic with ID: {1}";
    case '3002': return "Failed to create directory for partition with ID: {0} for stream with ID: {1} and topic with ID: {2}";
    case '3003': return "Cannot open partition log file";
    case '3004': return "Cannot read partitions directories.";
    case '3005': return "Failed to delete partition with ID: {0} for stream with ID: {1} and topic with ID: {2}";
    case '3006': return "Failed to delete partition directory with ID: {0} for stream with ID: {1} and topic with ID: {2}";
    case '3007': return "Partition with ID: {0} for topic with ID: {1} for stream with ID: {2} was not found.";
    case '3008': return "Topic with ID: {0} for stream with ID: {1} has no partitions.";
    case '3009': return "Cannot read partitions for topic with ID: {0} for stream with ID: {1}";
    case '3010': return "Failed to delete consumer offsets directory for path: {0}";
    case '3011': return "Failed to delete consumer offset file for path: {0}";
    case '3012': return "Failed to create consumer offsets directory for path: {0}";
    case '3020': return "Failed to read consumers offsets from path: {0}";
    case '3021': return "Consumer offset for consumer with ID: {0} was not found.";
    case '3022': return "Failed to resolve consumer with ID: {0}";
    case '3023': return "Cannot open consumer offsets file for path: {0}";
    case '3013': return "Partition id space exhausted for this topic";

    // MESSAGE
    case '4000': return "Segment not found";
    case '4001': return "Segment with start offset: {0} and partition with ID: {1} is closed";
    case '4002': return "Segment size is invalid";
    case '4003': return "Failed to create segment log file for Path: {0}.";
    case '4004': return "Failed to create segment index file for Path: {0}.";
    case '4005': return "Failed to create segment time index file for Path: {0}.";
    case '4006': return "Cannot save messages to segment.";
    case '4007': return "Cannot save index to segment.";
    case '4008': return "Cannot save time index to segment.";
    case '4009': return "Invalid messages count";
    case '4010': return "Cannot append message";
    case '4011': return "Cannot read message";
    case '4012': return "Cannot read message ID";
    case '4013': return "Cannot read message state";
    case '4014': return "Cannot read message timestamp";
    case '4015': return "Cannot read headers length";
    case '4016': return "Cannot read headers payload";
    case '4017': return "Too big headers payload";
    case '4018': return "Invalid header key";
    case '4019': return "Invalid header value";
    case '4020': return "Cannot read message length";
    case '4021': return "Cannot save messages to segment";
    case '4022': return "Too big message payload";
    case '4023': return "Too many messages";
    case '4024': return "Empty message payload";
    case '4025': return "Invalid message payload length";
    case '4026': return "Cannot read message checksum";
    case '4027': return "Invalid message checksum: {0}, expected: {1}, for offset: {2}";
    case '4028': return "Invalid key value length";
    case '4029': return "Command length error: {0}";
    case '4030': return "Incorrect Segments Count size: {0}";
    case '4031': return "Non-zero offset: {0} at index: {1}";
    case '4032': return "Non-zero timestamp: {0} at index: {1}";
    case '4033': return "Missing index: {0}";
    case '4034': return "Invalid indexes byte size: {0}B, should be divisible by 16";
    case '4035': return "Invalid indexes count: {0}, expected: {1}";
    case '4036': return "Invalid messages byte size: {0}B, expected: {1}B";
    case '4037': return "Too small message: {0}B, expected: {1}B";
    case '4038': return "Invalid message timestamp delta: {0} microseconds exceeds the per-batch limit";
    case '4039': return "Invalid batch checksum: {0}, expected: {1}, for base offset: {2}";
    case '4040': return "Invalid header kind code: {0}";
    case '4041': return "Unsupported option key: {0}";
    case '4042': return "Invalid option value for key: {0}";
    case '4043': return "Options block exceeds its limits: {0}";
    case '4050': return "Cannot sed messages due to client disconnection";
    case '4051': return "Background send error";
    case '4052': return "Background send timeout";
    case '4053': return "Background send buffer is full";
    case '4054': return "Background worker disconnected";
    case '4055': return "Background send buffer overflow";
    case '4057': return "Producer closed";
    case '4100': return "Invalid offset: {0}";
    case '4101': return "Invalid reserved field value: {0}, expected: 0";

    // CONSUMER GROUP
    case '5000': return "Consumer group with ID: {0} for topic with ID: {1} was not found.";
    case '5002': return "Invalid consumer group ID";
    case '5003': return "Consumer group with name: {0} for topic with ID: {1} was not found.";
    case '5004': return "Consumer group with name: {0} for topic with ID: {1} already exists.";
    case '5005': return "Invalid consumer group name";
    case '5006': return "Consumer group member with client ID: {0} for group with ID: {1} for topic with ID: {2} was not found.";
    case '5007': return "Failed to create consumer group info file for ID: {0} for topic with ID: {1} for stream with ID: {2}.";
    case '5008': return "Failed to delete consumer group info file for ID: {0} for topic with ID: {1} for stream with ID: {2}.";
    case '5009': return "Consumer group member with client ID: {0} does not own partition: {1} at the current generation (rebalance in progress).";
    case '6000': return "Base offset is missing";
    case '6001': return "Last offset delta is missing";
    case '6002': return "Max timestamp is missing";
    case '6003': return "Length is missing";
    case '6004': return "Payload is missing";
    case '7000': return "Cannot read batch base offset";
    case '7001': return "Cannot read batch length";
    case '7002': return "Cannot read batch last offset delta";
    case '7003': return "Cannot read batch maximum timestamp";
    case '7004': return "Cannot read batch payload";
    case '8000': return "Invalid connection string";
    case '9000': return "Snapshot file completion failed";
    case '10000': return "Cannot serialize resource";
    case '10001': return "Cannot deserialize resource";
    case '10002': return "Cannot read file";
    case '10003': return "Cannot read file metadata";
    case '10004': return "Cannot seek file";
    case '10005': return "Cannot append to file";
    case '10006': return "Cannot write to file";
    case '10007': return "Cannot overwrite file";
    case '10008': return "Cannot delete file";
    case '10009': return "Cannot sync file";
    case '10010': return "Cannot read index offset";
    case '10011': return "Cannot read index position";
    case '10012': return "Cannot read index timestamp";
    case '10013': return "Timestamp out of range: {0}";
    case '11000': return "Shard not found for stream ID: {0}, topic ID: {1}, partition ID: {2}";
    case '11001': return "Shard communication error";
    case '12000': return "Cannot bind to socket with addr: {0}";
    case '12001': return "Task execution timeout";
    case '13000': return "IO error: {0}";
    case '14000': return "VSR session already bound; reset before re-binding";
    case '14001': return "VSR session value {0} is invalid (must be non-zero)";
    case '14003': return "Incompatible binary protocol version: client {}, server accepts [{}, {}]";

    default: return 'error';
  }
}
