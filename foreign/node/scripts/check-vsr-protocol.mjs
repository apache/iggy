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

import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const root = resolve(import.meta.dirname, '../../..');
const read = (path) => readFile(resolve(root, path), 'utf8');
const nodeRoot = resolve(import.meta.dirname, '..');
const readNode = (path) => readFile(resolve(nodeRoot, path), 'utf8');

const [
  rustCodes,
  rustDispatch,
  rustHeader,
  rustCommand,
  rustOperation,
  rustProtocolCargo,
  nodeCodes,
  nodeHeader,
  nodeOperation,
  nodeRegister,
] = await Promise.all([
  read('core/binary_protocol/src/codes.rs'),
  read('core/binary_protocol/src/dispatch.rs'),
  read('core/binary_protocol/src/consensus/header.rs'),
  read('core/binary_protocol/src/consensus/command.rs'),
  read('core/binary_protocol/src/consensus/operation.rs'),
  read('core/binary_protocol/Cargo.toml'),
  readNode('src/wire/command.code.ts'),
  readNode('src/wire/vsr/header.ts'),
  readNode('src/wire/vsr/operation.ts'),
  readNode('src/wire/vsr/register.ts'),
]);

const numericValues = (source, pattern) =>
  [...source.matchAll(pattern)].map((match) => Number(match[1]));

const rustCommandCodes = numericValues(
  rustCodes,
  /^pub const [A-Z0-9_]+_CODE: u32 = ([0-9]+);$/gm
).sort((left, right) => left - right);
const nodeCommandBlock =
  nodeCodes.match(/export const COMMAND_CODE = \{([\s\S]*?)\n\};/)?.[1] ?? '';
const nodeCommandCodes = numericValues(
  nodeCommandBlock,
  /^\s*[A-Za-z0-9]+:\s*([0-9]+),/gm
).sort((left, right) => left - right);
assert.deepEqual(
  nodeCommandCodes,
  rustCommandCodes,
  'Node COMMAND_CODE differs from the Rust command registry'
);

const enumValues = (source, pattern) =>
  new Map(
    [...source.matchAll(pattern)].map((match) => [
      match[1],
      Number(match[2])
    ])
  );
const rustCodeValues = enumValues(
  rustCodes,
  /^pub const ([A-Z0-9_]+_CODE): u32 = ([0-9]+);$/gm
);
const nodeCodeValues = enumValues(
  nodeCommandBlock,
  /^\s*([A-Za-z0-9]+):\s*([0-9]+),/gm
);
const rustOperations = enumValues(
  rustOperation,
  /^\s+([A-Za-z0-9]+)\s*=\s*([0-9]+),$/gm
);
const nodeOperations = enumValues(
  nodeOperation,
  /^\s+([A-Za-z0-9]+):\s*([0-9]+),?$/gm
);
assert.deepEqual(
  nodeOperations,
  rustOperations,
  'Node Operation differs from the Rust consensus enum'
);

const nodeReplicatedBlock =
  nodeOperation.match(
    /const REPLICATED_OPERATION[\s\S]*?new Map\(\[([\s\S]*?)\]\);/
  )?.[1] ?? '';
const rustReplicated = [...rustDispatch.matchAll(
  /CommandMeta::replicated\(\s*([A-Z0-9_]+),[\s\S]*?Operation::([A-Za-z0-9]+)/g
)].map((match) => [
  rustCodeValues.get(match[1]),
  rustOperations.get(match[2])
]).sort(([left], [right]) => left - right);
const nodeReplicated = [...nodeReplicatedBlock.matchAll(
  /\[COMMAND_CODE\.([A-Za-z0-9]+), Operation\.([A-Za-z0-9]+)\]/g
)].map((match) => [
  nodeCodeValues.get(match[1]),
  nodeOperations.get(match[2])
]).sort(([left], [right]) => left - right);
assert.ok(rustReplicated.length > 0, 'Rust replicated command map was not found');
assert.ok(nodeReplicated.length > 0, 'Node replicated command map was not found');
assert.deepEqual(
  nodeReplicated,
  rustReplicated,
  'Node replicated code-to-operation map differs from Rust dispatch'
);

// No namespace-packing parity to check: the client wire carries no routing
// namespace, so the packing rules stay entirely server-side and this SDK has
// nothing to mirror. What still matters is that the client never grows a
// namespace field back -- the offset recomputation below catches that, since
// reintroducing one would move every field after it.
assert.ok(
  !/namespace/i.test(nodeHeader.replace(/\/\*[\s\S]*?\*\/|\/\/.*/g, '')),
  'Node request header must not carry a namespace field'
);

const rustEvictionBlock =
  rustHeader.match(/pub enum EvictionReason \{([\s\S]*?)\n\}/)?.[1] ?? '';
const rustEvictions = enumValues(
  rustEvictionBlock,
  /^\s+([A-Za-z0-9]+)\s*=\s*([0-9]+),$/gm
);
const nodeEvictionBlock =
  nodeHeader.match(/export const EvictionReason = \{([\s\S]*?)\n\}/)?.[1] ??
  '';
const nodeEvictions = enumValues(
  nodeEvictionBlock,
  /^\s+([A-Za-z0-9]+):\s*([0-9]+),?$/gm
);
assert.deepEqual(
  nodeEvictions,
  rustEvictions,
  'Node EvictionReason differs from the Rust consensus enum'
);

const rustCommandBlock =
  rustCommand.match(/pub enum Command \{([\s\S]*?)\n\}/)?.[1] ?? '';
const rustCommandTable = enumValues(
  rustCommandBlock,
  /^\s+([A-Za-z0-9]+)\s*=\s*([0-9]+),$/gm
);
const nodeCommandTable = enumValues(
  nodeHeader.match(/export const Command = \{([\s\S]*?)\n\}/)?.[1] ?? '',
  /^\s+([A-Za-z0-9]+):\s*([0-9]+),?$/gm
);
assert.ok(nodeCommandTable.size > 0, 'Node Command table was not found');
for (const [name, value] of nodeCommandTable)
  assert.equal(
    rustCommandTable.get(name),
    value,
    `Node Command.${name} differs from Rust`
  );

const rustHeaderSize = Number(
  rustHeader.match(/pub const HEADER_SIZE: usize = ([0-9]+);/)?.[1]
);
const nodeHeaderSize = Number(
  nodeHeader.match(/export const HEADER_SIZE = ([0-9]+);/)?.[1]
);
assert.equal(nodeHeaderSize, rustHeaderSize, 'VSR header size differs');

// Header field offsets: recompute the #[repr(C)] layout from the Rust struct
// declarations so a field inserted before the consumed offsets fails here
// instead of desyncing the hardcoded Node tables.
const FIELD_LAYOUT = new Map([
  ['u8', [1, 1]],
  ['u16', [2, 2]],
  ['u32', [4, 4]],
  ['u64', [8, 8]],
  ['u128', [16, 16]],
  ['Command', [1, 1]],
  ['Operation', [1, 1]],
  ['EvictionReason', [1, 1]]
]);

const rustStructOffsets = (structName) => {
  const body = rustHeader.match(
    new RegExp(`pub struct ${structName} \\{([\\s\\S]*?)\\n\\}`)
  )?.[1];
  assert.ok(body, `Rust struct ${structName} was not found`);
  const offsets = new Map();
  let offset = 0;
  for (const [, field, type] of body.matchAll(
    /pub ([a-z_0-9]+): ([A-Za-z0-9_]+|\[u8; [0-9]+\]),/g
  )) {
    const arrayLength = type.match(/\[u8; ([0-9]+)\]/)?.[1];
    const [size, align] = arrayLength
      ? [Number(arrayLength), 1]
      : FIELD_LAYOUT.get(type) ?? [];
    assert.ok(
      size !== undefined,
      `unknown Rust field type ${type} in ${structName}`
    );
    offset = Math.ceil(offset / align) * align;
    offsets.set(field, offset);
    offset += size;
  }
  assert.equal(
    offset,
    rustHeaderSize,
    `computed ${structName} layout does not fill HEADER_SIZE`
  );
  return offsets;
};

const nodeOffsetTable = (tableName) => {
  const block = nodeHeader.match(
    new RegExp(`export const ${tableName} = \\{([\\s\\S]*?)\\n\\} as const;`)
  )?.[1];
  assert.ok(block, `Node offset table ${tableName} was not found`);
  return enumValues(block, /^\s+([A-Za-z0-9]+):\s*([0-9]+),?$/gm);
};

const toSnakeCase = (name) =>
  name.replace(/([A-Z])/g, '_$1').toLowerCase();

for (const [tableName, structName] of [
  ['REQUEST_OFFSET', 'RequestHeader'],
  ['REPLY_OFFSET', 'ReplyHeader'],
  ['EVICTION_OFFSET', 'EvictionHeader']
]) {
  const rustOffsets = rustStructOffsets(structName);
  for (const [field, value] of nodeOffsetTable(tableName))
    assert.equal(
      value,
      rustOffsets.get(toSnakeCase(field)),
      `Node ${tableName}.${field} differs from the Rust ${structName} layout`
    );
}

// Operation classification: evaluate the compiled Node predicates against
// the band constants and allowlists declared by the Rust enum.
const operationModule = await import(
  pathToFileURL(resolve(nodeRoot, 'dist/wire/vsr/operation.js')).href
);
const internalStart = rustOperations.get('CreateTopicWithAssignments');
const metadataStart = rustOperations.get('CreateStream');
const partitionStart = rustOperations.get('SendMessages');
const rustMetadataNames = new Set(
  [...(rustOperation.match(
    /fn is_metadata[\s\S]*?matches!\(\s*self,([\s\S]*?)\)\s*\n\s*\}/
  )?.[1] ?? '').matchAll(/Self::([A-Za-z0-9]+)/g)].map((match) => match[1])
);
assert.ok(rustMetadataNames.size > 0, 'Rust is_metadata allowlist not found');
const rustResultFramedNames = new Set(
  [...(rustOperation.match(
    /fn is_result_framed[\s\S]*?matches!\(\s*self,([\s\S]*?)\)\s*\n\s*\}/
  )?.[1] ?? '').matchAll(/Self::([A-Za-z0-9]+)/g)].map((match) => match[1])
);
assert.ok(
  rustResultFramedNames.size > 0,
  'Rust is_result_framed allowlist not found'
);
for (const [name, value] of rustOperations) {
  const internal = value >= internalStart && value < metadataStart;
  const metadata = internal || rustMetadataNames.has(name);
  assert.equal(
    operationModule.isMetadata(value),
    metadata,
    `Node isMetadata(${name}) differs from Rust is_metadata`
  );
  assert.equal(
    operationModule.isPartition(value),
    value >= partitionStart,
    `Node isPartition(${name}) differs from Rust is_partition`
  );
  assert.equal(
    operationModule.isResultFramed(value),
    metadata || rustResultFramedNames.has(name),
    `Node isResultFramed(${name}) differs from Rust is_result_framed`
  );
}

const protocolVersion =
  rustProtocolCargo.match(/^version = "([0-9]+)\.([0-9]+)\.([0-9]+)/m);
assert.ok(protocolVersion, 'binary protocol crate version is missing');
const nodePackedVersion = nodeRegister.match(
  /export const IGGY_PROTOCOL_VERSION =\s*\(([0-9]+) << 20\) \| \(([0-9]+) << 10\) \| ([0-9]+);/
);
assert.ok(nodePackedVersion, 'Node packed protocol version source changed');
assert.deepEqual(
  [Number(nodePackedVersion[1]), Number(nodePackedVersion[2])],
  [Number(protocolVersion[1]), Number(protocolVersion[2])],
  'Node protocol major.minor differs from iggy_binary_protocol'
);

console.log('Node VSR protocol mirror matches Rust sources');
