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
//

import { HeaderKind } from './message/header.type.js';
import {
  serializeHeaders,
  deserializeHeaderValue,
  HeaderKeyFactory,
  type HeaderValue,
  type ParsedHeaderValue,
} from './message/header.utils.js';

/** Maximum number of key-value entries in one options block. */
export const MAX_OPTIONS = 1024;

/**
 * Maximum total byte length of an encoded options block.
 *
 * Mirrors the Rust `MAX_OPTIONS_BYTES`, which in turn mirrors the user-headers
 * budget: options ride that codec and inherit its limit.
 */
export const MAX_OPTIONS_BYTES = 100 * 1000;

/**
 * Key and value length bound, inherited from the header-field codec rather
 * than being an options-specific rule (`serializeHeaders` enforces the same
 * range on the way out).
 */
const MAX_HEADER_FIELD_LENGTH = 255;

/** A resource option entry: UTF-8 string key with a typed value. */
export type OptionEntry = {
  key: string,
  value: HeaderValue
};

/** Deserialized options block keyed by option name. */
export type ParsedOptions = Record<string, ParsedHeaderValue>;

/** Result of deserializing a length-prefixed options block. */
export type OptionsDeserialized = {
  /** Number of bytes consumed, length prefix included */
  bytesRead: number,
  /** Deserialized options */
  options: ParsedOptions
};

/**
 * Serializes resource options into a TLV block.
 * Reuses the user-headers TLV encoding: each field is
 * `[kind:u8][len:u32_le][bytes]`, alternating key, value.
 * Empty options serialize to zero bytes.
 *
 * @param options - Option entries to serialize
 * @returns Serialized options block
 * @throws Error if an options constraint is violated
 */
export const serializeOptions = (options: OptionEntry[]): Buffer => {
  if (options.length > MAX_OPTIONS)
    throw new Error(
      `Options block has ${options.length} entries, exceeds maximum ${MAX_OPTIONS}`);

  // No key-length check here: `serializeHeaders` already bounds every field
  // to 1..=255, so an options-specific cap would only duplicate it.
  const block = serializeHeaders(options.map(({ key, value }) =>
    ({ key: HeaderKeyFactory.String(key), value })));

  if (block.length > MAX_OPTIONS_BYTES)
    throw new Error(
      `Options block is ${block.length} bytes, exceeds maximum ${MAX_OPTIONS_BYTES}`);

  return block;
};

/**
 * Deserializes a bare options TLV block spanning `[pos, end)`.
 *
 * @param p - Buffer containing the options block
 * @param pos - Starting position of the block
 * @param end - End position of the block (exclusive)
 * @returns Deserialized options keyed by option name
 * @throws Error if a key is not a string or the block is malformed
 */
export const deserializeOptions = (
  p: Buffer, pos = 0, end = p.length
): ParsedOptions => {
  const options: ParsedOptions = {};
  while (pos < end) {
    const keyKind = p.readUInt8(pos);
    if (keyKind !== HeaderKind.String)
      throw new Error(`Option key kind ${keyKind} is not a string`);
    const keyLength = p.readUInt32LE(pos + 1);
    if (keyLength < 1 || keyLength > MAX_HEADER_FIELD_LENGTH)
      throw new Error(
        `Invalid option key length: ${keyLength}, ` +
        `must be between 1 and ${MAX_HEADER_FIELD_LENGTH}`);
    if (pos + 5 + keyLength > end)
      throw new Error('Option key overruns the block');
    const key = p.subarray(pos + 5, pos + 5 + keyLength).toString();
    pos += 5 + keyLength;

    const valueKind = p.readUInt8(pos);
    const valueLength = p.readUInt32LE(pos + 1);
    if (pos + 5 + valueLength > end)
      throw new Error(`Option value for key '${key}' overruns the block`);
    const valueBytes = p.subarray(pos + 5, pos + 5 + valueLength);
    pos += 5 + valueLength;

    let value: ParsedHeaderValue;
    try {
      value = deserializeHeaderValue(valueKind, valueBytes);
    } catch {
      // Unknown value kinds stay raw bytes, mirroring the wire
      // forward-compatibility contract for options.
      value = valueBytes;
    }
    options[key] = value;
  }
  return options;
};

/**
 * Deserializes a `u32_le`-length-prefixed options block at `pos`.
 *
 * @param p - Buffer containing `[options_len:u32_le][options TLV]`
 * @param pos - Starting position of the length prefix
 * @returns Bytes consumed (prefix included) and deserialized options
 */
export const deserializePrefixedOptions = (
  p: Buffer, pos = 0
): OptionsDeserialized => {
  const length = p.readUInt32LE(pos);
  const end = pos + 4 + length;
  // Without this, `subarray` clamps a truncated block silently: a known-kind
  // value throws inside `deserializeHeaderValue`, the forward-compat catch
  // swallows it and hands back raw bytes, and `bytesRead` over-reports so every
  // later field decodes from the wrong offset.
  if (end > p.length)
    throw new Error(
      `Options block overruns the payload: ${length} bytes declared at ${pos}, ` +
      `${p.length - pos - 4} available`);
  const options = deserializeOptions(p, pos + 4, end);
  return { bytesRead: 4 + length, options };
};
