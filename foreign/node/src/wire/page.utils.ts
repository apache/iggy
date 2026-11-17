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

import { OPERATION_TIMEOUT_MS } from '../client/client.operation.js';
import type { ClientProvider } from '../client/client.type.js';
import type { Command } from './command.utils.js';
import { DeserializeError, deserializeError, responseError } from './error.utils.js';
import { decodeCanonicalBase64 } from './system/metadata-changes.utils.js';

/** Text prefix of a page cursor. */
export const PAGE_CURSOR_PREFIX = 'pg1_';

/** Items per page when the query does not say. */
export const DEFAULT_PAGE_LIMIT = 100;

/** Most items one page returns. */
export const MAX_PAGE_LIMIT = 1000;

/** Most bytes of a cursor: its length on the wire is one byte. */
export const MAX_PAGE_CURSOR_SIZE = 255;

/** Most bytes of a name filter in UTF-8: its length on the wire is one byte. */
export const MAX_PAGE_NAME_SIZE = 255;

const MAX_PAGE_CURSOR_TEXT_LENGTH = PAGE_CURSOR_PREFIX.length + Math.ceil(MAX_PAGE_CURSOR_SIZE * 4 / 3);
const INVALID_COMMAND = 3;
/** A cursor is parsed outside of any command, so its refusal names none. */
const NO_COMMAND = 0;
/** `[order:u8][descending:u8][limit:u32][cursor_len:u8]` and `[name_len:u8]` */
const PAGE_BLOCK_FIXED_SIZE = 8;
/** `[count:u32]` after the next cursor of a reply */
const PAGE_COUNT_SIZE = 4;
const PAGE_NAME = 'list page';

/** Order of a list page. */
export const PageOrder = {
  /** The numeric ids */
  Id: 'id',
  /**
   * The creation order. Entities created at the same time keep the order of
   * their ids. Partitions and consumer groups walk by id in this order too,
   * because their ids follow creation.
   */
  Created: 'created'
} as const;

export type PageOrder = typeof PageOrder[keyof typeof PageOrder];

/** The wire code of each order. */
export const PAGE_ORDER_CODE: Readonly<Record<PageOrder, number>> = { id: 0, created: 1 };

/**
 * The position after which the next page of a list starts. Send it back as
 * you got it, with the query that returned it: it holds no order, direction,
 * filter or parent. Only the server reads its bytes, so the client keeps any
 * 1 to 255 bytes as they came, and the server refuses a cursor that it cannot
 * use.
 *
 * Its text form, `toString()` and `toJSON()`, is `pg1_` followed by unpadded
 * Base64url of the bytes.
 */
export class PageCursor {
  readonly #bytes: Buffer;

  private constructor(bytes: Buffer) {
    this.#bytes = bytes;
  }

  /**
   * Reads the text form. Anything but the prefix and canonical unpadded
   * Base64url of 1 to 255 bytes is refused.
   *
   * @throws ResponseError with code 3 (invalid command)
   */
  static parse(text: string): PageCursor {
    if (text.length > MAX_PAGE_CURSOR_TEXT_LENGTH)
      throw responseError(NO_COMMAND, INVALID_COMMAND, `a page cursor holds at most ${MAX_PAGE_CURSOR_SIZE} bytes`);
    const bytes = decodeCanonicalBase64(text, PAGE_CURSOR_PREFIX, NO_COMMAND);
    if (bytes.length === 0)
      throw responseError(NO_COMMAND, INVALID_COMMAND, 'a page cursor holds at least one byte');
    return new PageCursor(bytes);
  }

  /**
   * Keeps a copy of the cursor bytes a reply carries.
   *
   * @throws DeserializeError for 0 or more than 255 bytes
   */
  static fromBytes(bytes: Buffer): PageCursor {
    if (bytes.length === 0 || bytes.length > MAX_PAGE_CURSOR_SIZE)
      throw new DeserializeError(
        `cannot deserialize a page cursor of ${bytes.length} bytes, it holds 1 to ${MAX_PAGE_CURSOR_SIZE}`
      );
    return new PageCursor(Buffer.from(bytes));
  }

  /** A copy of the bytes a list request carries */
  toBytes(): Buffer {
    return Buffer.from(this.#bytes);
  }

  toString(): string {
    return PAGE_CURSOR_PREFIX + this.#bytes.toString('base64url');
  }

  toJSON(): string {
    return this.toString();
  }
}

/**
 * One page request of a list command. To read the next page, send the same
 * query with the `next` of the page as its `cursor`.
 *
 * Refused with `ResponseError` code 3 (invalid command) before anything is
 * sent: an unknown order, a limit outside 1 to 1000 and a cursor that is not a
 * `PageCursor`.
 */
export type PageQuery = {
  /** Defaults to `id` */
  order?: PageOrder,
  /** Walks from the end of the order, false when absent */
  descending?: boolean,
  /** Items per page, 1 to 1000, 100 when absent */
  limit?: number,
  /** The `next` of the previous page, absent for the first page */
  cursor?: PageCursor
};

/**
 * One page request of a list whose entries have a name: streams, topics,
 * consumer groups and users. Also refused before anything is sent: a name
 * that is empty, longer than 255 bytes of UTF-8 or not well-formed Unicode.
 */
export type NamedPageQuery = PageQuery & {
  /** Keeps only the entries whose name holds this text, case-sensitive */
  name?: string
};

/** One page of a list. A page can be short or empty and still have a `next`. */
export type Page<T> = {
  items: T[],
  /** The cursor of the next page, absent on the last page */
  next?: PageCursor
};

/** Reads one item at `position`, returning it with its size. */
export type PageItemReader<T> = (payload: Buffer, position: number) => { bytesRead: number, data: T };

/**
 * The page block that ends every list request: `[order:u8][descending:u8]
 * [limit:u32][cursor_len:u8][cursor][name_len:u8][name]`. A length of 0 means
 * no cursor or no name.
 *
 * @throws ResponseError with code 3 (invalid command) against `commandCode`
 * for a query that breaks a page rule
 */
export const serializeNamedPageQuery = (commandCode: number, query: NamedPageQuery): Buffer => {
  const { order = PageOrder.Id, descending = false, limit = DEFAULT_PAGE_LIMIT, cursor, name } = query;
  const refused = (detail: string) => responseError(commandCode, INVALID_COMMAND, detail);
  if (!Object.hasOwn(PAGE_ORDER_CODE, order))
    throw refused(`unknown order ${order}`);
  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_PAGE_LIMIT)
    throw refused(`limit ${limit} is outside 1 to ${MAX_PAGE_LIMIT}`);
  if (cursor !== undefined && !(cursor instanceof PageCursor))
    throw refused('a cursor is a PageCursor');
  const nameBytes = Buffer.from(name ?? '', 'utf8');
  if (name !== undefined && (nameBytes.length < 1 || nameBytes.length > MAX_PAGE_NAME_SIZE))
    throw refused(`a name holds 1 to ${MAX_PAGE_NAME_SIZE} bytes of UTF-8, got ${nameBytes.length}`);
  // UTF-8 replaces a lone surrogate, so the server would read another name.
  if (name !== undefined && nameBytes.toString('utf8') !== name)
    throw refused('a name is well-formed Unicode text');

  const cursorBytes = cursor?.toBytes() ?? Buffer.alloc(0);
  const block = Buffer.alloc(PAGE_BLOCK_FIXED_SIZE + cursorBytes.length + nameBytes.length);
  let position = block.writeUInt8(PAGE_ORDER_CODE[order], 0);
  position = block.writeUInt8(descending ? 1 : 0, position);
  position = block.writeUInt32LE(limit, position);
  position = block.writeUInt8(cursorBytes.length, position);
  position += cursorBytes.copy(block, position);
  position = block.writeUInt8(nameBytes.length, position);
  nameBytes.copy(block, position);
  return block;
};

/**
 * The page block of a list without a name filter, so its name length is 0.
 *
 * @throws ResponseError with code 3 (invalid command) against `commandCode`
 * for a query that breaks a page rule or that has a `name` property
 */
export const serializePageQuery = (commandCode: number, query: PageQuery): Buffer => {
  // TypeScript lets a NamedPageQuery pass as a PageQuery, and JS callers skip types.
  if ((query as NamedPageQuery).name !== undefined)
    throw responseError(commandCode, INVALID_COMMAND, 'this list has no name filter');
  return serializeNamedPageQuery(commandCode, query);
};

/**
 * Reads the reply of a list command: `[next_len:u8][next][count:u32][item]*`.
 * A body short of `count` items, or with bytes after them, is refused. The
 * next cursor keeps its bytes as they came, because only the server reads
 * them.
 *
 * @throws DeserializeError for a malformed reply
 */
export const deserializePage = <T>(payload: Buffer, readItem: PageItemReader<T>): Page<T> => {
  if (payload.length === 0)
    deserializeError(PAGE_NAME, 0, payload.length, 1);
  const countPosition = 1 + payload.readUInt8(0);
  let position = countPosition + PAGE_COUNT_SIZE;
  if (payload.length < position)
    deserializeError(PAGE_NAME, 0, payload.length, position);
  const count = payload.readUInt32LE(countPosition);

  const items: T[] = [];
  for (let index = 0; index < count; index++) {
    let item: { bytesRead: number, data: T };
    try {
      item = readItem(payload, position);
    } catch (cause) {
      throw new DeserializeError(`cannot deserialize ${PAGE_NAME}: item ${index} of ${count}`, { cause });
    }
    position += item.bytesRead;
    items.push(item.data);
  }
  // An item decoder can cut a name short and still count its full length, so
  // the items can end past the reply as well as before its end.
  if (position !== payload.length)
    throw new DeserializeError(
      `cannot deserialize ${PAGE_NAME}: its ${count} items end at byte ${position} of ${payload.length}`
    );
  return countPosition === 1
    ? { items }
    : { items, next: PageCursor.fromBytes(payload.subarray(1, countPosition)) };
};

/**
 * Reads a whole list one page at a time, in ascending id order and in pages
 * of 1000, until a page has no next cursor. The result is not one consistent
 * read: an entity that someone creates or deletes during the walk can appear
 * or not appear. The walk keeps the sign-in and the deadline that it started
 * with.
 *
 * @throws DeserializeError for a page whose next cursor is the cursor its
 * request sent, because the walk would never end
 */
export const readWholeList = async <I, T>(
  getClient: ClientProvider, command: Command<I, Page<T>>, request: (query: PageQuery) => I
): Promise<T[]> => {
  const client = await getClient();
  const release = client.hold?.();
  try {
    const call = { deadline: Date.now() + OPERATION_TIMEOUT_MS, lineage: client.lineage };
    const items: T[] = [];
    let query: PageQuery = { order: PageOrder.Id, descending: false, limit: MAX_PAGE_LIMIT };
    for (;;) {
      const page = command.deserialize(
        await client.sendCommand(command.code, command.serialize(request(query)), call));
      // One push per item: a spread of a huge page could overflow the call stack.
      for (const item of page.items)
        items.push(item);
      if (page.next === undefined)
        return items;
      if (query.cursor?.toBytes().equals(page.next.toBytes()))
        throw new DeserializeError(
          `cannot deserialize ${PAGE_NAME}: the server answered the page cursor it was sent`
        );
      query = { ...query, cursor: page.next };
    }
  } finally {
    release?.();
  }
};
