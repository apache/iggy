/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import JSONBigInt from 'json-bigint';

// Configure json-bigint to parse as strings to avoid BigInt issues with math operations
const JSONbig = JSONBigInt({
  useNativeBigInt: true,
  alwaysParseAsBig: false,
  strict: false,
  // Custom reviver to convert BigInt to string to avoid math operation issues
  protoAction: 'ignore',
  constructorAction: 'ignore'
});

export const getJson = async (res: Response): Promise<unknown | null> => {
  const text = await res.text();
  try {
    // Parse with json-bigint which handles large numbers correctly
    const parsed = JSONbig.parse(text);

    // Convert any BigInt values to strings or numbers depending on size
    const json = convertBigInts(parsed);
    return json as unknown;
  } catch (err) {
    return null;
  }
};

// Recursively convert BigInt values to appropriate types
function convertBigInts(obj: any): any {
  if (obj === null || obj === undefined) {
    return obj;
  }

  if (typeof obj === 'bigint') {
    // If it fits in a safe number, convert to number for math operations
    // Otherwise keep as string for display
    if (obj <= Number.MAX_SAFE_INTEGER && obj >= Number.MIN_SAFE_INTEGER) {
      return Number(obj);
    }
    return obj.toString();
  }

  if (Array.isArray(obj)) {
    return obj.map(convertBigInts);
  }

  if (typeof obj === 'object') {
    const result: any = {};
    for (const key in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        result[key] = convertBigInts(obj[key]);
      }
    }
    return result;
  }

  return obj;
}
