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

/**
 * Checks if a u128 value looks like it could be a UUID
 * UUIDs typically have specific bit patterns, but for simplicity
 * we check if the value is within a reasonable range for UUIDs
 */
export function looksLikeUuid(id: string): boolean {
  try {
    const num = BigInt(id);
    // Check if the number is large enough to potentially be a UUID
    // UUIDs are 128-bit, so they're typically very large numbers
    // We'll consider anything > u64::MAX as potentially a UUID
    const u64Max = BigInt('18446744073709551615');
    return num > u64Max;
  } catch {
    return false;
  }
}

/**
 * Formats a u128 number as a UUID string (8-4-4-4-12 format)
 * Example: 340282366920938463463374607431768211455 -> ffffffff-ffff-ffff-ffff-ffffffffffff
 */
export function formatAsUuid(id: string): string {
  try {
    const num = BigInt(id);
    // Convert to hex and pad to 32 characters (128 bits = 32 hex chars)
    const hex = num.toString(16).padStart(32, '0');

    // Format as UUID: 8-4-4-4-12
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
  } catch {
    return id;
  }
}

/**
 * Formats a message ID intelligently:
 * - If it looks like a UUID, formats it as UUID
 * - Otherwise returns the raw string
 */
export function formatMessageId(id: string): string {
  if (looksLikeUuid(id)) {
    return formatAsUuid(id);
  }
  return id;
}
