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
 * Reverses the keys and values of a record object.
 * Creates a new record where the original values become keys and vice versa.
 *
 * @typeParam T - Type of the original keys
 * @typeParam U - Type of the original values (becomes keys in result)
 * @param input - Record object to reverse
 * @returns New record with swapped keys and values
 */
export function reverseRecord<
  T extends PropertyKey,
  U extends PropertyKey,
>(input: Record<T, U>) {
  return Object.fromEntries(
    Object.entries(input).map(([key, value]) => [
      value,
      key,
    ]),
  ) as Record<U, T>
}

/**
 * Extracts the value types from an object type.
 *
 * @typeParam T - Object type to extract values from
 */
export type ValueOf<T> = T[keyof T];
