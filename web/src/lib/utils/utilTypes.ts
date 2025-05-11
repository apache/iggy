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

export type NonNegativeInteger<T extends number> = `${T}` extends
  | `-${string}`
  | `${string}.${string}`
  ? never
  : T;

type RemoveUnderscoreFirstLetter<S extends string> = S extends `${infer FirstLetter}${infer U}`
  ? `${FirstLetter extends '_' ? U : `${FirstLetter}${U}`}`
  : S;

type CamelToSnakeCase<S extends string> = S extends `${infer T}${infer U}`
  ? `${T extends Capitalize<T> ? '_' : ''}${RemoveUnderscoreFirstLetter<
      Lowercase<T>
    >}${CamelToSnakeCase<U>}`
  : S;

export type KeysToSnakeCase<T extends object> = {
  [K in keyof T as CamelToSnakeCase<K & string>]: T[K];
};
