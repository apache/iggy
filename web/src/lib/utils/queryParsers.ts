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

import type { NonNegativeInteger } from './utilTypes';
import { z } from 'zod';

export function positiveIntOrDefault<T extends number>(
  value: unknown,
  defaultValue: NonNegativeInteger<T>
) {
  const parsed = z.number().safeParse(value);
  return parsed.success ? parsed.data : defaultValue;
}

export function stringOrDefault(value: unknown, defaultValue: string) {
  const parsed = z.string().safeParse(value);
  return parsed.success ? parsed.data : defaultValue;
}

export function oneOfOrDefault<T extends string[]>(
  value: unknown,
  valuesSet: T,
  defaultValue: T[number]
): T[number] {
  const parsed = z.string().safeParse(value);

  if (parsed.success) {
    return valuesSet.includes(parsed.data) ? parsed.data : defaultValue;
  } else {
    return defaultValue;
  }
}
