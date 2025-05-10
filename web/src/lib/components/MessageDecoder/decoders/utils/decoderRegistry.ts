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

import type { Decoder } from '../types';
import { jsonDecoder } from '../BuiltInDecoders/jsonDecoder';
import { xmlDecoder } from '../BuiltInDecoders/xmlDecoder';
import { stringDecoder } from '../BuiltInDecoders/stringDecoder';

class DecoderRegistry {
  private decoders: Map<string, Decoder> = new Map();

  constructor() {
    this.registerBuiltInDecoders();
  }

  private registerBuiltInDecoders() {
    [jsonDecoder, xmlDecoder, stringDecoder].forEach((decoder) => {
      this.register(decoder);
    });
  }

  public register(decoder: Decoder) {
    this.decoders.set(decoder.name.toLowerCase(), decoder);
  }

  public get(name: string): Decoder | undefined {
    return this.decoders.get(name.toLowerCase());
  }

  public getNames(): string[] {
    return Array.from(this.decoders.keys());
  }
}

export const decoderRegistry = new DecoderRegistry();
