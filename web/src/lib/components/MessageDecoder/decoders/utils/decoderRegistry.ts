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
