export interface Decoder {
  name: string;
  decode: (payload: string) => string | null;
}
