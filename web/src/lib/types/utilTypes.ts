export type CloseModalFn = (cb?: () => Promise<void>) => void;

export type Entries<T> = {
  [K in keyof T]: [K, T[K]];
}[keyof T][];

export type ObjectKeys<T> = T extends object
  ? (keyof T)[]
  : T extends number
  ? []
  : T extends Array<any> | string
  ? string[]
  : never;
