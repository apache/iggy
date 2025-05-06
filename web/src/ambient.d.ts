import type { Entries, ObjectKeys } from '$lib/types/utilTypes';

declare global {
  interface ObjectConstructor {
    entries<T extends object>(o: T): Entries<T>;

    keys<T>(o: T): ObjectKeys<T>;
  }
}
