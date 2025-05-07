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
