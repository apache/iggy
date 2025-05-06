
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

export type ValueOf<T> = T[keyof T];
