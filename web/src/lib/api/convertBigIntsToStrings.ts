export const convertBigIntsToStrings = <T>(data: T): T => {
  if (typeof data === 'bigint') {
    return data.toString() as unknown as T;
  }

  if (Array.isArray(data)) {
    return data.map(item => convertBigIntsToStrings(item)) as unknown as T;
  }

  if (typeof data === 'object' && data !== null) {
    const result = {} as T;
    for (const [key, value] of Object.entries(data)) {
      result[key as keyof T] = convertBigIntsToStrings(value);
    }
    return result;
  }

  return data;
};
