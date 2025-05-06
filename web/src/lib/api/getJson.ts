export const getJson = async (res: Response): Promise<unknown | null> => {
  const text = await res.text();
  try {
    const json = JSON.parse(text, (_key, value) => {
      if (typeof value === 'number' && value > Number.MAX_SAFE_INTEGER) {
        return BigInt(value);
      }
      return value;
    });
    return json as unknown;
  } catch (err) {
    return null;
  }
};
