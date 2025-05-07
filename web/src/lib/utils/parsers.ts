export function isNumber(value: string) {
  return !isNaN(value as any);
}

export function parseNestedJson(input: any) {
  if (typeof input === 'string') {
    try {
      const parsed = JSON.parse(input);
      return parseNestedJson(parsed);
    } catch (e) {
      return input;
    }
  }

  if (input && typeof input === 'object' && !Array.isArray(input)) {
    const result = {};
    for (const [key, value] of Object.entries(input)) {
      result[key] = parseNestedJson(value);
    }
    return result;
  }

  if (Array.isArray(input)) {
    return input.map((item) => parseNestedJson(item));
  }

  return input;
}
