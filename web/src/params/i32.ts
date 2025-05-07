export function match(value: any) {
  return /^-?\d+(\.\d+)?$/.test(value);
}
