export function decodeBase64(str: string): string | null {
  try {
    return atob(str);
  } catch {
    return null;
  }
}
