import { setError } from 'sveltekit-superforms/client';

export const dataHas = (data: unknown, ...args: string[]) => {
  if (data && typeof data === 'object') {
    return args.every((arg) => arg in data);
  }

  return false;
};
