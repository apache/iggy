import type { ApiSchema } from './ApiSchema';
import { getJson } from './getJson';

export const fetchRouteApi = async (
  arg: ApiSchema
): Promise<{ data: any; status: number; ok: boolean }> => {
  try {
    const res = await fetch('/api/proxy', { body: JSON.stringify(arg), method: 'POST' });
    return (await getJson(res)) as any;
  } catch (err) {
    throw new Error('fetchRouteApi error');
  }
};
