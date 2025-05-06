import { env } from '$env/dynamic/public';
import type { ApiSchema } from '$lib/api/ApiSchema';
import { tokens } from '$lib/utils/constants/tokens';
import { type Cookies } from '@sveltejs/kit';

export async function fetchIggyApi(
  args: ApiSchema & { queryParams?: Record<string, string>; cookies?: Cookies }
): Promise<Response | unknown> {
  const { path, method, queryParams, cookies } = args;

  try {
    const headers = new Headers();
    headers.set('Content-Type', 'application/json');

    if (cookies) {
      const accessToken = cookies.get(tokens.accessToken);
      if (accessToken) headers.set('Authorization', `Bearer ${accessToken}`);
    }

    let fullUrl = `${env.PUBLIC_IGGY_API_URL}${path}`;

    if (queryParams) {
      const query = new URLSearchParams(Object.entries(queryParams));
      fullUrl += '?' + query.toString();
    }

    const result = await fetch(fullUrl, {
      headers,
      method,
      ...('body' in args ? { body: JSON.stringify(args.body) } : {})
    });

    return result;
  } catch (e: unknown) {
    return e;
  }
}
