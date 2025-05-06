import { fetchIggyApi } from '$lib/api/fetchApi';
import { handleFetchErrors } from '$lib/api/handleFetchErrors';
import { error, json, type RequestHandler } from '@sveltejs/kit';
import { convertBigIntsToStrings } from '$lib/api/convertBigIntsToStrings';

export const POST: RequestHandler = async ({ request, cookies }) => {
  const { path, body, method, queryParams } = await request.json();

  if (!path || !method) {
    const message = `routes/api/proxy/+server.ts no path or body or method provided`;
    console.error(message);
    error(500, {
            message
          });
  }

  const result = await fetchIggyApi({ body, path, method, cookies, queryParams });

  const { data, response } = await handleFetchErrors(result, cookies);

  const safeData = convertBigIntsToStrings(data);

  return json({ data: safeData, ok: response.ok, status: response.status });
};
