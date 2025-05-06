import type { HandleClientError } from '@sveltejs/kit';

export const handleError: HandleClientError = async ({ error }) => {
  console.log('client error handler', error);
  return {
    message: 'Whoops!',
    errorId: 1
  };
};
