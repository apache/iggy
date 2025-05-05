import { typedRoute } from '$lib/types/appRoutes';
import { tokens } from '$lib/utils/constants/tokens.js';
import { redirect, type Actions } from '@sveltejs/kit';

export const actions = {
  default({ cookies }) {
    // eat the cookie
    cookies.set(tokens.accessToken, '', {
      path: '/',
      expires: new Date(0)
    });

    console.log('deleting cookie');

    // redirect the user
    redirect(302, typedRoute('/auth/sign-in'));
  }
} satisfies Actions;
