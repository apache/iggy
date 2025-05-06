// See https://kit.svelte.dev/docs/types#app
// for information about these interfaces
declare global {
  namespace App {
    interface Locals {
      user: {
        name: string;
        role: 'admin' | 'user';
      };
    }
    // interface Error {}

    // interface PageData {
    //   user: {
    //     name: string;
    //     role: string;
    //   };
    // }
    // interface Platform {}
  }
}

export {};
