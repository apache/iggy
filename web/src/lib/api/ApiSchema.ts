import type { GlobalPermissions, StreamPermissions } from '$lib/domain/Permissions';
import type { KeysToSnakeCase } from '$lib/utils/utilTypes';

type Permissions = {
  global: KeysToSnakeCase<GlobalPermissions>;
  streams: Record<number, StreamPermissions>;
};

type Users =
  | { method: 'POST'; path: '/users/login'; body: { username: string; password: string } }
  | { method: 'POST'; path: '/users/logout' }
  | {
      method: 'POST';
      path: '/users';
      body: {
        username: string;
        password: string;
        status: 'active' | 'inactive';
        permissions: Permissions | null;
      };
    }
  | {
      method: 'GET';
      path: '/users';
    }
  | {
      method: 'GET';
      path: `/users/${number}`;
    }
  | {
      method: 'GET';
      path: `/users/${number}`;
      body: {
        username: string;
        status: 'active' | 'inactive';
        permissions: Permissions | null;
      };
    }
  | {
      method: 'PUT';
      path: `/users/${number}/password`;
      body: {
        current_password: string;
        new_password: string;
      };
    }
  | {
      method: 'PUT';
      path: `/users/${number}/permissions`;
      body: {
        permissions: Permissions;
      };
    }
  | {
      method: 'DELETE';
      path: `/users/${number}`;
    };

type Streams =
  | {
      method: 'GET';
      path: '/streams';
    }
  | {
      method: 'GET';
      path: `/streams/${number}`;
    }
  | {
      method: 'POST';
      path: '/streams';
      body: {
        stream_id: number;
        name: string;
      };
    }
  | {
      method: 'PUT';
      path: `/streams/${number}`;
      body: {
        name: string;
      };
    }
  | {
      method: 'DELETE';
      path: `/streams/${number}`;
    };

type Topics =
  | {
      method: 'GET';
      path: `/streams/${number}/topics`;
    }
  | {
      method: 'GET';
      path: `/streams/${number}/topics/${number}`;
    }
  | {
      method: 'GET';
      path: `/streams/${number}/topics/${number}/messages`;
    }
  | {
      method: 'POST';
      path: `/streams/${number}/topics`;
      body: {
        compression_algorithm: "none" | "gzip";
        max_topic_size: number;
        message_expiry: number;
        name: string;
        partitions_count: number;
        stream_id: number;
        topic_id: number;
      };
    }
  | {
      method: 'PUT';
      path: `/streams/${number}/topics/${number}`;
      body: {
        name: string;
        message_expiry: number;
        compression_algorithm: number;
      };
    }
  | {
      method: 'DELETE';
      path: `/streams/${number}/topics/${number}`;
    }
  | {
      method: 'POST';
      path: `/streams/${number}/topics/${number}/partitions`;
      body: {
        partitions_count: number;
      };
    }
  | {
      method: 'DELETE';
      path: `/streams/${number}/topics/${number}/partitions`;
      queryParams: {
        partitions_count: number;
      };
    };

type Auth =
  | {
      path: '/users/login';
      method: 'POST';
      body: {
        username: string;
        password: string;
      };
    }
  | {
      method: 'POST';
      path: '/users/logout';
    }
  | {
      path: '/users/refresh-token';
      method: 'POST';
      body: {
        token: string;
      };
    };

type Stats = {
  method: 'GET';
  path: '/stats';
};

export type ApiSchema = Users | Streams | Stats | Topics | Auth;
