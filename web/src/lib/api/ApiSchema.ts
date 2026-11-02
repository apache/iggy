// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import type {
  OptionInputValue,
  StreamCreateBody,
  StreamUpdateBody,
  TopicCreateBody,
  TopicUpdateBody,
  UserUpdateBody
} from '#lib/api/resourceOptions.js';
import type { GlobalPermissions, StreamPermissions } from '#lib/domain/Permissions.js';
import type { KeysToSnakeCase } from '#lib/utils/utilTypes.js';

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
        options?: Record<string, OptionInputValue>;
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
      method: 'PUT';
      path: `/users/${number}`;
      body: UserUpdateBody;
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
      body: StreamCreateBody;
    }
  | {
      method: 'PUT';
      path: `/streams/${number}`;
      body: StreamUpdateBody;
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
      body: TopicCreateBody;
    }
  | {
      method: 'PUT';
      path: `/streams/${number}/topics/${number}`;
      body: TopicUpdateBody;
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

type BaseApiSchema = Users | Streams | Stats | Topics | Auth;

export type ApiSchema = BaseApiSchema & {
  queryParams?: Record<string, string | number | boolean>;
};
