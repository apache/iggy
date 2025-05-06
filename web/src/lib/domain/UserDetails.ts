import { permissionsMapper, type Permissions } from './Permissions';
import { userMapper, type User } from './User';

export type UserDetails = User & { permissions: Permissions };

export const userDetailsMapper = (item: any) => {
  return {
    ...userMapper(item),
    permissions: permissionsMapper(item.permissions)
  };
};
