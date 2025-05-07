import { formatDate } from '$lib/utils/formatters/dateFormatter';

export type User = {
  id: number;
  createdAt: string;
  status: 'active' | 'inactive';
  username: string;
};

export function userMapper(item: any): User {
  return {
    id: item.id,
    createdAt: formatDate(item.created_at),
    status: item.status,
    username: `${item.username} ${item.id === 1 ? '(root)' : ''}`
  };
}
