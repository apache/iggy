
import { reverseRecord } from '../type.utils.js';

type CodeMap = Record<string, string>;

export const COMMAND_CODE: CodeMap = {
  Ping: '1',
  GetStats: '10',
  GetMe: '20',
  GetClient: '21',
  GetClients: '22',
  GetUser: '31',
  GetUsers: '32',
  CreateUser: '33',
  DeleteUser: '34',
  UpdateUser: '35',
  UpdatePermissions: '36',
  ChangePassword: '37',
  LoginUser: '38',
  LogoutUser: '39',
  GetAccessTokens: '41',
  CreateAccessToken: '42',
  DeleteAccessToken: '43',
  LoginWithAccessToken: '44',
  PollMessages: '100',
  SendMessages: '101',
  GetOffset: '120',
  StoreOffset: '121',
  GetStream: '200',
  GetStreams: '201',
  CreateStream: '202',
  DeleteStream: '203',
  UpdateStream: '204',
  PurgeStream: '205',
  GetTopic: '300',
  GetTopics: '301',
  CreateTopic: '302',
  DeleteTopic: '303',
  UpdateTopic: '304',
  PurgeTopic: '305',
  CreatePartitions: '402',
  DeletePartitions: '403',
  GetGroup: '600',
  GetGroups: '601',
  CreateGroup: '602',
  DeleteGroup: '603',
  JoinGroup: '604',
  LeaveGroup: '605',
};

const reverseCommandCodeMap = reverseRecord(COMMAND_CODE);

export const translateCommandCode = (code: number): string => {
  return reverseCommandCodeMap[code.toString()] || `unknow_command_code_${code}`
};
