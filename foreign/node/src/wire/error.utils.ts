
import { translateCommandCode } from './command.code.js';
import { translateErrorCode } from './error.code.js';

export const responseError = (cmdCode: number, errCode: number) => new Error(
  `command: { code: ${cmdCode}, name: ${translateCommandCode(cmdCode)} } ` +
  `error: {code: ${errCode}, message: ${translateErrorCode(errCode)} }`
);
