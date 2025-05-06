
export const toDate = (n: bigint): Date => new Date(Number(n / BigInt(1000)));

export const serializeUUID = (id: string) => Buffer.from(id.replaceAll('-', ''), 'hex');

export const deserializeUUID = (p: Buffer) => {
  const v = p.toString('hex');
  return `${v.slice(0, 8)}-` +
    `${v.slice(8, 12)}-${v.slice(12, 16)}-${v.slice(16, 20)}-` +
    `${v.slice(20, 32)}`;
};
