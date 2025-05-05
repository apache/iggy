import prettyBytes from 'pretty-bytes';

export function bytesFormatter(bytes: any) {
  // The bytes size string is already formatted by the API
  return bytes;

  // const over1Mb = bytes > 1000000;
  // const decimalPlaces = over1Mb ? 2 : 0;

  // return prettyBytes(bytes, {
  //   maximumFractionDigits: decimalPlaces,
  //   minimumFractionDigits: decimalPlaces
  // });

  // if (bytes === 0) return '0 Bytes';

  // const k = 1024;
  // const sizes = ['Bytes', 'kB', 'MB', 'GB'] as const;

  // const i = Math.floor(Math.log(bytes) / Math.log(k));

  // const size = sizes[i];
  // const decimalPlaces = size !== 'Bytes' && size !== 'kB' ? 2 : 0;

  // return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${size}`;
}
