import { formatDuration, intervalToDuration, isValid } from 'date-fns';
import { number } from 'zod';

export const durationFormatter = (seconds: number) => {
  if (seconds <= 0 || seconds.toString().length > 11 || !isValid(seconds)) return '';

  const duration = intervalToDuration({ start: 0, end: seconds * 1000 });

  return formatDuration(duration, {
    format: ['years', 'months', 'days', 'hours', 'minutes', 'seconds'],
    zero: false,
    delimiter: ', '
  });
};
