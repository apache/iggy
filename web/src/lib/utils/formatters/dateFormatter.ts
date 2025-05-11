/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { format, formatDuration, intervalToDuration } from 'date-fns';

export const formatDate = (date: number) => format(date / 1000, 'dd MMM yyyy HH:mm:ss');

export const formatDateWithMicroseconds = (timestamp: number) => {
  return format(timestamp / 1000, 'yyyy-MM-dd HH:mm:ss.SSS');
};

export const formatRuntime = (duration: number) => {
  return formatDuration(intervalToDuration({ start: 0, end: duration / 1000 }), {
    zero: true,
    format: ['years', 'months', 'days', 'hours', 'minutes', 'seconds'],
    delimiter: '',
    locale: {
      formatDistance: (_token, count) => {
        if (count === 0 && !['xHours', 'xMinutes', 'xSeconds'].includes(_token)) {
          return '';
        }

        switch (true) {
          case _token === 'xDays':
            return `${count}d:`;
          case _token === 'xMonths':
            return `${count}mo:`;
          case _token === 'xYears':
            return `${count}y:`;
          case _token === 'xHours':
            return `${String(count).padStart(2, '0')}:`;
          case _token === 'xMinutes':
            return `${String(count).padStart(2, '0')}:`;
          case _token === 'xSeconds':
            return String(count).padStart(2, '0');
        }
        return '';
      }
    }
  });
};
