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

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  DurationParseError,
  nanosecondsToMilliseconds,
  parseHumantimeDuration,
  parseIggyDurationNanoseconds
} from './duration.utils.js';

const ns = (
  seconds: number | bigint,
  nanos = 0n
): bigint => BigInt(seconds) * 1_000_000_000n + nanos;

const parseNs = (input: string): bigint => {
  const { seconds, nanoseconds } = parseHumantimeDuration(input);
  return seconds * 1_000_000_000n + nanoseconds;
};

describe('parseHumantimeDuration', () => {
  it('parses every unit spelling', () => {
    const cases: [string, bigint][] = [
      ['17nsec', ns(0n, 17n)],
      ['17nanos', ns(0n, 17n)],
      ['33ns', ns(0n, 33n)],
      ['3usec', ns(0n, 3000n)],
      ['78us', ns(0n, 78_000n)],
      ['163µs', ns(0n, 163_000n)],
      ['31msec', ns(0n, 31_000_000n)],
      ['31millis', ns(0n, 31_000_000n)],
      ['6ms', ns(0n, 6_000_000n)],
      ['3000s', ns(3000n)],
      ['300secs', ns(300n)],
      ['50seconds', ns(50n)],
      ['100m', ns(6000n)],
      ['12mins', ns(720n)],
      ['7minutes', ns(420n)],
      ['2h', ns(7200n)],
      ['7hrs', ns(25_200n)],
      ['24hours', ns(86_400n)],
      ['2days', ns(172_800n)],
      ['365d', ns(31_536_000n)],
      ['7weeks', ns(4_233_600n)],
      ['104wks', ns(62_899_200n)],
      ['52w', ns(31_449_600n)],
      ['3months', ns(3n * 2_630_016n)],
      ['15yrs', ns(15n * 31_557_600n)],
      ['10yr', ns(10n * 31_557_600n)],
      ['17y', ns(536_479_200n)]
    ];
    for (const [input, expected] of cases)
      assert.equal(parseNs(input), expected, input);
  });

  it('combines tokens with and without whitespace', () => {
    assert.equal(parseNs('2h 37min'), ns(9420n));
    assert.equal(parseNs('2h 15m'), ns(8100n));
    assert.equal(parseNs('20 min 17 nsec '), ns(1200n, 17n));
    assert.equal(parseNs('1.234s0.345ms0.678us0ns'), ns(1n, 234_345_678n));
    assert.equal(
      parseNs('1.234s 1.345ms 1.678us 1ns'),
      ns(1n, 235_346_679n)
    );
  });

  it('supports fractional values with exact division only', () => {
    assert.equal(parseNs('4.2s'), 4_200_000_000n);
    assert.equal(parseNs('1.5minute'), ns(90n));
    assert.equal(parseNs('0.5h'), ns(1800n));
    assert.equal(parseNs('1.123456789s'), ns(1n, 123_456_789n));
    assert.equal(parseNs('31.000001ms'), ns(0n, 31_000_001n));
    // Precision losses are rejected rather than truncated.
    for (const input of [
      '0.000123456789s',
      '31.0000001ms',
      '1.0000000002s',
      '0.0000000002s'
    ])
      assert.throws(() => parseHumantimeDuration(input), (error: unknown) =>
        error instanceof DurationParseError &&
        error.kind === 'number-overflow'
      );
  });

  it('rejects malformed fractional input', () => {
    for (const input of ['1.s', '1..s'])
      assert.throws(() => parseHumantimeDuration(input), (error: unknown) =>
        error instanceof DurationParseError &&
        error.kind === 'invalid-character'
      );
    for (const input of ['.1s', '.'])
      assert.throws(() => parseHumantimeDuration(input), (error: unknown) =>
        error instanceof DurationParseError &&
        error.kind === 'number-expected'
      );
  });

  it('reports overflow like u64 arithmetic', () => {
    for (const input of [
      '100000000000000000000ns',
      '100000000000000ms',
      '10000000000000000000m',
      '100000000000000000d',
      '10000000000000y'
    ])
      assert.throws(() => parseHumantimeDuration(input), (error: unknown) =>
        error instanceof DurationParseError &&
        error.kind === 'number-overflow'
      );
  });

  it('produces the Rust error messages verbatim', () => {
    const messageOf = (input: string): string => {
      try {
        parseHumantimeDuration(input);
      } catch (error) {
        return (error as Error).message;
      }
      return '';
    };
    assert.equal(
      messageOf('123'),
      'time unit needed, for example 123sec or 123ms'
    );
    assert.equal(
      messageOf('10 months 1'),
      'time unit needed, for example 1sec or 1ms'
    );
    assert.equal(
      messageOf('10nights'),
      'unknown time unit "nights", supported units: ns, us/µs, ms, sec, ' +
      'min, hours, days, weeks, months, years (and few variations)'
    );
    assert.equal(messageOf('\0'), 'expected number at 0');
    assert.equal(messageOf('\r'), 'value was empty');
    assert.equal(messageOf('1~'), 'invalid character at 1');
    assert.equal(messageOf('1Nå'), 'invalid character at 2');
    assert.equal(
      messageOf('222nsec221nanosmsec7s5msec572s'),
      'unknown time unit "nanosmsec", supported units: ns, us/µs, ms, ' +
      'sec, min, hours, days, weeks, months, years (and few variations)'
    );
  });
});

describe('parseIggyDurationNanoseconds', () => {
  it('maps the zero spellings to zero case-insensitively', () => {
    for (const value of [
      '0', 'unlimited', 'disabled', 'none', 'UNLIMITED', 'None', 'Disabled'
    ]) {
      assert.equal(parseIggyDurationNanoseconds(value), 0n);
      // The literal "0" is also short-circuited by humantime itself.
      assert.equal(nanosecondsToMilliseconds(0n), 0);
    }
  });

  it('rounds sub-millisecond results half up', () => {
    assert.equal(nanosecondsToMilliseconds(parseIggyDurationNanoseconds('500ms')), 500);
    assert.equal(nanosecondsToMilliseconds(parseIggyDurationNanoseconds('17ns')), 0);
    assert.equal(nanosecondsToMilliseconds(parseIggyDurationNanoseconds('1.5ms')), 2);
    assert.equal(nanosecondsToMilliseconds(parseIggyDurationNanoseconds('1.005s')), 1005);
  });
});
