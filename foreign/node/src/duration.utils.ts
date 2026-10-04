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

/**
 * TypeScript port of the duration grammar accepted by the Rust SDK.
 *
 * `parseHumantimeDuration` mirrors humantime 2.4.0's `parse_duration`,
 * which `IggyDuration::from_str` delegates to after lowercasing and
 * mapping its zero spellings. Control flow, the unit table, u64 overflow
 * checks, exact fraction division, and error messages are kept identical
 * so both SDKs accept and reject the same strings.
 */

const MAX_U64 = 18_446_744_073_709_551_615n;

export type DurationErrorKind =
  | 'invalid-character'
  | 'number-expected'
  | 'unknown-unit'
  | 'number-overflow'
  | 'empty';

/** Carries the message the corresponding Rust error's Display produces. */
export class DurationParseError extends TypeError {
  kind: DurationErrorKind;

  constructor(kind: DurationErrorKind, message: string) {
    super(message);
    this.kind = kind;
  }
}

const overflowError = () =>
  new DurationParseError(
    'number-overflow',
    'number is too large or cannot be represented without a lack of ' +
    'precision (values below 1ns are not supported)'
  );

const checkedMul = (a: bigint, b: bigint): bigint => {
  const product = a * b;
  if (product > MAX_U64)
    throw overflowError();
  return product;
};

const checkedAdd = (a: bigint, b: bigint): bigint => {
  const sum = a + b;
  if (sum > MAX_U64)
    throw overflowError();
  return sum;
};

// humantime's OverflowOp::div: a non-exact division loses precision,
// reported as overflow rather than truncated.
const checkedExactDiv = (a: bigint, b: bigint): bigint => {
  if (a % b !== 0n)
    throw overflowError();
  return a / b;
};

type UnitKind =
  | 'ns' | 'us' | 'ms' | 's' | 'm' | 'h' | 'd' | 'w' | 'M' | 'y';

const UNITS: Record<string, UnitKind> = {
  nanos: 'ns', nsec: 'ns', ns: 'ns',
  usec: 'us', us: 'us', 'µs': 'us',
  millis: 'ms', msec: 'ms', ms: 'ms',
  seconds: 's', second: 's', secs: 's', sec: 's', s: 's',
  minutes: 'm', minute: 'm', min: 'm', mins: 'm', m: 'm',
  hours: 'h', hour: 'h', hr: 'h', hrs: 'h', h: 'h',
  days: 'd', day: 'd', d: 'd',
  weeks: 'w', week: 'w', wk: 'w', wks: 'w', w: 'w',
  months: 'M', month: 'M',
  years: 'y', year: 'y', yr: 'y', yrs: 'y', y: 'y'
};

// Month is 30.44 days and year is 365.25 days, matching the Rust table.
const SECONDS_PER_UNIT: Record<UnitKind, bigint> = {
  ns: 0n, us: 0n, ms: 0n, s: 1n, m: 60n,
  h: 3600n, d: 86_400n, w: 604_800n, M: 2_630_016n, y: 31_557_600n
};

const NANOS_PER_UNIT: Record<UnitKind, bigint> = {
  ns: 1n, us: 1000n, ms: 1_000_000n, s: 0n, m: 0n,
  h: 0n, d: 0n, w: 0n, M: 0n, y: 0n
};

type Fraction = {
  numerator: bigint,
  denominator: bigint
};

type ParsedDuration = {
  seconds: bigint,
  nanoseconds: bigint
};

const utf8ByteLength = (char: string): number => {
  const code = char.codePointAt(0)!;
  return code <= 0x7f ? 1 : code <= 0x7ff ? 2 : code <= 0xffff ? 3 : 4;
};

const isDigit = (c: string): boolean => c >= '0' && c <= '9';
const isUnitChar = (c: string): boolean => /^[a-zA-Zµ]$/.test(c);

class DurationParser {
  private readonly chars: string[];
  private readonly byteLengths: number[];
  /** Index of the next unconsumed char. */
  private index = 0;
  /** Byte offset of the next unconsumed char; error offsets are byte-based. */
  private consumedBytes = 0;

  constructor(src: string) {
    this.chars = Array.from(src);
    this.byteLengths = this.chars.map(utf8ByteLength);
  }

  private off(): number {
    return this.consumedBytes;
  }

  private next(): string | undefined {
    const c = this.chars[this.index];
    if (c === undefined)
      return undefined;
    this.index += 1;
    this.consumedBytes += this.byteLengths[this.index - 1];
    return c;
  }

  /** Byte-exact source slice; unit spellings are ASCII-only. */
  private sliceBytes(startByte: number, endByte: number): string {
    let collected = '';
    let position = 0;
    for (let i = 0; i < this.chars.length && position < endByte; i += 1) {
      if (position >= startByte)
        collected += this.chars[i];
      position += this.byteLengths[i];
    }
    return collected;
  }

  parse(): ParsedDuration {
    // The error offset is fixed where scanning began, matching the Rust
    // implementation's single capture outside the loop.
    let n = this.parseFirstChar(this.off());
    // Rust: `.ok_or(Error::Empty)` on the first-char scan.
    if (n === undefined)
      throw new DurationParseError('empty', 'value was empty');
    let seconds = 0n;
    let nanoseconds = 0n;

    outer:
    while (true) {
      let fraction: Fraction | undefined;
      // Offsets refresh at iteration end only, so a break leaves the
      // offset pointing at the char that caused it.
      let off = this.off();
      while (true) {
        const c = this.next();
        if (c === undefined)
          break;
        if (isDigit(c)) {
          n = checkedAdd(checkedMul(n, 10n), BigInt(c));
        } else if (isUnitChar(c)) {
          break;
        } else if (c === '.') {
          // The scanner's final offset becomes the unit start.
          const scanned = this.parseFractionalPart(off);
          fraction = scanned.fraction;
          off = scanned.offset;
          break;
        } else if (!/\s/.test(c)) {
          throw new DurationParseError(
            'invalid-character',
            `invalid character at ${off}`
          );
        }
        off = this.off();
      }
      const start = off;
      let unitEnd = this.off();
      while (true) {
        const c = this.next();
        if (c === undefined)
          break;
        if (isDigit(c)) {
          ({ seconds, nanoseconds } = this.addUnit(
            n, fraction, start, unitEnd, seconds, nanoseconds
          ));
          n = BigInt(c);
          continue outer;
        }
        if (/\s/.test(c))
          break;
        if (!isUnitChar(c))
          throw new DurationParseError(
            'invalid-character',
            `invalid character at ${unitEnd}`
          );
        unitEnd = this.off();
      }
      ({ seconds, nanoseconds } = this.addUnit(
        n, fraction, start, unitEnd, seconds, nanoseconds
      ));
      const next = this.parseFirstChar(this.off());
      if (next === undefined)
        return { seconds, nanoseconds };
      n = next;
    }
  }

  private parseFirstChar(scanStart: number): bigint | undefined {
    for (;;) {
      const c = this.next();
      if (c === undefined)
        return undefined;
      if (isDigit(c))
        return BigInt(c);
      if (/\s/.test(c))
        continue;
      throw new DurationParseError(
        'number-expected',
        `expected number at ${scanStart}`
      );
    }
  }

  /**
   * Consumes fraction digits after the decimal separator. Whitespace
   * between digits is tolerated; the returned offset is the position the
   * scanner stopped at, which becomes the unit start.
   */
  private parseFractionalPart(
    startOffset: number
  ): { fraction: Fraction, offset: number } {
    let numerator = 0n;
    let denominator = 1n;
    // Leading zeros grow the denominator only.
    let zeros = true;
    let off = startOffset;
    for (;;) {
      const c = this.next();
      if (c === undefined) {
        off = this.off();
        break;
      }
      if (c === '0') {
        denominator = checkedMul(denominator, 10n);
        if (!zeros)
          numerator = checkedMul(numerator, 10n);
      } else if (isDigit(c)) {
        zeros = false;
        denominator = checkedMul(denominator, 10n);
        numerator = checkedAdd(checkedMul(numerator, 10n), BigInt(c));
      } else if (isUnitChar(c)) {
        break;
      } else if (!/\s/.test(c)) {
        throw new DurationParseError(
          'invalid-character',
          `invalid character at ${off}`
        );
      }
      off = this.off();
    }
    if (denominator === 1n)
      throw new DurationParseError(
        'invalid-character',
        `invalid character at ${off}`
      );
    return { fraction: { numerator, denominator }, offset: off };
  }

  private addUnit(
    n: bigint,
    fraction: Fraction | undefined,
    start: number,
    end: number,
    seconds: bigint,
    nanoseconds: bigint
  ): ParsedDuration {
    const unitSlice = this.sliceBytes(start, end);
    const kind = UNITS[unitSlice];
    if (kind === undefined) {
      const detail = unitSlice.length === 0
        ? `time unit needed, for example ${n}sec or ${n}ms`
        : 'unknown time unit "' + unitSlice + '", supported units: ns, ' +
          'us/µs, ms, sec, min, hours, days, weeks, months, years ' +
          '(and few variations)';
      throw new DurationParseError('unknown-unit', detail);
    }

    const intSeconds = checkedMul(n, SECONDS_PER_UNIT[kind]);
    const intNanos = checkedMul(n, NANOS_PER_UNIT[kind]);
    ({ seconds, nanoseconds } = addCurrent(
      seconds, nanoseconds, intSeconds, intNanos
    ));

    if (fraction === undefined)
      return { seconds, nanoseconds };

    // Fractional part: sub-nanosecond results are a precision loss.
    const { numerator, denominator } = fraction;
    let fracSeconds = 0n;
    let fracNanos = 0n;
    switch (kind) {
      case 'ns':
        throw overflowError();
      case 'us':
        fracNanos =
          checkedExactDiv(checkedMul(numerator, 1000n), denominator);
        break;
      case 'ms':
        fracNanos = checkedExactDiv(
          checkedMul(numerator, 1_000_000n), denominator
        );
        break;
      case 's':
        fracNanos = checkedExactDiv(
          checkedMul(numerator, 1_000_000_000n), denominator
        );
        break;
      default:
        fracSeconds = checkedExactDiv(
          checkedMul(numerator, SECONDS_PER_UNIT[kind]), denominator
        );
    }
    return addCurrent(seconds, nanoseconds, fracSeconds, fracNanos);
  }
}

const addCurrent = (
  seconds: bigint,
  nanoseconds: bigint,
  addSeconds: bigint,
  addNanos: bigint
): ParsedDuration => {
  // Strictly-greater carry preserved from the Rust implementation.
  let totalNanos = checkedAdd(nanoseconds, addNanos);
  let totalSeconds = addSeconds;
  if (totalNanos > 1_000_000_000n) {
    totalSeconds = checkedAdd(totalSeconds, totalNanos / 1_000_000_000n);
    totalNanos %= 1_000_000_000n;
  }
  return {
    seconds: checkedAdd(seconds, totalSeconds),
    nanoseconds: totalNanos
  };
};

/** Parses a humantime expression into seconds plus subsecond nanos. */
export const parseHumantimeDuration = (input: string): ParsedDuration => {
  // Rust short-circuits the literal "0" before the parser runs.
  if (input === '0')
    return { seconds: 0n, nanoseconds: 0n };
  return new DurationParser(input).parse();
};

const ZERO_SPELLINGS = new Set(['0', 'unlimited', 'disabled', 'none']);

/**
 * Mirrors `IggyDuration::from_str`: case-insensitive, with the zero
 * spellings mapped to a zero duration before humantime parsing.
 *
 * @returns Total nanoseconds as BigInt.
 */
export const parseIggyDurationNanoseconds = (value: string): bigint => {
  const lowered = value.toLowerCase();
  if (ZERO_SPELLINGS.has(lowered))
    return 0n;
  const { seconds, nanoseconds } = parseHumantimeDuration(lowered);
  return seconds * 1_000_000_000n + nanoseconds;
};

/** Rounds nanoseconds to whole milliseconds, half up. */
export const nanosecondsToMilliseconds = (totalNanoseconds: bigint): number =>
  Number((totalNanoseconds + 500_000n) / 1_000_000n);
