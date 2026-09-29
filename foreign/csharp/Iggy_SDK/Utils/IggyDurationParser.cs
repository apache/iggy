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

using System.Text;

namespace Apache.Iggy.Utils;

/// <summary>
///     Parses humantime durations such as <c>5s</c> or <c>1h 30m</c>. The input is lowercased and the zero spellings
///     are mapped first. The grammar keeps u64 overflow checks and exact fraction division, so a value that loses
///     precision is rejected, not rounded.
/// </summary>
internal static class IggyDurationParser
{
    private const ulong NanosPerSecond = 1_000_000_000;

    private const string OverflowMessage =
        "number is too large or cannot be represented without a lack of precision (values below 1ns are not supported)";

    private static readonly Dictionary<string, Unit> Units = new()
    {
        ["nanos"] = Unit.Nanosecond,
        ["nsec"] = Unit.Nanosecond,
        ["ns"] = Unit.Nanosecond,
        ["usec"] = Unit.Microsecond,
        ["us"] = Unit.Microsecond,
        ["µs"] = Unit.Microsecond,
        ["millis"] = Unit.Millisecond,
        ["msec"] = Unit.Millisecond,
        ["ms"] = Unit.Millisecond,
        ["seconds"] = Unit.Second,
        ["second"] = Unit.Second,
        ["secs"] = Unit.Second,
        ["sec"] = Unit.Second,
        ["s"] = Unit.Second,
        ["minutes"] = Unit.Minute,
        ["minute"] = Unit.Minute,
        ["min"] = Unit.Minute,
        ["mins"] = Unit.Minute,
        ["m"] = Unit.Minute,
        ["hours"] = Unit.Hour,
        ["hour"] = Unit.Hour,
        ["hr"] = Unit.Hour,
        ["hrs"] = Unit.Hour,
        ["h"] = Unit.Hour,
        ["days"] = Unit.Day,
        ["day"] = Unit.Day,
        ["d"] = Unit.Day,
        ["weeks"] = Unit.Week,
        ["week"] = Unit.Week,
        ["wk"] = Unit.Week,
        ["wks"] = Unit.Week,
        ["w"] = Unit.Week,
        ["months"] = Unit.Month,
        ["month"] = Unit.Month,
        ["years"] = Unit.Year,
        ["year"] = Unit.Year,
        ["yr"] = Unit.Year,
        ["yrs"] = Unit.Year,
        ["y"] = Unit.Year
    };

    private enum Unit
    {
        Nanosecond,
        Microsecond,
        Millisecond,
        Second,
        Minute,
        Hour,
        Day,
        Week,
        Month,
        Year
    }

    /// <summary>
    ///     Parses a duration such as <c>5s</c>, <c>1h 30m</c> or <c>unlimited</c> (zero).
    /// </summary>
    /// <exception cref="FormatException">
    ///     Thrown when the value does not parse or does not fit in a <see cref="TimeSpan" />.
    /// </exception>
    internal static TimeSpan Parse(string value)
    {
        var (seconds, nanoseconds) = ParseParts(value);
        try
        {
            // TimeSpan counts 100 ns ticks, so a sub-tick remainder is dropped.
            return TimeSpan.FromTicks(checked((long)seconds * TimeSpan.TicksPerSecond + nanoseconds / 100));
        }
        catch (OverflowException)
        {
            throw new FormatException(OverflowMessage);
        }
    }

    /// <summary>
    ///     Parses like <see cref="Parse" />, but without the <see cref="TimeSpan" /> range limit.
    /// </summary>
    /// <exception cref="FormatException">Thrown when the value does not parse.</exception>
    internal static (ulong Seconds, uint Nanoseconds) ParseParts(string value)
    {
        var lowered = value.ToLowerInvariant();
        return lowered is "0" or "unlimited" or "disabled" or "none" ? (0, 0) : ParseHumantime(lowered);
    }

    /// <summary>
    ///     Parses a lowercase humantime duration into whole seconds plus the subsecond nanoseconds.
    /// </summary>
    /// <exception cref="FormatException">Thrown when the value does not parse.</exception>
    internal static (ulong Seconds, uint Nanoseconds) ParseHumantime(string input)
    {
        try
        {
            return new Parser(input).Parse();
        }
        catch (OverflowException)
        {
            throw new FormatException(OverflowMessage);
        }
    }

    private static ulong Mul(ulong left, ulong right)
    {
        return checked(left * right);
    }

    private static ulong Add(ulong left, ulong right)
    {
        return checked(left + right);
    }

    // A division that is not exact would lose precision, which humantime reports as an overflow.
    private static ulong Div(ulong dividend, ulong divisor)
    {
        if (dividend % divisor != 0)
        {
            throw new FormatException(OverflowMessage);
        }

        return dividend / divisor;
    }

    private readonly record struct Fraction(ulong Numerator, ulong Denominator);

    /// <summary>
    ///     Walks the input by char. Every accepted character, including Unicode whitespace and <c>µ</c>, is a single
    ///     UTF-16 char, so a surrogate fails at its first half. Error offsets are UTF-8 byte offsets.
    /// </summary>
    private sealed class Parser(string source)
    {
        private int _index;
        private ulong _nanoseconds;
        private ulong _seconds;

        internal (ulong Seconds, uint Nanoseconds) Parse()
        {
            var number = ParseFirstChar() ?? throw new FormatException("value was empty");
            while (true)
            {
                Fraction? fraction = null;
                var offset = _index;
                while (Next() is { } c)
                {
                    if (IsDigit(c))
                    {
                        number = Add(Mul(number, 10), Digit(c));
                    }
                    else if (char.IsWhiteSpace(c))
                    {
                    }
                    else if (IsUnitChar(c))
                    {
                        break;
                    }
                    else if (c == '.')
                    {
                        fraction = ParseFractionalPart(ref offset);
                        break;
                    }
                    else
                    {
                        throw InvalidCharacter(offset);
                    }

                    offset = _index;
                }

                var start = offset;
                offset = _index;
                var nextNumber = false;
                while (Next() is { } c)
                {
                    if (IsDigit(c))
                    {
                        AddUnit(number, fraction, start, offset);
                        number = Digit(c);
                        nextNumber = true;
                        break;
                    }

                    if (char.IsWhiteSpace(c))
                    {
                        break;
                    }

                    if (!IsUnitChar(c))
                    {
                        throw InvalidCharacter(offset);
                    }

                    offset = _index;
                }

                if (nextNumber)
                {
                    continue;
                }

                AddUnit(number, fraction, start, offset);
                if (ParseFirstChar() is not { } next)
                {
                    return (_seconds, (uint)_nanoseconds);
                }

                number = next;
            }
        }

        private ulong? ParseFirstChar()
        {
            var offset = _index;
            while (Next() is { } c)
            {
                if (IsDigit(c))
                {
                    return Digit(c);
                }

                if (!char.IsWhiteSpace(c))
                {
                    throw new FormatException($"expected number at {ByteOffset(offset)}");
                }
            }

            return null;
        }

        private Fraction ParseFractionalPart(ref int offset)
        {
            ulong numerator = 0;
            ulong denominator = 1;
            var zeros = true;
            while (Next() is { } c)
            {
                if (c == '0')
                {
                    denominator = Mul(denominator, 10);
                    if (!zeros)
                    {
                        numerator = Mul(numerator, 10);
                    }
                }
                else if (IsDigit(c))
                {
                    zeros = false;
                    denominator = Mul(denominator, 10);
                    numerator = Add(Mul(numerator, 10), Digit(c));
                }
                else if (char.IsWhiteSpace(c))
                {
                }
                else if (IsUnitChar(c))
                {
                    break;
                }
                else
                {
                    throw InvalidCharacter(offset);
                }

                offset = _index;
            }

            // No digits after the separator, e.g. "1.".
            if (denominator == 1)
            {
                throw InvalidCharacter(offset);
            }

            return new Fraction(numerator, denominator);
        }

        private void AddUnit(ulong number, Fraction? fraction, int start, int end)
        {
            var unitText = source[start..end];
            if (!Units.TryGetValue(unitText, out var unit))
            {
                throw new FormatException(unitText.Length == 0
                    ? $"time unit needed, for example {number}sec or {number}ms"
                    : $"unknown time unit \"{unitText}\", supported units: ns, us/µs, ms, sec, min, hours, days, "
                      + "weeks, months, years (and few variations)");
            }

            var (seconds, nanoseconds) = unit switch
            {
                Unit.Nanosecond => (0UL, number),
                Unit.Microsecond => (0UL, Mul(number, 1000)),
                Unit.Millisecond => (0UL, Mul(number, 1_000_000)),
                Unit.Second => (number, 0UL),
                Unit.Minute => (Mul(number, 60), 0UL),
                Unit.Hour => (Mul(number, 3600), 0UL),
                Unit.Day => (Mul(number, 86_400), 0UL),
                Unit.Week => (Mul(number, 86_400 * 7), 0UL),
                Unit.Month => (Mul(number, 2_630_016), 0UL),
                _ => (Mul(number, 31_557_600), 0UL)
            };
            AddCurrent(seconds, nanoseconds);

            if (fraction is not { } part)
            {
                return;
            }

            var (numerator, denominator) = (part.Numerator, part.Denominator);
            (seconds, nanoseconds) = unit switch
            {
                Unit.Nanosecond => throw new FormatException(OverflowMessage),
                Unit.Microsecond => (0UL, Div(Mul(numerator, 1000), denominator)),
                Unit.Millisecond => (0UL, Div(Mul(numerator, 1_000_000), denominator)),
                Unit.Second => (0UL, Div(Mul(numerator, NanosPerSecond), denominator)),
                Unit.Minute => (0UL, Div(Mul(numerator, 60 * NanosPerSecond), denominator)),
                Unit.Hour => (Div(Mul(numerator, 3600), denominator), 0UL),
                Unit.Day => (Div(Mul(numerator, 86_400), denominator), 0UL),
                Unit.Week => (Div(Mul(numerator, 86_400 * 7), denominator), 0UL),
                Unit.Month => (Div(Mul(numerator, 2_630_016), denominator), 0UL),
                _ => (Div(Mul(numerator, 31_557_600), denominator), 0UL)
            };
            AddCurrent(seconds, nanoseconds);
        }

        private void AddCurrent(ulong seconds, ulong nanoseconds)
        {
            nanoseconds = Add(_nanoseconds, nanoseconds);
            if (nanoseconds >= NanosPerSecond)
            {
                seconds = Add(seconds, nanoseconds / NanosPerSecond);
                nanoseconds %= NanosPerSecond;
            }

            _seconds = Add(_seconds, seconds);
            _nanoseconds = nanoseconds;
        }

        private char? Next()
        {
            return _index < source.Length ? source[_index++] : null;
        }

        private FormatException InvalidCharacter(int offset)
        {
            return new FormatException($"invalid character at {ByteOffset(offset)}");
        }

        private int ByteOffset(int offset)
        {
            return Encoding.UTF8.GetByteCount(source.AsSpan(0, offset));
        }

        private static bool IsDigit(char c)
        {
            return c is >= '0' and <= '9';
        }

        private static ulong Digit(char c)
        {
            return (ulong)(c - '0');
        }

        private static bool IsUnitChar(char c)
        {
            return c is >= 'a' and <= 'z' or 'µ';
        }
    }
}
