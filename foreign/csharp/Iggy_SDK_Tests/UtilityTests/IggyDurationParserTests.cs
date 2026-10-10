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

using Apache.Iggy.Utils;

namespace Apache.Iggy.Tests.UtilityTests;

public sealed class IggyDurationParserTests
{
    private const string OverflowMessage =
        "number is too large or cannot be represented without a lack of precision (values below 1ns are not supported)";

    private const string SupportedUnits =
        "supported units: ns, us/µs, ms, sec, min, hours, days, weeks, months, years (and few variations)";

    [Theory]
    [InlineData("17nsec", 0UL, 17U)]
    [InlineData("17nanos", 0UL, 17U)]
    [InlineData("33ns", 0UL, 33U)]
    [InlineData("3usec", 0UL, 3000U)]
    [InlineData("78us", 0UL, 78_000U)]
    [InlineData("163µs", 0UL, 163_000U)]
    [InlineData("31msec", 0UL, 31_000_000U)]
    [InlineData("31millis", 0UL, 31_000_000U)]
    [InlineData("6ms", 0UL, 6_000_000U)]
    [InlineData("3000s", 3000UL, 0U)]
    [InlineData("300secs", 300UL, 0U)]
    [InlineData("50seconds", 50UL, 0U)]
    [InlineData("1second", 1UL, 0U)]
    [InlineData("100m", 6000UL, 0U)]
    [InlineData("12mins", 720UL, 0U)]
    [InlineData("1min", 60UL, 0U)]
    [InlineData("7minutes", 420UL, 0U)]
    [InlineData("1minute", 60UL, 0U)]
    [InlineData("2h", 7200UL, 0U)]
    [InlineData("1hr", 3600UL, 0U)]
    [InlineData("7hrs", 25_200UL, 0U)]
    [InlineData("1hour", 3600UL, 0U)]
    [InlineData("24hours", 86_400UL, 0U)]
    [InlineData("1day", 86_400UL, 0U)]
    [InlineData("2days", 172_800UL, 0U)]
    [InlineData("365d", 31_536_000UL, 0U)]
    [InlineData("1week", 604_800UL, 0U)]
    [InlineData("7weeks", 4_233_600UL, 0U)]
    [InlineData("1wk", 604_800UL, 0U)]
    [InlineData("104wks", 62_899_200UL, 0U)]
    [InlineData("52w", 31_449_600UL, 0U)]
    [InlineData("1month", 2_630_016UL, 0U)]
    [InlineData("3months", 3 * 2_630_016UL, 0U)]
    [InlineData("1year", 31_557_600UL, 0U)]
    [InlineData("15yrs", 15 * 31_557_600UL, 0U)]
    [InlineData("10yr", 10 * 31_557_600UL, 0U)]
    [InlineData("17y", 536_479_200UL, 0U)]
    public void ParseHumantime_ParsesEveryUnitSpelling(string input, ulong seconds, uint nanoseconds)
    {
        Assert.Equal((seconds, nanoseconds), IggyDurationParser.ParseHumantime(input));
    }

    [Theory]
    [InlineData("2h 37min", 9420UL, 0U)]
    [InlineData("2h 15m", 8100UL, 0U)]
    [InlineData("1h 1m 1s", 3661UL, 0U)]
    [InlineData("1h30m", 5400UL, 0U)]
    [InlineData("20 min 17 nsec ", 1200UL, 17U)]
    [InlineData("  5s", 5UL, 0U)]
    [InlineData("1.234s0.345ms0.678us0ns", 1UL, 234_345_678U)]
    [InlineData("1.234s 1.345ms 1.678us 1ns", 1UL, 235_346_679U)]
    [InlineData("999999999ns 1ns", 1UL, 0U)]
    public void ParseHumantime_CombinesTokensWithAndWithoutWhitespace(string input, ulong seconds,
        uint nanoseconds)
    {
        Assert.Equal((seconds, nanoseconds), IggyDurationParser.ParseHumantime(input));
    }

    [Theory]
    [InlineData("4.2s", 4UL, 200_000_000U)]
    [InlineData("1.5minute", 90UL, 0U)]
    [InlineData("0.01m", 0UL, 600_000_000U)]
    [InlineData("0.5h", 1800UL, 0U)]
    [InlineData("1.123456789s", 1UL, 123_456_789U)]
    [InlineData("31.000001ms", 0UL, 31_000_001U)]
    [InlineData("1.5us", 0UL, 1500U)]
    public void ParseHumantime_SupportsExactFractions(string input, ulong seconds, uint nanoseconds)
    {
        Assert.Equal((seconds, nanoseconds), IggyDurationParser.ParseHumantime(input));
    }

    [Theory]
    [InlineData("0.000123456789s")]
    [InlineData("31.0000001ms")]
    [InlineData("1.0000000002s")]
    [InlineData("0.0000000002s")]
    [InlineData("1.5ns")]
    public void ParseHumantime_RejectsPrecisionLossAsOverflow(string input)
    {
        var exception = Assert.Throws<FormatException>(() => IggyDurationParser.ParseHumantime(input));

        Assert.Equal(OverflowMessage, exception.Message);
    }

    [Theory]
    [InlineData("100000000000000000000ns")]
    [InlineData("100000000000000ms")]
    [InlineData("10000000000000000000m")]
    [InlineData("100000000000000000d")]
    [InlineData("10000000000000y")]
    public void ParseHumantime_ReportsU64Overflow(string input)
    {
        var exception = Assert.Throws<FormatException>(() => IggyDurationParser.ParseHumantime(input));

        Assert.Equal(OverflowMessage, exception.Message);
    }

    [Theory]
    [InlineData("1.s", "invalid character at 1")]
    [InlineData("1..s", "invalid character at 1")]
    [InlineData(".1s", "expected number at 0")]
    [InlineData(".", "expected number at 0")]
    public void ParseHumantime_RejectsMalformedFractions(string input, string message)
    {
        var exception = Assert.Throws<FormatException>(() => IggyDurationParser.ParseHumantime(input));

        Assert.Equal(message, exception.Message);
    }

    [Theory]
    [InlineData("123", "time unit needed, for example 123sec or 123ms")]
    [InlineData("10 months 1", "time unit needed, for example 1sec or 1ms")]
    [InlineData("10nights", "unknown time unit \"nights\", " + SupportedUnits)]
    [InlineData("222nsec221nanosmsec7s5msec572s", "unknown time unit \"nanosmsec\", " + SupportedUnits)]
    [InlineData("\0", "expected number at 0")]
    [InlineData("\r", "value was empty")]
    [InlineData("", "value was empty")]
    [InlineData("1~", "invalid character at 1")]
    [InlineData("1nå", "invalid character at 2")]
    [InlineData("1N", "invalid character at 1")]
    [InlineData("1µå", "invalid character at 3")]
    public void ParseHumantime_ProducesErrorMessages(string input, string message)
    {
        var exception = Assert.Throws<FormatException>(() => IggyDurationParser.ParseHumantime(input));

        Assert.Equal(message, exception.Message);
    }

    [Theory]
    [InlineData("500ms", 500)]
    [InlineData("5s", 5000)]
    [InlineData("2m", 120_000)]
    [InlineData("1h", 3_600_000)]
    [InlineData("0.5s", 500)]
    [InlineData("1h 1m 1s", 3_661_000)]
    [InlineData("1h30m", 5_400_000)]
    [InlineData("5d", 432_000_000)]
    [InlineData("2w", 1_209_600_000)]
    [InlineData("1y", 31_557_600_000)]
    [InlineData("5sec", 5000)]
    [InlineData("5msec", 5)]
    [InlineData("1.005s", 1005)]
    [InlineData("0s", 0)]
    public void Parse_ConvertsToTimeSpan(string input, long milliseconds)
    {
        Assert.Equal(TimeSpan.FromMilliseconds(milliseconds), IggyDurationParser.Parse(input));
    }

    [Fact]
    public void Parse_KeepsSubMillisecondPrecisionDownToOneTick()
    {
        Assert.Equal(TimeSpan.FromMicroseconds(5), IggyDurationParser.Parse("5usec"));
        Assert.Equal(TimeSpan.FromTicks(5), IggyDurationParser.Parse("500nsec"));
        Assert.Equal(TimeSpan.Zero, IggyDurationParser.Parse("17ns"));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("unlimited")]
    [InlineData("disabled")]
    [InlineData("none")]
    [InlineData("UNLIMITED")]
    [InlineData("None")]
    [InlineData("Disabled")]
    public void Parse_MapsZeroSpellingsToZeroCaseInsensitively(string input)
    {
        Assert.Equal(TimeSpan.Zero, IggyDurationParser.Parse(input));
    }

    [Fact]
    public void Parse_IsCaseInsensitive()
    {
        Assert.Equal(TimeSpan.FromSeconds(3661), IggyDurationParser.Parse("1H 1M 1S"));
    }

    [Theory]
    [InlineData("5")]
    [InlineData("-1s")]
    [InlineData("ms")]
    [InlineData("")]
    [InlineData("abc")]
    [InlineData("s")]
    [InlineData("1 hour and 30 minutes")]
    public void Parse_RejectsInvalidDurations(string input)
    {
        Assert.Throws<FormatException>(() => IggyDurationParser.Parse(input));
    }

    [Fact]
    public void Parse_RejectsDurationsBeyondTimeSpan()
    {
        // Past the ~29,000 years a TimeSpan covers.
        var exception = Assert.Throws<FormatException>(() => IggyDurationParser.Parse("100000y"));

        Assert.Equal(OverflowMessage, exception.Message);
    }
}
