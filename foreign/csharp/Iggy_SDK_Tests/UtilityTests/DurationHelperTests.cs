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

public sealed class DurationHelperTests
{
    [Fact]
    public void FromDuration_Zero_ReturnsZeroTimeSpan()
    {
        var result = DurationHelpers.FromDuration(0);

        Assert.Equal(TimeSpan.Zero, result);
    }

    [Fact]
    public void FromDuration_NormalValue_ReturnsCorrectTimeSpan()
    {
        ulong microseconds = 1_000_000;
        var result = DurationHelpers.FromDuration(microseconds);

        Assert.Equal(TimeSpan.FromSeconds(1), result);
    }

    [Fact]
    public void FromDuration_MillisecondsValue_ReturnsCorrectTimeSpan()
    {
        ulong microseconds = 5_000;
        var result = DurationHelpers.FromDuration(microseconds);

        Assert.Equal(TimeSpan.FromMilliseconds(5), result);
    }

    [Fact]
    public void FromDuration_LargeValidValue_ReturnsCorrectTimeSpan()
    {
        var microseconds = (ulong)TimeSpan.MaxValue.TotalMicroseconds;
        var result = DurationHelpers.FromDuration(microseconds);

        Assert.Equal(TimeSpan.FromMicroseconds(microseconds), result);
    }

    [Fact]
    public void FromDuration_ValueExceedsLongMaxValue_ReturnsTimeSpanMaxValue()
    {
        var microseconds = (ulong)long.MaxValue + 1;
        var result = DurationHelpers.FromDuration(microseconds);

        Assert.Equal(TimeSpan.MaxValue, result);
    }

    [Fact]
    public void FromDuration_ValueLongMaxValueDividedBy100_ReturnsNotTimeSpanMaxValue()
    {
        var microseconds = (ulong)long.MaxValue / 100;
        var result = DurationHelpers.FromDuration(microseconds);

        Assert.NotEqual(TimeSpan.MaxValue, result);
    }

    [Fact]
    public void FromDuration_ValueLongMaxValueDividedByTicksPerMicrosecond_ReturnsTimeSpanMaxValue()
    {
        var microseconds = (ulong)long.MaxValue / TimeSpan.TicksPerMicrosecond;
        var result = DurationHelpers.FromDuration(microseconds);

        Assert.Equal(TimeSpan.MaxValue, result);
    }

    [Fact]
    public void FromDuration_ValueLongMaxValue_ReturnsTimeSpanMaxValue()
    {
        var microseconds = (ulong)long.MaxValue;
        var result = DurationHelpers.FromDuration(microseconds);

        Assert.Equal(TimeSpan.MaxValue, result);
    }

    [Fact]
    public void FromDuration_UlongMaxValue_ReturnsTimeSpanMaxValue()
    {
        var result = DurationHelpers.FromDuration(ulong.MaxValue);

        Assert.Equal(TimeSpan.MaxValue, result);
    }

    [Fact]
    public void ToDuration_Null_ReturnsZero()
    {
        var result = DurationHelpers.ToDuration(null);

        Assert.Equal(0UL, result);
    }

    [Fact]
    public void ToDuration_ZeroTimeSpan_ReturnsZero()
    {
        var result = DurationHelpers.ToDuration(TimeSpan.Zero);

        Assert.Equal(0UL, result);
    }

    [Fact]
    public void ToDuration_NormalValue_ReturnsCorrectMicroseconds()
    {
        var duration = TimeSpan.FromSeconds(1);
        var result = DurationHelpers.ToDuration(duration);

        Assert.Equal(1_000_000UL, result);
    }

    [Fact]
    public void ToDuration_MillisecondsValue_ReturnsCorrectMicroseconds()
    {
        var duration = TimeSpan.FromMilliseconds(5);
        var result = DurationHelpers.ToDuration(duration);

        Assert.Equal(5_000UL, result);
    }

    [Fact]
    public void ToDuration_TimeSpanMaxValue_ReturnsUlongMaxValue()
    {
        var result = DurationHelpers.ToDuration(TimeSpan.MaxValue);

        Assert.Equal(ulong.MaxValue, result);
    }

    [Fact]
    public void ToDuration_LargeValue_ReturnsCorrectMicroseconds()
    {
        var duration = TimeSpan.FromDays(365);
        var result = DurationHelpers.ToDuration(duration);

        var expectedMicroseconds = (ulong)(365 * 24 * 60 * 60 * 1_000_000L);
        Assert.Equal(expectedMicroseconds, result);
    }

    [Fact]
    public void RoundTrip_NormalValue_PreservesValue()
    {
        ulong originalMicroseconds = 1_000_000;
        var timeSpan = DurationHelpers.FromDuration(originalMicroseconds);
        var result = DurationHelpers.ToDuration(timeSpan);

        Assert.Equal(originalMicroseconds, result);
    }

    [Fact]
    public void RoundTrip_Zero_PreservesValue()
    {
        ulong originalMicroseconds = 0;
        var timeSpan = DurationHelpers.FromDuration(originalMicroseconds);
        var result = DurationHelpers.ToDuration(timeSpan);

        Assert.Equal(originalMicroseconds, result);
    }

    [Fact]
    public void RoundTrip_UlongMaxValue_PreservesValue()
    {
        var originalMicroseconds = ulong.MaxValue;
        var timeSpan = DurationHelpers.FromDuration(originalMicroseconds);
        var result = DurationHelpers.ToDuration(timeSpan);

        Assert.Equal(originalMicroseconds, result);
    }

    [Fact]
    public void RoundTrip_TimeSpan_PreservesValue()
    {
        var originalTimeSpan = TimeSpan.FromHours(2);
        var microseconds = DurationHelpers.ToDuration(originalTimeSpan);
        var result = DurationHelpers.FromDuration(microseconds);

        Assert.Equal(originalTimeSpan, result);
    }

    [Fact]
    public void RoundTrip_TimeSpanMaxValue_PreservesValue()
    {
        var originalTimeSpan = TimeSpan.MaxValue;
        var microseconds = DurationHelpers.ToDuration(originalTimeSpan);
        var result = DurationHelpers.FromDuration(microseconds);

        Assert.Equal(originalTimeSpan, result);
    }

    [Theory]
    [InlineData(1UL)]
    [InlineData(1000UL)]
    [InlineData(1_000_000UL)]
    [InlineData(60_000_000UL)]
    [InlineData(3_600_000_000UL)]
    public void FromDuration_VariousValues_ReturnsExpectedTimeSpan(ulong microseconds)
    {
        var result = DurationHelpers.FromDuration(microseconds);

        Assert.Equal(TimeSpan.FromMicroseconds(microseconds), result);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(60000)]
    [InlineData(3600000)]
    public void ToDuration_VariousMilliseconds_ReturnsExpectedMicroseconds(int milliseconds)
    {
        var duration = TimeSpan.FromMilliseconds(milliseconds);
        var result = DurationHelpers.ToDuration(duration);

        Assert.Equal((ulong)milliseconds * 1000, result);
    }
}
