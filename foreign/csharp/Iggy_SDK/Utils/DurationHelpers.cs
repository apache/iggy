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

namespace Apache.Iggy.Utils;

/// <summary>
///     Converts between Iggy durations (ulong microseconds) and .NET TimeSpan.
/// </summary>
/// <remarks>
///     Iggy stores durations (e.g., message expiry) as ulong microseconds, but TimeSpan uses long ticks internally.
///     Since ulong.MaxValue exceeds what TimeSpan can represent from microseconds, we map ulong.MaxValue to
///     TimeSpan.MaxValue and vice versa.
/// </remarks>
public static class DurationHelpers
{
    /// <summary>
    ///     Converts microseconds to TimeSpan. Returns TimeSpan.MaxValue if duration exceeds representable range.
    /// </summary>
    public static TimeSpan FromDuration(ulong duration)
    {
        if (duration >= long.MaxValue / TimeSpan.TicksPerMicrosecond)
        {
            return TimeSpan.MaxValue;
        }

        return TimeSpan.FromMicroseconds(duration);
    }

    /// <summary>
    ///     Converts TimeSpan to microseconds. Returns ulong.MaxValue if TimeSpan.MaxValue is passed.
    /// </summary>
    public static ulong ToDuration(TimeSpan? duration)
    {
        if (duration == null)
        {
            return 0;
        }

        if (duration == TimeSpan.MaxValue)
        {
            return ulong.MaxValue;
        }

        return (ulong)duration.Value.TotalMicroseconds;
    }
}
