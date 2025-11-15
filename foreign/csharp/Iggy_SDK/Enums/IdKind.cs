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


using System.Runtime.CompilerServices;

namespace Apache.Iggy.Enums;

/// <summary>
///     The kind of identifier.
/// </summary>
public enum IdKind
{
    /// <summary>
    ///     Numeric identifier.
    /// </summary>
    Numeric,

    /// <summary>
    ///     String identifier.
    /// </summary>
    String
}

internal static class Extensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static byte GetByte(this IdKind kind)
    {
        return kind switch
        {
            IdKind.Numeric => 1,
            IdKind.String => 2,
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static byte GetByte(this Partitioning kind)
    {
        return kind switch
        {
            Partitioning.Balanced => 1,
            Partitioning.PartitionId => 2,
            Partitioning.MessageKey => 3,
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
        };
    }
}
