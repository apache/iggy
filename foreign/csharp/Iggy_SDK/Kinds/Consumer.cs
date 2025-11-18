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

using Apache.Iggy.Enums;

namespace Apache.Iggy.Kinds;

/// <summary>
///     Consumer identifier
/// </summary>
public readonly struct Consumer
{
    /// <summary>
    ///     Consumer type.
    /// </summary>
    public required ConsumerType Type { get; init; }

    /// <summary>
    ///     Consumer identifier.
    /// </summary>
    public required Identifier ConsumerId { get; init; }

    /// <summary>
    ///     Creates a new regular consumer identifier.
    /// </summary>
    /// <param name="id">Identifier value</param>
    /// <returns>Consumer instance</returns>
    public static Consumer New(int id)
    {
        return new Consumer
        {
            ConsumerId = Identifier.Numeric(id),
            Type = ConsumerType.Consumer
        };
    }

    /// <summary>
    ///     Creates a new regular consumer identifier.
    /// </summary>
    /// <param name="id">Identifier value</param>
    /// <returns>Consumer instance</returns>
    public static Consumer New(string id)
    {
        return new Consumer
        {
            ConsumerId = Identifier.String(id),
            Type = ConsumerType.Consumer
        };
    }

    /// <summary>
    ///     Creates a new consumer group identifier.
    /// </summary>
    /// <param name="id">Identifier value</param>
    /// <returns>Consumer instance</returns>
    public static Consumer Group(int id)
    {
        return new Consumer
        {
            ConsumerId = Identifier.Numeric(id),
            Type = ConsumerType.ConsumerGroup
        };
    }

    /// <summary>
    ///     Creates a new consumer group identifier.
    /// </summary>
    /// <param name="id">Identifier value</param>
    /// <returns>>Consumer instance</returns>
    public static Consumer Group(string id)
    {
        return new Consumer
        {
            ConsumerId = Identifier.String(id),
            Type = ConsumerType.ConsumerGroup
        };
    }
}
