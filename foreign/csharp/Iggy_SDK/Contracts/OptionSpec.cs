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

using Apache.Iggy.Headers;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Resource whose option catalog the server describes.
/// </summary>
public enum OptionsScope
{
    /// <summary>
    ///     Keys a CreateTopic accepts.
    /// </summary>
    Topic = 1,

    /// <summary>
    ///     Keys a CreateStream accepts; none yet.
    /// </summary>
    Stream = 2,

    /// <summary>
    ///     Keys a CreateUser accepts; none yet.
    /// </summary>
    User = 3
}

/// <summary>
///     One entry of a resource's option catalog, as served by DescribeOptions.
/// </summary>
public class OptionSpec
{
    /// <summary>
    ///     Option key the create command accepts.
    /// </summary>
    public required string Key { get; set; }

    /// <summary>
    ///     Canonical kind for this key: what the server encodes its default under, and
    ///     what a value set at create is stored as whatever kind the client sent, since
    ///     create admission re-encodes the block from its own parse. An update stores
    ///     the client's bytes verbatim and is the exception.
    /// </summary>
    public required HeaderKind Kind { get; set; }

    /// <summary>
    ///     The key's default in <see cref="Kind" />'s encoding; empty when the key
    ///     has no default.
    /// </summary>
    public required byte[] DefaultValue { get; set; } = [];

    /// <summary>
    ///     What the option does, including any bounds it is checked against.
    /// </summary>
    public required string Description { get; set; }
}
