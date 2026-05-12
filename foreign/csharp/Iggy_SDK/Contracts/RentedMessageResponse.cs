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
using Apache.Iggy.Mappers;
using Apache.Iggy.Messages;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Response containing rented payload and raw header memory.
///     The payload and raw headers are only valid while the owning <see cref="PolledMessagesRental" /> is alive.
/// </summary>
public sealed class RentedMessageResponse
{
    private Dictionary<HeaderKey, HeaderValue>? _userHeaders;
    private bool _userHeadersInitialized;

    /// <summary>
    ///     Message header.
    /// </summary>
    public required MessageHeader Header { get; init; }

    /// <summary>
    ///     Message payload backed by rented memory.
    /// </summary>
    public required ReadOnlyMemory<byte> Payload { get; init; }

    /// <summary>
    ///     Raw user header bytes backed by rented memory.
    /// </summary>
    public ReadOnlyMemory<byte> RawUserHeaders { get; init; }

    /// <summary>
    ///     Parsed user headers. Parsed lazily and cached on first access.
    /// </summary>
    public Dictionary<HeaderKey, HeaderValue>? UserHeaders
    {
        get
        {
            if (!_userHeadersInitialized)
            {
                _userHeaders = RawUserHeaders.IsEmpty
                    ? null
                    : BinaryMapper.TryMapHeaders(RawUserHeaders.Span);
                _userHeadersInitialized = true;
            }

            return _userHeaders;
        }
        init
        {
            _userHeaders = value;
            _userHeadersInitialized = true;
        }
    }
}
