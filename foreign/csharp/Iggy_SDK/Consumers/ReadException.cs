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

using Apache.Iggy.Exceptions;
using Apache.Iggy.Kinds;

namespace Apache.Iggy.Consumers;

/// <summary>
///     An error of a read of a <see cref="PartitionReader" />. The reader stays usable after it and retries by itself.
///     <see cref="Exception.InnerException" /> is the error itself.
/// </summary>
public sealed class ReadException(uint? partitionId, Exception error, PollingStrategy? skip = null)
    : Exception(partitionId is { } partition
        ? $"Reading partition {partition} failed: {error.Message}"
        : $"Reading failed: {error.Message}", error)
{
    /// <summary>The partition of the error, or null for an error of the whole reader.</summary>
    public uint? PartitionId { get; } = partitionId;

    /// <summary>
    ///     For a message that does not decode: the continuation after it. The reader reads the message again until a
    ///     <see cref="PartitionReader.SeekAsync" /> to this continuation skips it. The messages before it arrive in a
    ///     batch before this error.
    /// </summary>
    public PollingStrategy? Skip { get; } = skip;

    /// <summary>
    ///     For a message that does not decode: its offset, known when its frame decoded and a later step failed, such
    ///     as its decryption. Null when the framing of the reply is malformed, so no message can be told apart.
    /// </summary>
    public ulong? Offset { get; } = (error as MessageDecryptionException)?.Offset;

    /// <summary>
    ///     For a message that does not decode: the record as the reply carried it, its 48-byte frame header, payload
    ///     and user headers, before any decryption. For malformed framing, the remaining bytes of the reply records
    ///     from where decoding stopped. Null for an error that is not a decode failure, and for a message of a JSON
    ///     reply over HTTP, which carries no records.
    /// </summary>
    public byte[]? Record { get; } = error switch
    {
        MessageDecryptionException decryption => decryption.Record,
        MalformedResponseException malformed => malformed.Record,
        _ => null
    };
}
