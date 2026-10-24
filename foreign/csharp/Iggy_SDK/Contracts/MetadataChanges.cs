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

using System.Text.Json.Serialization;
using Apache.Iggy.Enums;
using Apache.Iggy.JsonConverters;

namespace Apache.Iggy.Contracts;

/// <summary>
///     One page of the metadata change log, or of the current state.
/// </summary>
/// <remarks>
///     JSON support is read only. <see cref="System.Text.Json.JsonSerializer" /> deserializes the HTTP answer, and
///     serializing a page throws <see cref="NotSupportedException" />.
/// </remarks>
[JsonConverter(typeof(MetadataChangesConverter))]
public sealed class MetadataChanges
{
    /// <summary>Outcome of the query. Only <see cref="MetadataChangeStatus.Ok" /> carries records or items.</summary>
    public required MetadataChangeStatus Status { get; init; }

    /// <summary>Whether the page holds change records or current state items.</summary>
    public required MetadataChangePhase Phase { get; init; }

    /// <summary>The answering node's log id, null before it applied op 1.</summary>
    public UInt128? LogId { get; init; }

    /// <summary>Lowest whole operation from which the requested groups are complete.</summary>
    public required ulong OldestOp { get; init; }

    /// <summary>
    ///     The requested groups are complete for every timestamp above this value, in microseconds, 0 when nothing
    ///     was evicted.
    /// </summary>
    public required ulong RetainedSince { get; init; }

    /// <summary>Highest metadata operation the answering node applied.</summary>
    public required ulong HeadOp { get; init; }

    /// <summary>
    ///     The cursor to send back. Present on <see cref="MetadataChangeStatus.Ok" />, echoed on
    ///     <see cref="MetadataChangeStatus.Behind" />, null on expired and reset. On a current state page it is the
    ///     anchor to follow the changes from once the scan is complete.
    /// </summary>
    public MetadataChangeCursor? Next { get; init; }

    /// <summary>More entries remain after <see cref="Next" />, or another state page exists.</summary>
    public required bool HasMore { get; init; }

    /// <summary>Change records in <c>(op, ordinal)</c> order. Empty on a current state page.</summary>
    public required IReadOnlyList<MetadataChangeRecord> Records { get; init; }

    /// <summary>Current state items. Empty on a change page.</summary>
    public required IReadOnlyList<MetadataCurrentItem> Items { get; init; }

    /// <summary>Continuation of a current state scan, null once it is complete.</summary>
    public MetadataCurrentPageToken? CurrentPageToken { get; init; }
}

/// <summary>
///     One change record: the committed operation that produced it, its position within that operation, and what
///     is true after it.
/// </summary>
public sealed class MetadataChangeRecord
{
    /// <summary>The metadata operation that produced the record.</summary>
    public required ulong Op { get; init; }

    /// <summary>Position of the record within <see cref="Op" />.</summary>
    public required uint Ordinal { get; init; }

    /// <summary>Prepare checksum of <see cref="Op" />.</summary>
    public required UInt128 Checksum { get; init; }

    /// <summary>Prepare timestamp of <see cref="Op" />, in microseconds since the Unix epoch.</summary>
    public required ulong Timestamp { get; init; }

    /// <summary>The acting user, null for server actions.</summary>
    public uint? ActorUserId { get; init; }

    /// <summary>The kind, null for a record this client does not know.</summary>
    public MetadataChangeKind? Kind { get; init; }

    /// <summary>The group, null for a record this client does not know.</summary>
    public MetadataChangeGroup? Group => Kind is { } kind ? MetadataChangeGroups.Of(kind) : null;

    /// <summary>
    ///     The typed body, or <see cref="MetadataUnknownBody" /> and <see cref="MetadataUnknownJsonBody" /> for a
    ///     record this client does not know. A mirror that cannot apply a record stops without advancing its cursor.
    /// </summary>
    public required MetadataChangeBody Body { get; init; }
}

/// <summary>
///     One current state item.
/// </summary>
public sealed class MetadataCurrentItem
{
    /// <summary>The group, null for an item this client does not know.</summary>
    public MetadataChangeGroup? Group { get; init; }

    /// <summary>
    ///     The typed body, or <see cref="MetadataUnknownBody" /> and <see cref="MetadataUnknownJsonBody" /> for an
    ///     item this client does not know.
    /// </summary>
    public required MetadataChangeBody Body { get; init; }
}
