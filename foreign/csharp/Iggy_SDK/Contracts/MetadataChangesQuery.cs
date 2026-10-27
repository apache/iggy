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

using System.Globalization;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Where a metadata changes query starts.
/// </summary>
public sealed record MetadataChangeStart
{
    private MetadataChangeStart(MetadataChangeStartMode mode, MetadataChangeCursor? cursor = null,
        MetadataCurrentPageToken? pageToken = null)
    {
        Mode = mode;
        Cursor = cursor;
        PageToken = pageToken;
    }

    /// <summary>The oldest operation the requested groups still hold completely.</summary>
    public static MetadataChangeStart Earliest { get; } = new(MetadataChangeStartMode.Earliest);

    /// <summary>No records, only a cursor at the newest retained record of the requested groups.</summary>
    public static MetadataChangeStart Latest { get; } = new(MetadataChangeStartMode.Latest);

    /// <summary>The start mode.</summary>
    public MetadataChangeStartMode Mode { get; }

    /// <summary>The cursor the records follow, set only for <see cref="MetadataChangeStartMode.After" />.</summary>
    public MetadataChangeCursor? Cursor { get; }

    /// <summary>The page token of a continued current state scan, null on its first page.</summary>
    public MetadataCurrentPageToken? PageToken { get; }

    /// <summary>Records after <paramref name="cursor" />.</summary>
    public static MetadataChangeStart After(MetadataChangeCursor cursor)
    {
        return new MetadataChangeStart(MetadataChangeStartMode.After, cursor);
    }

    /// <summary>
    ///     Current state pages: without a token the scan starts, with the token a page returned it continues.
    /// </summary>
    public static MetadataChangeStart Current(MetadataCurrentPageToken? pageToken = null)
    {
        return new MetadataChangeStart(MetadataChangeStartMode.Current, pageToken: pageToken);
    }
}

/// <summary>
///     A query of <see cref="IIggySystem.GetMetadataChangesAsync" />.
/// </summary>
/// <remarks>
///     <see cref="Since" /> and <see cref="Until" /> keep records whose prepare timestamp is in the inclusive range
///     and apply only to <see cref="MetadataChangeStart.Earliest" /> and <see cref="MetadataChangeStart.After" />.
/// </remarks>
public sealed record MetadataChangesQuery
{
    /// <summary>Records or items per page when the caller does not say.</summary>
    public const uint DEFAULT_LIMIT = 100;

    /// <summary>Most records or items one page returns.</summary>
    public const uint MAX_LIMIT = 1000;

    /// <summary>Where the query starts.</summary>
    public MetadataChangeStart Start { get; init; } = MetadataChangeStart.Earliest;

    /// <summary>
    ///     The groups to read. Empty means every group, duplicates are ignored. A current state scan must include
    ///     the parent groups of every requested group, for example <c>Topics</c> needs <c>Streams</c>.
    /// </summary>
    public IReadOnlyList<MetadataChangeGroup> Groups { get; init; } = [];

    /// <summary>Lowest prepare timestamp to keep, in microseconds since the Unix epoch.</summary>
    public ulong? Since { get; init; }

    /// <summary>Highest prepare timestamp to keep, in microseconds since the Unix epoch.</summary>
    public ulong? Until { get; init; }

    /// <summary>Records or items per page, 1 to 1000.</summary>
    public uint Limit { get; init; } = DEFAULT_LIMIT;

    /// <summary>
    ///     Applies the rules every transport and the server share, so a refused query is never sent.
    /// </summary>
    /// <exception cref="IggyInvalidStatusCodeException">Invalid command (code 3) naming the broken rule.</exception>
    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(Start);
        ArgumentNullException.ThrowIfNull(Groups);
        if (Limit is 0 or > MAX_LIMIT)
        {
            throw Refused($"The limit must be 1 to {MAX_LIMIT}, got {Limit}.");
        }

        if (Since > Until)
        {
            throw Refused("Since must not be above until.");
        }

        if (Groups.Any(group => !MetadataChangeGroups.IsDefined(group)))
        {
            throw Refused("The groups name a group this client does not know.");
        }

        var mask = MetadataChangeGroups.Mask(Groups);
        var hasTimeBound = Since.HasValue || Until.HasValue;
        switch (Start.Mode)
        {
            case MetadataChangeStartMode.After when Start.Cursor is not { IsCanonical: true }:
                throw Refused("A cursor at op 0 carries no checksum and no ordinal.");
            case MetadataChangeStartMode.Latest or MetadataChangeStartMode.Current when hasTimeBound:
                throw Refused("Time bounds apply only when starting at the earliest record or after a cursor.");
            case MetadataChangeStartMode.Current when MetadataChangeGroups.MissesParent(mask):
                throw Refused("A current state scan must include the parent groups of every requested group.");
            case MetadataChangeStartMode.Current when Start.PageToken is { } token && token.GroupsMask != mask:
                throw Refused("The page token was issued for other groups.");
        }
    }

    /// <summary>The query string of <c>GET /metadata/changes</c>, in the order the server documents.</summary>
    internal string ToHttpQuery()
    {
        var pairs = new List<string>(6);
        switch (Start.Mode)
        {
            case MetadataChangeStartMode.After:
                pairs.Add($"after={Start.Cursor}");
                break;
            case MetadataChangeStartMode.Current:
                pairs.Add("from=current");
                if (Start.PageToken is { } token)
                {
                    pairs.Add($"current_after={token}");
                }

                break;
            default:
                pairs.Add($"from={Start.Mode.ToString().ToLowerInvariant()}");
                break;
        }

        if (Groups.Count > 0)
        {
            var names = MetadataChangeGroups.FromMask(MetadataChangeGroups.Mask(Groups))
                .Select(SnakeCaseNames<MetadataChangeGroup>.Name);
            pairs.Add($"groups={string.Join(',', names)}");
        }

        if (Since is { } since)
        {
            pairs.Add($"since={since.ToString(CultureInfo.InvariantCulture)}");
        }

        if (Until is { } until)
        {
            pairs.Add($"until={until.ToString(CultureInfo.InvariantCulture)}");
        }

        pairs.Add($"limit={Limit.ToString(CultureInfo.InvariantCulture)}");
        return string.Join('&', pairs);
    }

    private static IggyInvalidStatusCodeException Refused(string message)
    {
        return VsrError.Exception(VsrError.INVALID_COMMAND, $"Metadata changes query refused: {message}");
    }
}
