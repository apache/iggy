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
///     One page read of a list, such as <see cref="IIggyPartition.GetPartitionsPageAsync" />. To read the next page,
///     send the same query with the <see cref="Page{T}.Next" /> of the page: <c>query with { Cursor = page.Next }</c>.
///     A list with a name filter takes a <see cref="NamedPageQuery" />.
/// </summary>
public sealed record PageQuery
{
    /// <summary>Items per page when the caller does not say.</summary>
    public const uint DEFAULT_LIMIT = 100;

    /// <summary>Most items one page returns.</summary>
    public const uint MAX_LIMIT = 1000;

    private const string ORDER_KEY = "order";
    private const string DESCENDING_KEY = "descending";
    private const string LIMIT_KEY = "limit";
    private const string CURSOR_KEY = "cursor";

    /// <summary>The order of the read.</summary>
    public PageOrder Order { get; init; } = PageOrder.Id;

    /// <summary>Whether the read starts at the end of the order.</summary>
    public bool Descending { get; init; }

    /// <summary>Items per page, 1 to 1000.</summary>
    public uint Limit { get; init; } = DEFAULT_LIMIT;

    /// <summary>The next cursor of the previous page, null for the first page.</summary>
    public PageCursor? Cursor { get; init; }

    /// <summary>
    ///     Applies the page rules of the server, so a refused query is never sent: a known order and a limit of 1 to
    ///     1000. A <see cref="PageCursor" /> holds 1 to 255 bytes by construction.
    /// </summary>
    /// <exception cref="IggyInvalidStatusCodeException">Invalid command (code 3) naming the broken rule.</exception>
    internal void Validate()
    {
        if (!Enum.IsDefined(Order))
        {
            throw Refused($"The order {(byte)Order} is unknown.");
        }

        if (Limit is 0 or > MAX_LIMIT)
        {
            throw Refused($"The limit must be 1 to {MAX_LIMIT}, got {Limit}.");
        }
    }

    /// <summary>
    ///     The query string of an HTTP list route. It always holds the limit, so the server answers one page and not
    ///     the whole list. The server decodes it as a form, so every value is escaped.
    /// </summary>
    internal string ToHttpQuery()
    {
        var pairs = new List<string>
        {
            Pair(ORDER_KEY, SnakeCaseNames<PageOrder>.Name(Order)),
            Pair(DESCENDING_KEY, Descending ? "true" : "false"),
            Pair(LIMIT_KEY, Limit.ToString(CultureInfo.InvariantCulture))
        };
        if (Cursor is not null)
        {
            pairs.Add(Pair(CURSOR_KEY, Cursor.ToString()));
        }

        return string.Join('&', pairs);
    }

    internal static string Pair(string key, string value)
    {
        return $"{key}={Uri.EscapeDataString(value)}";
    }

    internal static IggyInvalidStatusCodeException Refused(string message)
    {
        return VsrError.Exception(VsrError.INVALID_COMMAND, $"List page query refused: {message}");
    }
}
