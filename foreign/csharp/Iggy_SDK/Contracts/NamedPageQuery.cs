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

using System.Text;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Utils;

namespace Apache.Iggy.Contracts;

/// <summary>
///     One page read of a list with a name filter, such as <see cref="IIggyStream.GetStreamsPageAsync" />. To read the
///     next page, send the same query with the <see cref="Page{T}.Next" /> of the page:
///     <c>query with { Page = query.Page with { Cursor = page.Next } }</c>.
/// </summary>
public sealed record NamedPageQuery
{
    /// <summary>Most bytes of a name filter in UTF-8.</summary>
    public const int MAX_NAME_SIZE = WireName.MAX_LENGTH;

    private const string NAME_KEY = "name";

    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    /// <summary>The order, the page size and the cursor of the read.</summary>
    public PageQuery Page { get; init; } = new();

    /// <summary>Keeps only the entries whose name holds this text, case-sensitive. Null keeps every entry.</summary>
    public string? Name { get; init; }

    /// <summary>
    ///     Applies the rules of <see cref="PageQuery.Validate" /> and a name of 1 to 255 bytes of UTF-8, so a refused
    ///     query is never sent.
    /// </summary>
    /// <exception cref="IggyInvalidStatusCodeException">Invalid command (code 3) naming the broken rule.</exception>
    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(Page);
        Page.Validate();
        if (Name is null)
        {
            return;
        }

        int size;
        try
        {
            size = StrictUtf8.GetByteCount(Name);
        }
        catch (EncoderFallbackException)
        {
            // A lone surrogate has no UTF-8 form, so the server would read another name.
            throw PageQuery.Refused("The name must be valid Unicode text.");
        }

        if (size is 0 or > MAX_NAME_SIZE)
        {
            throw PageQuery.Refused($"The name must be 1 to {MAX_NAME_SIZE} bytes of UTF-8, got {size}.");
        }
    }

    /// <summary>The query string of <see cref="PageQuery.ToHttpQuery" />, with the name when there is one.</summary>
    internal string ToHttpQuery()
    {
        var query = Page.ToHttpQuery();
        return Name is null ? query : $"{query}&{PageQuery.Pair(NAME_KEY, Name)}";
    }
}
