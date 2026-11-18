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

namespace Apache.Iggy.Contracts;

/// <summary>
///     One page of a list.
/// </summary>
/// <remarks>
///     A page can be short or empty and still have a <see cref="Next" />. A read of a whole list ends at the first
///     page without one.
/// </remarks>
/// <typeparam name="T">The item type of the list.</typeparam>
public sealed class Page<T>
{
    /// <summary>The items of the page, in the order of the query.</summary>
    public required IReadOnlyList<T> Items { get; init; }

    /// <summary>The cursor of the next page, null on the last page.</summary>
    public PageCursor? Next { get; init; }
}
