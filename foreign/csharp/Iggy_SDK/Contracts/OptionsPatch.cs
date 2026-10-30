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

using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Headers;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Contracts;

/// <summary>
///     A validated update: the keys to set and the keys to reset to their defaults, the reset keys in ascending
///     ordinal (key byte) order. Every key not named keeps its stored value.
/// </summary>
internal sealed record OptionsPatch(
    OptionsScope Scope,
    IReadOnlyDictionary<string, HeaderValue> Set,
    IReadOnlyList<string> Reset)
{
    private const string RESET_FIELD = "reset";

    /// <summary>
    ///     The REST update body: one flat object of the keys to set, plus a <c>reset</c> array when there is one.
    /// </summary>
    internal Dictionary<string, object> ToHttpBody()
    {
        if (Set.ContainsKey(RESET_FIELD))
        {
            throw VsrError.Exception(VsrError.UNSUPPORTED_OPTION_KEY, $"Unsupported option key {RESET_FIELD}.");
        }

        var body = new Dictionary<string, object>(OptionInputs.From(Set));
        if (Reset.Count > 0)
        {
            body[RESET_FIELD] = Reset.Order(StringComparer.Ordinal).ToArray();
        }

        return body;
    }
}
