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

using System.Text.RegularExpressions;

namespace Apache.Iggy.Tools.ErrorCodeGenerator;

/// <summary>
///     Represents a parsed error code from Rust.
/// </summary>
public sealed record RustErrorCode(string Name, int Code, string Message);

/// <summary>
///     Parses Rust IggyError enum from iggy_error.rs file.
/// </summary>
public static partial class RustErrorParser
{
    // Combined pattern that matches #[error("...")] followed by VariantName... = code
    // Handles multiline definitions for both error attributes and struct variants.
    // - \s* matches whitespace including newlines
    // - [^"]+ captures the error message
    // - [^)]* and [^}]* handle tuple/struct fields spanning multiple lines
    [GeneratedRegex(
        @"#\[error\(\s*""([^""]+)""\s*\)\]\s*([A-Z][a-zA-Z0-9_]*)(?:\s*\([^)]*\)|\s*\{[^}]*\})?\s*=\s*(\d+)")]
    private static partial Regex ErrorVariantRegex();

    /// <summary>
    ///     Parses the Rust iggy_error.rs file and extracts error codes.
    /// </summary>
    public static List<RustErrorCode> Parse(string rustFileContent)
    {
        var errors = new List<RustErrorCode>();
        var matches = ErrorVariantRegex().Matches(rustFileContent);

        foreach (Match match in matches)
        {
            var message = match.Groups[1].Value;
            var name = match.Groups[2].Value;
            var code = int.Parse(match.Groups[3].Value);

            errors.Add(new RustErrorCode(name, code, message));
        }

        return errors;
    }

    /// <summary>
    ///     Parses the Rust file from the given path.
    /// </summary>
    public static async Task<List<RustErrorCode>> ParseFileAsync(string filePath)
    {
        var content = await File.ReadAllTextAsync(filePath);
        return Parse(content);
    }
}
