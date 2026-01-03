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
///     Represents a parsed error code from C#.
/// </summary>
public sealed record CSharpErrorCode(string Name, int Code);

/// <summary>
///     Parses C# IggyErrorCode enum.
/// </summary>
public static partial class CSharpEnumParser
{
    // Matches: EnumValue = 123, or EnumValue = 123
    [GeneratedRegex(@"^\s*([A-Z][a-zA-Z0-9_]*)\s*=\s*(\d+)\s*,?\s*$")]
    private static partial Regex EnumValueRegex();

    /// <summary>
    ///     Parses the C# IggyErrorCode.cs file and extracts error codes.
    /// </summary>
    public static List<CSharpErrorCode> Parse(string csharpFileContent)
    {
        var errors = new List<CSharpErrorCode>();
        var lines = csharpFileContent.Split('\n');

        var inEnum = false;

        foreach (var line in lines)
        {
            // Detect start of enum
            if (line.Contains("public enum IggyErrorCode"))
            {
                inEnum = true;
                continue;
            }

            // Detect end of enum
            if (inEnum && line.Trim() == "}")
            {
                break;
            }

            if (!inEnum)
            {
                continue;
            }

            // Check for enum value
            var match = EnumValueRegex().Match(line);
            if (match.Success)
            {
                var name = match.Groups[1].Value;
                var code = int.Parse(match.Groups[2].Value);
                errors.Add(new CSharpErrorCode(name, code));
            }
        }

        return errors;
    }

    /// <summary>
    ///     Parses the C# file from the given path.
    /// </summary>
    public static async Task<List<CSharpErrorCode>> ParseFileAsync(string filePath)
    {
        var content = await File.ReadAllTextAsync(filePath);
        return Parse(content);
    }
}
