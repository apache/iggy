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

using System.CommandLine;
using Apache.Iggy.Tools.ErrorCodeGenerator;

var rootCommand = new RootCommand("Generates C# IggyErrorCode enum from Rust iggy_error.rs file");

// Generate command
var generateCommand = new Command("generate", "Generate C# error code files from Rust source");

var rustFileOption = new Option<FileInfo>("--rust-file", "-r")
{
    Description = "Path to the Rust iggy_error.rs file (default: ../../core/common/src/error/iggy_error.rs)",
    DefaultValueFactory = _ => new FileInfo("../../core/common/src/error/iggy_error.rs")
};

var outputDirOption = new Option<DirectoryInfo>("--output", "-o")
{
    Description = "Output directory for generated C# files (default: Iggy_SDK/Enums/)",
    DefaultValueFactory = _ => new DirectoryInfo("Iggy_SDK/Enums/")
};

var dryRunOption = new Option<bool>("--dry-run", "-d") { Description = "Preview generated code without writing files" };

var licenseFileOption = new Option<FileInfo>("--license-file", "-l")
{
    Description = "Path to the license file (default: ../../ASF_LICENSE.txt)",
    DefaultValueFactory = _ => new FileInfo("../../ASF_LICENSE.txt")
};

generateCommand.Add(rustFileOption);
generateCommand.Add(outputDirOption);
generateCommand.Add(dryRunOption);
generateCommand.Add(licenseFileOption);

generateCommand.SetAction(async (parseResult, cancellationToken) =>
{
    var rustFile = parseResult.GetValue(rustFileOption)!;
    var outputDir = parseResult.GetValue(outputDirOption)!;
    var dryRun = parseResult.GetValue(dryRunOption);
    var licenseFile = parseResult.GetValue(licenseFileOption)!;

    if (!rustFile.Exists)
    {
        Console.Error.WriteLine($"Error: Rust file not found: {rustFile.FullName}");
        return 1;
    }

    if (!licenseFile.Exists)
    {
        Console.Error.WriteLine($"Error: License file not found: {licenseFile.FullName}");
        return 1;
    }

    Console.WriteLine($"Using license file: {licenseFile.FullName}");
    var licenseContent = await File.ReadAllTextAsync(licenseFile.FullName, cancellationToken);
    var license = CSharpCodeGenerator.ConvertLicenseToComments(licenseContent);

    Console.WriteLine($"Parsing Rust file: {rustFile.FullName}");
    List<RustErrorCode> errors = await RustErrorParser.ParseFileAsync(rustFile.FullName);
    Console.WriteLine($"Found {errors.Count} error codes");

    var enumCode = CSharpCodeGenerator.GenerateEnum(errors, license);
    var extensionsCode = CSharpCodeGenerator.GenerateExtensions(errors, license);

    if (dryRun)
    {
        Console.WriteLine("\n=== IggyErrorCode.cs ===\n");
        Console.WriteLine(enumCode);
        Console.WriteLine("\n=== IggyErrorCodeExtensions.cs ===\n");
        Console.WriteLine(extensionsCode);
    }
    else
    {
        if (!outputDir.Exists)
        {
            outputDir.Create();
        }

        var enumPath = Path.Combine(outputDir.FullName, "IggyErrorCode.cs");
        var extensionsPath = Path.Combine(outputDir.FullName, "IggyErrorCodeExtensions.cs");

        await File.WriteAllTextAsync(enumPath, enumCode, cancellationToken);
        Console.WriteLine($"Generated: {enumPath}");

        await File.WriteAllTextAsync(extensionsPath, extensionsCode, cancellationToken);
        Console.WriteLine($"Generated: {extensionsPath}");

        Console.WriteLine("\nDone!");
    }

    return 0;
});

// Validate command
var validateCommand = new Command("validate", "Validate that C# enum matches Rust source");

var validateRustFileOption = new Option<FileInfo>("--rust-file", "-r")
{
    Description = "Path to the Rust iggy_error.rs file (default: ../../core/common/src/error/iggy_error.rs)",
    DefaultValueFactory = _ => new FileInfo("../../core/common/src/error/iggy_error.rs")
};

var csharpDirOption = new Option<DirectoryInfo>("--csharp-dir", "-c")
{
    Description = "Directory containing IggyErrorCode.cs (default: Iggy_SDK/Enums/)",
    DefaultValueFactory = _ => new DirectoryInfo("Iggy_SDK/Enums/")
};

validateCommand.Add(validateRustFileOption);
validateCommand.Add(csharpDirOption);

validateCommand.SetAction(async (parseResult, cancellationToken) =>
{
    var rustFile = parseResult.GetValue(validateRustFileOption)!;
    var csharpDir = parseResult.GetValue(csharpDirOption)!;

    if (!rustFile.Exists)
    {
        Console.Error.WriteLine($"Error: Rust file not found: {rustFile.FullName}");
        return 1;
    }

    var csharpFile = new FileInfo(Path.Combine(csharpDir.FullName, "IggyErrorCode.cs"));
    if (!csharpFile.Exists)
    {
        Console.Error.WriteLine($"Error: C# file not found: {csharpFile.FullName}");
        return 1;
    }

    Console.WriteLine($"Parsing Rust file: {rustFile.FullName}");
    List<RustErrorCode> rustErrors = await RustErrorParser.ParseFileAsync(rustFile.FullName);
    Console.WriteLine($"Found {rustErrors.Count} error codes in Rust");

    Console.WriteLine($"Parsing C# file: {csharpFile.FullName}");
    List<CSharpErrorCode> csharpErrors = await CSharpEnumParser.ParseFileAsync(csharpFile.FullName);
    Console.WriteLine($"Found {csharpErrors.Count} error codes in C#");

    var hasErrors = false;

    // Check for missing in C#
    List<RustErrorCode> missingInCSharp = rustErrors
        .Where(r => !csharpErrors.Any(c => c.Name == r.Name && c.Code == r.Code))
        .ToList();

    if (missingInCSharp.Count > 0)
    {
        hasErrors = true;
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"\nMissing in C# ({missingInCSharp.Count}):");
        Console.ResetColor();
        foreach (var error in missingInCSharp)
        {
            Console.WriteLine($"  - {error.Name} = {error.Code}");
        }
    }

    // Check for extra in C#
    List<CSharpErrorCode> extraInCSharp = csharpErrors
        .Where(c => !rustErrors.Any(r => r.Name == c.Name && r.Code == c.Code))
        .ToList();

    if (extraInCSharp.Count > 0)
    {
        hasErrors = true;
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine($"\nExtra in C# (not in Rust) ({extraInCSharp.Count}):");
        Console.ResetColor();
        foreach (var error in extraInCSharp)
        {
            Console.WriteLine($"  - {error.Name} = {error.Code}");
        }
    }

    // Check for code mismatches
    List<(RustErrorCode Rust, CSharpErrorCode CSharp)> codeMismatches = rustErrors
        .SelectMany(r => csharpErrors
            .Where(c => c.Name == r.Name && c.Code != r.Code)
            .Select(c => (Rust: r, CSharp: c)))
        .ToList();

    if (codeMismatches.Count > 0)
    {
        hasErrors = true;
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"\nCode mismatches ({codeMismatches.Count}):");
        Console.ResetColor();
        foreach (var (rust, csharp) in codeMismatches)
        {
            Console.WriteLine($"  - {rust.Name}: Rust={rust.Code}, C#={csharp.Code}");
        }
    }

    if (hasErrors)
    {
        Console.WriteLine("\nValidation FAILED");
        return 1;
    }

    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine("\nValidation PASSED - C# enum matches Rust source");
    Console.ResetColor();
    return 0;
});

rootCommand.Add(generateCommand);
rootCommand.Add(validateCommand);

return await rootCommand.Parse(args).InvokeAsync();
