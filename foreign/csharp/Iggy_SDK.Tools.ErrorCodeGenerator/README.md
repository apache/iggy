# Iggy Error Code Generator

A .NET tool that generates C# `IggyErrorCode` enum and extension methods from the Rust `iggy_error.rs` source file.

## Purpose

This tool ensures the C# SDK error codes stay synchronized with the Rust server implementation. It parses the Rust error
enum and generates:

- `IggyErrorCode.cs` - Enum with all error codes
- `IggyErrorCodeExtensions.cs` - Helper methods to categorize errors (e.g., `IsNotFound()`, `IsAlreadyExists()`)

## Usage

### Running from source

```bash
cd foreign/csharp

# Generate error code files (uses default paths)
dotnet run --project Iggy_SDK.Tools.ErrorCodeGenerator -- generate

# Preview without writing files
dotnet run --project Iggy_SDK.Tools.ErrorCodeGenerator -- generate --dry-run

# Validate C# enum matches Rust source
dotnet run --project Iggy_SDK.Tools.ErrorCodeGenerator -- validate

# With custom paths
dotnet run --project Iggy_SDK.Tools.ErrorCodeGenerator -- generate \
  --rust-file ../../core/common/src/error/iggy_error.rs \
  --output Iggy_SDK/Enums/
```

### Installing as global tool

```bash
cd foreign/csharp/Iggy_SDK.Tools.ErrorCodeGenerator

# Pack the tool
dotnet pack -c Release

# Install globally
dotnet tool install --global --add-source ./bin/Release Apache.Iggy.Tools.ErrorCodeGenerator

# Now you can run from the foreign/csharp directory
iggy-error-codegen generate
iggy-error-codegen validate

# Or with custom paths from anywhere
iggy-error-codegen generate -r <rust-file> -o <output-dir>
iggy-error-codegen validate -r <rust-file> -c <csharp-dir>
```

## Commands

### `generate`

Generates C# error code files from Rust source.

| Option           | Alias | Default                                     | Description                             |
|------------------|-------|---------------------------------------------|-----------------------------------------|
| `--rust-file`    | `-r`  | `../../core/common/src/error/iggy_error.rs` | Path to the Rust `iggy_error.rs` file   |
| `--output`       | `-o`  | `Iggy_SDK/Enums/`                           | Output directory for generated C# files |
| `--license-file` | `-l`  | `../../ASF_LICENSE.txt`                     | Path to license file                    |
| `--dry-run`      | `-d`  | `false`                                     | Preview generated code without writing  |

### `validate`

Validates that the C# enum matches the Rust source.

| Option         | Alias | Default                                     | Description                             |
|----------------|-------|---------------------------------------------|-----------------------------------------|
| `--rust-file`  | `-r`  | `../../core/common/src/error/iggy_error.rs` | Path to the Rust `iggy_error.rs` file   |
| `--csharp-dir` | `-c`  | `Iggy_SDK/Enums/`                           | Directory containing `IggyErrorCode.cs` |
