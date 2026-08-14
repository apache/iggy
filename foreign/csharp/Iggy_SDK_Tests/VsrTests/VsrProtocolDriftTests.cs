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

using System.Buffers.Binary;
using System.Globalization;
using System.Reflection;
using System.Text.RegularExpressions;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.VsrTests;

/// <summary>
///     Asserts the .NET VSR constants still match the Rust wire definitions they were ported from. The unit
///     tests above pin the .NET side against itself; this one pins it against
///     <c>core/binary_protocol/</c>, so a Rust-side change fails here instead of in production framing.
///     Skipped when the Rust sources are not on disk (packaged SDK builds).
/// </summary>
public sealed class VsrProtocolDriftTests
{
    private const string HeaderPath = "core/binary_protocol/src/consensus/header.rs";
    private const string CommandPath = "core/binary_protocol/src/consensus/command.rs";
    private const string OperationPath = "core/binary_protocol/src/consensus/operation.rs";
    private const string CodesPath = "core/binary_protocol/src/codes.rs";
    private const string DispatchPath = "core/binary_protocol/src/dispatch.rs";
    private const string ErrorPath = "core/common/src/error/iggy_error.rs";
    private const string ManifestPath = "core/binary_protocol/Cargo.toml";
    private const string EvictionGraderPath = "core/common/src/error/eviction.rs";
    private const string ReplyResultPath = "core/binary_protocol/src/consensus/reply_result.rs";
    private const string CredentialDefaultsPath = "core/common/src/http/users/defaults.rs";

    private const string LoginRegisterResponsePath =
        "core/binary_protocol/src/responses/users/login_register.rs";

    private const string SyncConsumerGroupResponsePath =
        "core/binary_protocol/src/responses/consumer_groups/sync_consumer_group.rs";

    /// <summary>
    ///     Codes the SDK deliberately does not resolve the way the Rust dispatch table declares them, with the
    ///     reason each one is exempt. Anything not listed here must agree with the table.
    /// </summary>
    private static readonly Dictionary<string, VsrOperation?> CodeMappingExceptions = new()
    {
        // Non-replicated in the table because the legacy transport routes them; under VSR they are the
        // consensus handshake itself and carry their own operations.
        ["LOGIN_REGISTER_CODE"] = VsrOperation.Register,
        ["LOGIN_REGISTER_WITH_PAT_CODE"] = VsrOperation.Register,
        ["LOGOUT_USER_CODE"] = VsrOperation.Logout,

        // VSR has no legacy login. ForCode throws rather than encoding a request that would look like a
        // working login while binding no session; null means "asserted to throw".
        ["LOGIN_USER_CODE"] = null,
        ["LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE"] = null
    };

    [Fact]
    public void RequestHeaderOffsets_MatchTheRustLayout()
    {
        IReadOnlyDictionary<string, int> offsets = RustStruct.Offsets(ReadRustSource(HeaderPath), "RequestHeader");

        Assert.Equal(VsrHeader.SIZE_OFFSET, offsets["size"]);
        Assert.Equal(VsrHeader.COMMAND_OFFSET, offsets["command"]);
        Assert.Equal(VsrHeader.REQUEST_CLIENT_OFFSET, offsets["client"]);
        Assert.Equal(VsrHeader.REQUEST_TIMESTAMP_OFFSET, offsets["timestamp"]);
        Assert.Equal(VsrHeader.REQUEST_ID_OFFSET, offsets["request"]);
        Assert.Equal(VsrHeader.REQUEST_OPERATION_OFFSET, offsets["operation"]);
        Assert.Equal(VsrHeader.REQUEST_SESSION_OFFSET, offsets["session"]);
        Assert.Equal(VsrHeader.REQUEST_RESERVED_OFFSET, offsets["reserved"]);

        // A routing group reintroduced on the client header would shift every field
        // after it and silently reinstate a derivation this SDK no longer performs.
        Assert.DoesNotContain("namespace", offsets.Keys);
        Assert.DoesNotContain("group", offsets.Keys);
    }

    [Fact]
    public void ReplyHeaderOffsets_MatchTheRustLayout()
    {
        IReadOnlyDictionary<string, int> offsets = RustStruct.Offsets(ReadRustSource(HeaderPath), "ReplyHeader");

        Assert.Equal(VsrHeader.SIZE_OFFSET, offsets["size"]);
        Assert.Equal(VsrHeader.COMMAND_OFFSET, offsets["command"]);
        Assert.Equal(VsrHeader.REPLY_OPERATION_OFFSET, offsets["operation"]);
        Assert.Equal(VsrHeader.REPLY_STATUS_OFFSET, offsets["status"]);

        Assert.DoesNotContain("namespace", offsets.Keys);
        Assert.DoesNotContain("group", offsets.Keys);
    }

    [Fact]
    public void EvictionHeaderOffsets_MatchTheRustLayout()
    {
        IReadOnlyDictionary<string, int> offsets = RustStruct.Offsets(ReadRustSource(HeaderPath), "EvictionHeader");

        Assert.Equal(VsrHeader.COMMAND_OFFSET, offsets["command"]);
        Assert.Equal(VsrHeader.EVICTION_CLIENT_OFFSET, offsets["client"]);
        Assert.Equal(VsrHeader.EVICTION_PROTOCOL_VERSION_OFFSET, offsets["server_protocol_version"]);
        Assert.Equal(VsrHeader.EVICTION_PROTOCOL_VERSION_MIN_OFFSET, offsets["server_protocol_version_min"]);
        Assert.Equal(VsrHeader.EVICTION_REASON_OFFSET, offsets["reason"]);
    }

    [Fact]
    public void HeaderSize_MatchesTheRustConstant()
    {
        var source = ReadRustSource(HeaderPath);
        var declared = Regex.Match(source, @"pub const HEADER_SIZE: usize = (\d+);");

        Assert.True(declared.Success, "HEADER_SIZE is no longer declared in header.rs.");
        Assert.Equal(VsrHeader.HEADER_SIZE, int.Parse(declared.Groups[1].Value, CultureInfo.InvariantCulture));
        Assert.Equal(VsrHeader.HEADER_SIZE, RustStruct.Size(source, "RequestHeader"));
        Assert.Equal(VsrHeader.HEADER_SIZE, RustStruct.Size(source, "ReplyHeader"));
        Assert.Equal(VsrHeader.HEADER_SIZE, RustStruct.Size(source, "EvictionHeader"));
    }

    [Fact]
    public void Command2Discriminants_MatchTheRustEnum()
    {
        IReadOnlyDictionary<string, int> rust = RustEnum.Discriminants(ReadRustSource(CommandPath), "Command2");

        AssertSubsetMatches<Command2>(rust);
    }

    [Fact]
    public void EvictionReasons_MatchTheRustEnumExactly()
    {
        IReadOnlyDictionary<string, int> rust = RustEnum.Discriminants(ReadRustSource(HeaderPath), "EvictionReason");

        AssertExactMatch<EvictionReason>(rust);
    }

    [Fact]
    public void Operations_MatchTheRustEnumExactly()
    {
        IReadOnlyDictionary<string, int> rust = RustEnum.Discriminants(ReadRustSource(OperationPath), "Operation");

        AssertExactMatch<VsrOperation>(rust);
    }

    [Fact]
    public void ProtocolVersion_MatchesTheBinaryProtocolCrateVersion()
    {
        var manifest = ReadRustSource(ManifestPath);
        var declared = Regex.Match(manifest, @"^version\s*=\s*""(\d+)\.(\d+)\.(\d+)", RegexOptions.Multiline);

        Assert.True(declared.Success, "iggy_binary_protocol no longer declares a semver version.");
        Assert.Equal(LoginRegister.PROTOCOL_VERSION_MAJOR, Group(declared, 1));
        Assert.Equal(LoginRegister.PROTOCOL_VERSION_MINOR, Group(declared, 2));
        Assert.Equal(LoginRegister.PROTOCOL_VERSION_PATCH, Group(declared, 3));
    }

    /// <summary>
    ///     Pins <see cref="CommandCodes" /> against <c>codes.rs</c> as a set of values, the way the Node mirror
    ///     does. A name-keyed lookup alone lets a command added upstream pass unnoticed, because the constant it
    ///     would have to match does not exist here yet. Names are still compared where both sides declare them,
    ///     which catches a renumber that keeps the set size intact.
    /// </summary>
    [Fact]
    public void CommandCodes_MatchTheRustCodes()
    {
        var rust = Regex.Matches(ReadRustSource(CodesPath), @"pub const (\w+_CODE): u32 = (\d+);")
            .ToDictionary(code => code.Groups[1].Value, code => Group(code, 2));
        Assert.NotEmpty(rust);

        var dotnet = typeof(CommandCodes)
            .GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static)
            .Where(field => field is { IsLiteral: true, FieldType.Name: nameof(Int32) })
            .ToDictionary(field => field.Name, field => (int)field.GetRawConstantValue()!);

        var mismatches = rust
            .Where(code => !dotnet.ContainsValue(code.Value))
            .Select(code => $"{code.Key} = {code.Value} is declared in Rust and missing from CommandCodes")
            .Concat(dotnet
                .Where(code => !rust.ContainsValue(code.Value))
                .Select(code => $"{code.Key} = {code.Value} is declared in CommandCodes and missing from Rust"))
            .Concat(rust
                .Where(code => dotnet.TryGetValue(code.Key, out var actual) && actual != code.Value)
                .Select(code => $"{code.Key}: rust {code.Value}, .NET {dotnet[code.Key]}"))
            .ToList();

        Assert.Empty(mismatches);
    }

    /// <summary>
    ///     Pins <see cref="VsrOperations.ForCode" /> against the Rust dispatch table. The .NET mapping is a hand
    ///     written switch whose default arm is <see cref="VsrOperation.NonReplicated" />, so a command that gains
    ///     replication upstream would otherwise keep encoding as non-replicated here: it would apply on the node
    ///     that received it and never reach the others, diverging the replicas. This is the sweep
    ///     <c>no_replicated_command_ever_resolves_to_non_replicated</c> performs on the Rust side.
    /// </summary>
    [Fact]
    public void CommandOperationTable_MatchesTheRustDispatchTable()
    {
        var source = ReadRustSource(DispatchPath);

        // Table entries wrap across lines once the arguments are long enough, so the separators have to match
        // arbitrary whitespace rather than a single line.
        var replicated = Regex.Matches(source,
            @"CommandMeta::replicated\(\s*(\w+_CODE)\s*,\s*""[^""]*""\s*,\s*Operation::(\w+)\s*,?\s*\)");
        var nonReplicated = Regex.Matches(source, @"CommandMeta::non_replicated\(\s*(\w+_CODE)\s*,");

        Assert.NotEmpty(replicated);
        Assert.NotEmpty(nonReplicated);

        var mismatches = new List<string>();
        var matched = 0;

        foreach (Match entry in replicated)
        {
            var codeName = entry.Groups[1].Value;
            if (CommandCode(codeName) is not { } code)
            {
                // Skipping here would exempt the exact command this test exists to catch: one that gains
                // replication upstream while the .NET side has no code for it, so every call encodes as
                // non-replicated and applies on one node only.
                mismatches.Add($"{codeName}: rust replicates it, absent from .NET CommandCodes");

                continue;
            }

            matched++;
            if (!Enum.TryParse<VsrOperation>(entry.Groups[2].Value, out var expected))
            {
                mismatches.Add($"{codeName}: rust maps to Operation::{entry.Groups[2].Value}, absent from .NET");

                continue;
            }

            var actual = ResolveOperation(codeName, code);
            if (actual != expected)
            {
                mismatches.Add($"{codeName}: rust {expected}, .NET {ActualText(actual)}");
            }
        }

        foreach (Match entry in nonReplicated)
        {
            var codeName = entry.Groups[1].Value;
            if (CommandCode(codeName) is not { } code)
            {
                continue;
            }

            matched++;
            var expected = CodeMappingExceptions.TryGetValue(codeName, out var exempt)
                ? exempt
                : VsrOperation.NonReplicated;
            var actual = ResolveOperation(codeName, code);
            if (actual != expected)
            {
                mismatches.Add($"{codeName}: expected {ActualText(expected)}, .NET {ActualText(actual)}");
            }
        }

        Assert.Empty(mismatches);
        Assert.True(matched > 40, $"Only {matched} dispatch entries were matched by name; the Rust naming drifted.");
    }

    /// <summary>
    ///     Pins <see cref="VsrOperations.IsMetadata" /> against <c>Operation::is_metadata</c>. Metadata replies
    ///     lead their body with a committed result section, so an operation misclassified here has its rejection
    ///     entry decoded as payload and a refused command reads as a success.
    /// </summary>
    [Fact]
    public void MetadataOperations_MatchTheRustClassifier()
    {
        var body = MatchesArmBody(ReadRustSource(OperationPath), "is_metadata");
        var rust = Regex.Matches(body, @"Self::(\w+)").Select(match => match.Groups[1].Value).ToHashSet();
        Assert.NotEmpty(rust);

        var mismatches = new List<string>();
        foreach (VsrOperation operation in Enum.GetValues<VsrOperation>())
        {
            // is_internal() short-circuits ahead of the match arm on both sides, so those members are metadata
            // without appearing in the list.
            var expected = rust.Contains(operation.ToString()) || operation.IsInternal();
            if (operation.IsMetadata() != expected)
            {
                mismatches.Add($"{operation}: rust {expected}, .NET {operation.IsMetadata()}");
            }
        }

        Assert.Empty(mismatches);
    }

    /// <summary>
    ///     Pins <see cref="VsrOperations.IsResultFramed" /> against <c>Operation::is_result_framed</c>. The Rust
    ///     side composes it from <c>is_metadata</c> plus its own partition-plane list, so pinning
    ///     <c>is_metadata</c> alone leaves the second half free to drift. An operation that gains result framing
    ///     upstream and not here has <see cref="VsrReplyDecoder.SplitResultSection" /> skip the strip, and a
    ///     committed rejection decodes as payload: a refused command reads as a success.
    /// </summary>
    [Fact]
    public void ResultFramedOperations_MatchTheRustClassifier()
    {
        var body = MatchesArmBody(ReadRustSource(OperationPath), "is_result_framed");
        var rust = Regex.Matches(body, @"Self::(\w+)").Select(match => match.Groups[1].Value).ToHashSet();
        Assert.NotEmpty(rust);

        var mismatches = new List<string>();
        foreach (VsrOperation operation in Enum.GetValues<VsrOperation>())
        {
            // The Rust body is `is_metadata() || matches!(...)`, so the metadata members carry over without
            // appearing in the list.
            var expected = rust.Contains(operation.ToString()) || operation.IsMetadata();
            if (operation.IsResultFramed() != expected)
            {
                mismatches.Add($"{operation}: rust {expected}, .NET {operation.IsResultFramed()}");
            }
        }

        Assert.Empty(mismatches);
    }

    /// <summary>
    ///     Pins <see cref="VsrError" /> against the Rust error discriminants. These are copied by hand and drive
    ///     more than error text: <c>TRANSIENT_NOT_COMMITTED</c> and <c>TRANSIENT_NOT_ACCEPTED</c> are what tells a
    ///     same-session replay from a fresh-session reissue, so a renumber upstream would reissue a write whose
    ///     outcome is unknown under a client id the server cannot deduplicate against, applying it twice.
    /// </summary>
    [Fact]
    public void ErrorCodes_MatchTheRustDiscriminants()
    {
        var rust = Regex.Matches(ReadRustSource(ErrorPath), @"^\s{4}(\w+)(?:\([^)]*\))?\s*=\s*(\d+),",
                RegexOptions.Multiline)
            .ToDictionary(match => Normalize(match.Groups[1].Value),
                match => int.Parse(match.Groups[2].Value, CultureInfo.InvariantCulture));
        Assert.NotEmpty(rust);

        var mismatches = new List<string>();
        var matched = 0;

        foreach (FieldInfo field in typeof(VsrError).GetFields(BindingFlags.NonPublic | BindingFlags.Static))
        {
            if (!field.IsLiteral || field.FieldType != typeof(int))
            {
                continue;
            }

            if (!rust.TryGetValue(Normalize(field.Name), out var expected))
            {
                mismatches.Add($"{field.Name}: no Rust variant of that name remains");

                continue;
            }

            matched++;
            var actual = (int)field.GetValue(null)!;
            if (actual != expected)
            {
                mismatches.Add($"{field.Name}: rust {expected}, .NET {actual}");
            }
        }

        Assert.Empty(mismatches);
        Assert.True(matched > 10, $"Only {matched} error codes were matched by name; the Rust naming drifted.");
    }

    private static VsrOperation? ResolveOperation(string codeName, int code)
    {
        try
        {
            return VsrOperations.ForCode(code);
        }
        catch (IggyInvalidStatusCodeException)
        {
            // The legacy login codes reject rather than resolve; CodeMappingExceptions records that as null.
            _ = codeName;

            return null;
        }
    }

    private static string ActualText(VsrOperation? operation)
    {
        return operation?.ToString() ?? "rejected";
    }

    private static int? CommandCode(string name)
    {
        var field = typeof(CommandCodes).GetField(name,
            BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static);

        return field is null ? null : (int)field.GetValue(null)!;
    }

    /// <summary>Extracts the body of the <c>matches!</c> invocation inside the named method.</summary>
    private static string MatchesArmBody(string source, string method)
    {
        var start = source.IndexOf($"fn {method}(", StringComparison.Ordinal);
        Assert.True(start >= 0, $"Operation::{method} is gone from the Rust source.");

        var matches = source.IndexOf("matches!(", start, StringComparison.Ordinal);
        Assert.True(matches >= 0, $"Operation::{method} no longer classifies with matches!.");

        var depth = 0;
        for (var i = matches + "matches!".Length; i < source.Length; i++)
        {
            depth += source[i] switch
            {
                '(' => 1,
                ')' => -1,
                _ => 0
            };

            if (depth == 0)
            {
                return source[matches..(i + 1)];
            }
        }

        Assert.Fail($"Operation::{method} has an unbalanced matches! invocation.");

        return string.Empty;
    }

    /// <summary>
    ///     Pins the eviction reason to error mapping against <c>eviction_reason_to_error</c>, which the Rust
    ///     module documents as the single grader both its callers share "so the mappings cannot drift apart".
    ///     The .NET decoder is a third copy outside that guarantee, and the status it produces is what a caller
    ///     branches on, so an arm that silently regrades here changes public behaviour in one SDK only.
    /// </summary>
    [Fact]
    public void EvictionReasonErrors_MatchTheRustGrader()
    {
        IReadOnlyDictionary<string, int> errorCodes = RustErrorCodes();
        var body = RustItem.Body(ReadRustSource(EvictionGraderPath), "pub fn eviction_reason_to_error(");

        // Arms list one or more reasons separated by `|`; IncompatibleProtocol is a block arm and is asserted
        // separately below, on both of its branches.
        var arms = Regex.Matches(body,
            @"((?:\s*\|?\s*EvictionReason::\w+)+)\s*=>\s*IggyError::(\w+)");
        Assert.NotEmpty(arms);

        var expected = new Dictionary<string, int>(StringComparer.Ordinal);
        int? catchAll = null;

        foreach (Match arm in arms)
        {
            var code = errorCodes[Normalize(arm.Groups[2].Value)];
            foreach (Match reason in Regex.Matches(arm.Groups[1].Value, @"EvictionReason::(\w+)"))
            {
                expected[reason.Groups[1].Value] = code;
            }
        }

        var fallback = Regex.Match(body, @"_\s*=>\s*IggyError::(\w+)");
        Assert.True(fallback.Success, "eviction_reason_to_error no longer has a catch-all arm.");
        catchAll = errorCodes[Normalize(fallback.Groups[1].Value)];

        var mismatches = new List<string>();

        foreach (EvictionReason reason in Enum.GetValues<EvictionReason>())
        {
            if (reason == EvictionReason.IncompatibleProtocol)
            {
                continue;
            }

            var want = expected.TryGetValue(reason.ToString(), out var mapped) ? mapped : catchAll.Value;
            var got = StatusOf(reason);
            if (got != want)
            {
                mismatches.Add($"{reason}: rust {want}, .NET {got}");
            }
        }

        // A reason this build cannot decode arrives as null and must grade like the Rust catch-all.
        var unknown = StatusOf(null);
        if (unknown != catchAll.Value)
        {
            mismatches.Add($"<unrecognized>: rust {catchAll.Value}, .NET {unknown}");
        }

        Assert.Empty(mismatches);

        // IncompatibleProtocol reports the window, except when it is degenerate - a zero minimum or an
        // inverted range - which falls back to re-authentication.
        Assert.Equal(errorCodes["INCOMPATIBLEPROTOCOLVERSION"],
            StatusOf(EvictionReason.IncompatibleProtocol, serverVersion: 12, serverVersionMin: 8));
        Assert.Equal(errorCodes["UNAUTHENTICATED"],
            StatusOf(EvictionReason.IncompatibleProtocol, serverVersion: 12, serverVersionMin: 0));
        Assert.Equal(errorCodes["UNAUTHENTICATED"],
            StatusOf(EvictionReason.IncompatibleProtocol, serverVersion: 8, serverVersionMin: 12));
    }

    /// <summary>
    ///     Pins the result-section widths. They are hardcoded on the .NET side, and the Rust module notes that
    ///     the decoder and its encode mirror "share the widths below so they cannot drift".
    /// </summary>
    [Fact]
    public void ResultSectionWidths_MatchTheRustConstants()
    {
        var source = ReadRustSource(ReplyResultPath);

        Assert.Equal(VsrReplyDecoder.RESULT_COUNT_LENGTH, RustConstant(source, "RESULT_COUNT_LEN"));
        Assert.Equal(VsrReplyDecoder.RESULT_ENTRY_LENGTH, RustConstant(source, "RESULT_ENTRY_LEN"));
    }

    /// <summary>
    ///     Pins the credential bounds the client rejects on before spending a consensus round trip. Bounds that
    ///     drift wide let the server evict the session instead; bounds that drift narrow reject logins the
    ///     server would accept.
    /// </summary>
    [Fact]
    public void CredentialBounds_MatchTheRustDefaults()
    {
        var source = ReadRustSource(CredentialDefaultsPath);

        Assert.Equal(CredentialBounds.MIN_USERNAME_LENGTH, RustConstant(source, "MIN_USERNAME_LENGTH"));
        Assert.Equal(CredentialBounds.MAX_USERNAME_LENGTH, RustConstant(source, "MAX_USERNAME_LENGTH"));
        Assert.Equal(CredentialBounds.MIN_PASSWORD_LENGTH, RustConstant(source, "MIN_PASSWORD_LENGTH"));
        Assert.Equal(CredentialBounds.MAX_PASSWORD_LENGTH, RustConstant(source, "MAX_PASSWORD_LENGTH"));
    }

    /// <summary>
    ///     Pins the register reply body layout by reading the field offsets out of the Rust decoder and feeding
    ///     .NET a buffer laid out to them. The header offsets are pinned structurally above, but the bodies were
    ///     only ever checked against hand-written .NET expectations, which move together with the code.
    /// </summary>
    [Fact]
    public void LoginRegisterResponseLayout_MatchesTheRustDecoder()
    {
        var body = RustItem.Body(ReadRustSource(LoginRegisterResponsePath), "fn decode(buf: &[u8])");

        Assert.Equal(0, RustReadOffset(body, "read_u32_le", "user_id"));
        Assert.Equal(4, RustReadOffset(body, "read_u64_le", "session"));
        Assert.Equal(12, RustReadOffset(body, "read_u32_le", "server_protocol_version"));

        var versionOffset = Regex.Match(body, @"WireName::decode\(&buf\[(\d+)\.\.\]\)");
        Assert.True(versionOffset.Success, "The register reply no longer decodes its server version by offset.");
        Assert.Equal(16, Group(versionOffset, 1));

        Span<byte> reply = stackalloc byte[16 + 1 + 3];
        BinaryPrimitives.WriteUInt32LittleEndian(reply, 7);
        BinaryPrimitives.WriteUInt64LittleEndian(reply[4..], 42);
        BinaryPrimitives.WriteUInt32LittleEndian(reply[12..], 99);
        reply[16] = 3;
        "1.2"u8.CopyTo(reply[17..]);

        LoginRegisterResponse decoded = LoginRegister.Deserialize(reply);

        Assert.Equal(7u, decoded.UserId);
        Assert.Equal(42ul, decoded.Session);
    }

    /// <summary>
    ///     Pins the consumer-group assignment reply layout the same way. A silent offset shift here reassigns
    ///     partitions rather than failing, so every member of a group polls the wrong partitions.
    /// </summary>
    [Fact]
    public void SyncConsumerGroupResponseLayout_MatchesTheRustDecoder()
    {
        var body = RustItem.Body(ReadRustSource(SyncConsumerGroupResponsePath), "fn decode(buf: &[u8])");

        Assert.Equal(0, RustReadOffset(body, "read_u64_le", "generation"));
        Assert.Equal(8, RustReadOffset(body, "read_u32_le", "partitions_count"));

        var payloadOffset = Regex.Match(body, @"let mut offset = (\d+);");
        Assert.True(payloadOffset.Success, "The assignment reply no longer decodes its partitions by offset.");
        Assert.Equal(12, Group(payloadOffset, 1));

        Span<byte> reply = stackalloc byte[12 + 8];
        BinaryPrimitives.WriteUInt64LittleEndian(reply, 5);
        BinaryPrimitives.WriteUInt32LittleEndian(reply[8..], 2);
        BinaryPrimitives.WriteUInt32LittleEndian(reply[12..], 11);
        BinaryPrimitives.WriteUInt32LittleEndian(reply[16..], 13);

        SyncConsumerGroupAssignment decoded = SyncConsumerGroupAssignment.Decode(reply);

        Assert.Equal(5ul, decoded.Generation);
        Assert.Equal([11u, 13u], decoded.Partitions);
    }

    private static IReadOnlyDictionary<string, int> RustErrorCodes()
    {
        return Regex.Matches(ReadRustSource(ErrorPath), @"^\s{4}(\w+)(?:\([^)]*\))?\s*=\s*(\d+),",
                RegexOptions.Multiline)
            .ToDictionary(match => Normalize(match.Groups[1].Value),
                match => int.Parse(match.Groups[2].Value, CultureInfo.InvariantCulture));
    }

    private static int StatusOf(EvictionReason? reason, uint serverVersion = 0, uint serverVersionMin = 0)
    {
        var thrown = Assert.IsType<IggyInvalidStatusCodeException>(
            VsrReplyDecoder.ToException(new EvictionFrame(reason, serverVersion, serverVersionMin)));

        return thrown.StatusCode;
    }

    private static int RustConstant(string source, string name)
    {
        var declared = Regex.Match(source, $@"pub const {Regex.Escape(name)}\s*:\s*\w+\s*=\s*(\d+)\s*;");
        Assert.True(declared.Success, $"Rust no longer declares {name}.");

        return Group(declared, 1);
    }

    private static int RustReadOffset(string body, string reader, string field)
    {
        var read = Regex.Match(body, $@"let {Regex.Escape(field)} = {Regex.Escape(reader)}\(buf,\s*(\d+)\)");
        Assert.True(read.Success, $"Rust no longer decodes {field} with {reader} at a literal offset.");

        return Group(read, 1);
    }

    private static string Normalize(string name)
    {
        return name.Replace("_", string.Empty, StringComparison.Ordinal).ToUpperInvariant();
    }

    private static int Group(Match match, int index)
    {
        return int.Parse(match.Groups[index].Value, CultureInfo.InvariantCulture);
    }

    /// <summary>Every .NET member matches Rust, and Rust carries no member the .NET enum is missing.</summary>
    private static void AssertExactMatch<TEnum>(IReadOnlyDictionary<string, int> rust) where TEnum : struct, Enum
    {
        AssertSubsetMatches<TEnum>(rust);

        HashSet<string> ported = Enum.GetNames<TEnum>().ToHashSet();
        Assert.DoesNotContain(rust.Keys, name => !ported.Contains(name));
    }

    /// <summary>Every .NET member matches Rust; Rust members with no .NET counterpart are allowed.</summary>
    private static void AssertSubsetMatches<TEnum>(IReadOnlyDictionary<string, int> rust) where TEnum : struct, Enum
    {
        Assert.NotEmpty(rust);

        var mismatches = new List<string>();
        foreach (var name in Enum.GetNames<TEnum>())
        {
            var value = Convert.ToInt32(Enum.Parse<TEnum>(name), CultureInfo.InvariantCulture);
            if (!rust.TryGetValue(name, out var rustValue))
            {
                mismatches.Add($"{name}: missing on the Rust side");
            }
            else if (rustValue != value)
            {
                mismatches.Add($"{name}: rust {rustValue}, .NET {value}");
            }
        }

        Assert.Empty(mismatches);
    }

    private static string ReadRustSource(string relativePath)
    {
        var root = RepositoryRoot();
        if (root is null)
        {
            // Skipping suits a consumer running the suite outside a checkout, but in CI it would turn every
            // assertion in this file green on a Rust-side path change, which is the one drift the suite
            // cannot afford to miss.
            Assert.False(Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true",
                $"Rust sources are unavailable in CI: no ancestor of {AppContext.BaseDirectory} holds {HeaderPath}.");
            Assert.Skip("Rust sources are not available; run the drift check from a repository checkout.");
        }

        return File.ReadAllText(Path.Combine(root, relativePath));
    }

    private static string? RepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, HeaderPath)))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        return null;
    }
}

/// <summary>Discriminants of a <c>#[repr(u8)]</c> Rust enum, by member name.</summary>
internal static class RustEnum
{
    internal static IReadOnlyDictionary<string, int> Discriminants(string source, string name)
    {
        var body = RustItem.Body(source, $"pub enum {name} {{");
        var members = new Dictionary<string, int>();

        foreach (Match member in Regex.Matches(body, @"^\s*(\w+) = (\d+),", RegexOptions.Multiline))
        {
            members[member.Groups[1].Value] = int.Parse(member.Groups[2].Value, CultureInfo.InvariantCulture);
        }

        return members;
    }
}

/// <summary>
///     Field offsets of a <c>#[repr(C)]</c> Rust struct, computed from the declared field order the same way
///     rustc lays them out: each field starts at the next multiple of its alignment.
/// </summary>
internal static class RustStruct
{
    private static readonly Dictionary<string, (int Size, int Align)> ScalarLayouts = new()
    {
        ["u8"] = (1, 1),
        ["u16"] = (2, 2),
        ["u32"] = (4, 4),
        ["u64"] = (8, 8),
        ["u128"] = (16, 16),
        // Every enum the headers embed is `#[repr(u8)]`.
        ["Command2"] = (1, 1),
        ["Operation"] = (1, 1),
        ["EvictionReason"] = (1, 1)
    };

    internal static IReadOnlyDictionary<string, int> Offsets(string source, string name)
    {
        return Layout(source, name).Offsets;
    }

    internal static int Size(string source, string name)
    {
        return Layout(source, name).Size;
    }

    private static (Dictionary<string, int> Offsets, int Size) Layout(string source, string name)
    {
        var body = RustItem.Body(source, $"pub struct {name} {{");
        var offsets = new Dictionary<string, int>();
        var offset = 0;
        var structAlign = 1;

        foreach (Match field in Regex.Matches(body, @"^\s*pub (\w+): ([^,]+),", RegexOptions.Multiline))
        {
            var (size, align) = FieldLayout(field.Groups[2].Value.Trim(), name);
            offset = Align(offset, align);
            offsets[field.Groups[1].Value] = offset;
            offset += size;
            structAlign = Math.Max(structAlign, align);
        }

        return (offsets, Align(offset, structAlign));
    }

    private static (int Size, int Align) FieldLayout(string type, string structName)
    {
        if (ScalarLayouts.TryGetValue(type, out var scalar))
        {
            return scalar;
        }

        var array = Regex.Match(type, @"^\[u8; (\d+)\]$");
        if (array.Success)
        {
            return (int.Parse(array.Groups[1].Value, CultureInfo.InvariantCulture), 1);
        }

        throw new InvalidOperationException($"{structName} gained a field of unmapped type '{type}'.");
    }

    private static int Align(int offset, int alignment)
    {
        return (offset + alignment - 1) / alignment * alignment;
    }
}

internal static class RustItem
{
    /// <summary>The brace-delimited body following <paramref name="declaration" />, comments stripped.</summary>
    internal static string Body(string source, string declaration)
    {
        var start = source.IndexOf(declaration, StringComparison.Ordinal);
        if (start < 0)
        {
            throw new InvalidOperationException($"'{declaration}' is no longer declared in the Rust sources.");
        }

        var cursor = start + declaration.Length;
        var depth = 1;
        while (cursor < source.Length && depth > 0)
        {
            depth += source[cursor] switch
            {
                '{' => 1,
                '}' => -1,
                _ => 0
            };
            cursor++;
        }

        var body = source[(start + declaration.Length)..(cursor - 1)];

        return Regex.Replace(body, @"^\s*//.*$", string.Empty, RegexOptions.Multiline);
    }
}
