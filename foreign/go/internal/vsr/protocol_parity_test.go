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

package vsr

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test guards the codec against drift in the Rust wire contract. It reads
// the protocol crate sources directly, so it only runs inside a checkout.
// Module consumers get a skip.

// rustSources are the files every parity assertion reads, relative to the
// repository root.
var rustSources = map[string]string{
	"codes":     "core/binary_protocol/src/codes.rs",
	"dispatch":  "core/binary_protocol/src/dispatch.rs",
	"header":    "core/binary_protocol/src/consensus/header.rs",
	"command":   "core/binary_protocol/src/consensus/command.rs",
	"operation": "core/binary_protocol/src/consensus/operation.rs",
	"cargo":     "core/binary_protocol/Cargo.toml",
	"eviction":  "core/common/src/error/eviction.rs",
}

// goOperations names every discriminant the codec declares. It exists so the
// parity check can match the Rust enum by name, not just by value.
var goOperations = map[string]Operation{
	"Reserved":                        OperationReserved,
	"Register":                        OperationRegister,
	"NonReplicated":                   OperationNonReplicated,
	"Logout":                          OperationLogout,
	"CreateTopicWithAssignments":      OperationCreateTopicWithAssignments,
	"CreatePartitionsWithAssignments": OperationCreatePartitionsWithAssignments,
	"RemoveConsumerGroupMember":       OperationRemoveConsumerGroupMember,
	"CompleteConsumerGroupRevocation": OperationCompleteConsumerGroupRevocation,
	"TruncatePartition":               OperationTruncatePartition,
	"CreateStream":                    OperationCreateStream,
	"UpdateStream":                    OperationUpdateStream,
	"DeleteStream":                    OperationDeleteStream,
	"PurgeStream":                     OperationPurgeStream,
	"CreateTopic":                     OperationCreateTopic,
	"UpdateTopic":                     OperationUpdateTopic,
	"DeleteTopic":                     OperationDeleteTopic,
	"PurgeTopic":                      OperationPurgeTopic,
	"CreatePartitions":                OperationCreatePartitions,
	"DeletePartitions":                OperationDeletePartitions,
	"DeleteSegments":                  OperationDeleteSegments,
	"CreateConsumerGroup":             OperationCreateConsumerGroup,
	"DeleteConsumerGroup":             OperationDeleteConsumerGroup,
	"CreateUser":                      OperationCreateUser,
	"UpdateUser":                      OperationUpdateUser,
	"DeleteUser":                      OperationDeleteUser,
	"ChangePassword":                  OperationChangePassword,
	"UpdatePermissions":               OperationUpdatePermissions,
	"CreatePersonalAccessToken":       OperationCreatePersonalAccessToken,
	"DeletePersonalAccessToken":       OperationDeletePersonalAccessToken,
	"JoinConsumerGroup":               OperationJoinConsumerGroup,
	"LeaveConsumerGroup":              OperationLeaveConsumerGroup,
	"SendMessages":                    OperationSendMessages,
	"StoreConsumerOffset":             OperationStoreConsumerOffset,
	"DeleteConsumerOffset":            OperationDeleteConsumerOffset,
}

// goEvictionReasons names every eviction discriminant the codec declares.
var goEvictionReasons = map[string]EvictionReason{
	"Reserved":                EvictionReserved,
	"NoSession":               EvictionNoSession,
	"ClientReleaseTooLow":     EvictionClientReleaseTooLow,
	"ClientReleaseTooHigh":    EvictionClientReleaseTooHigh,
	"InvalidRequestOperation": EvictionInvalidRequestOperation,
	"InvalidRequestBody":      EvictionInvalidRequestBody,
	"InvalidRequestBodySize":  EvictionInvalidRequestBodySize,
	"SessionTooLow":           EvictionSessionTooLow,
	"SessionReleaseMismatch":  EvictionSessionReleaseMismatch,
	"InvalidCredentials":      EvictionInvalidCredentials,
	"InvalidToken":            EvictionInvalidToken,
	"UserInactive":            EvictionUserInactive,
	"SessionError":            EvictionSessionError,
	"StaleClient":             EvictionStaleClient,
	"IncompatibleProtocol":    EvictionIncompatibleProtocol,
	"MalformedLogin":          EvictionMalformedLogin,
}

// goFrameCommands names the frame discriminants a client handles. The Rust
// enum is wider because it also covers replica-to-replica frames.
var goFrameCommands = map[string]FrameCommand{
	"Request":  FrameRequest,
	"Reply":    FrameReply,
	"Eviction": FrameEviction,
}

// goHeaderOffsets maps each Rust header struct to the offsets the codec reads
// or writes, keyed by the Rust field name.
var goHeaderOffsets = map[string]map[string]int{
	"RequestHeader": {
		"size":      requestOffsetSize,
		"command":   requestOffsetCommand,
		"client":    requestOffsetClient,
		"timestamp": requestOffsetTimestamp,
		"request":   requestOffsetRequest,
		"operation": requestOffsetOperation,
		"session":   requestOffsetSession,
		"reserved":  requestOffsetReserved,
	},
	"ReplyHeader": {
		"size":      replyOffsetSize,
		"command":   replyOffsetCommand,
		"operation": replyOffsetOperation,
		"status":    replyOffsetStatus,
	},
	"EvictionHeader": {
		"command":                     evictionOffsetCommand,
		"client":                      evictionOffsetClient,
		"server_protocol_version":     evictionOffsetServerProtocolVersion,
		"server_protocol_version_min": evictionOffsetServerProtocolVersionMin,
		"reason":                      evictionOffsetReason,
	},
}

// unimplementedCommandCodes are protocol codes the Go SDK deliberately does
// not declare. FlushUnsavedBuffer has no Go client method.
var unimplementedCommandCodes = []uint32{102}

// rustFieldLayout is the size and alignment of every field type the consensus
// headers use, enough to recompute their repr(C) offsets.
var rustFieldLayout = map[string][2]int{
	"u8":             {1, 1},
	"u16":            {2, 2},
	"u32":            {4, 4},
	"u64":            {8, 8},
	"u128":           {16, 16},
	"Command":        {1, 1},
	"Operation":      {1, 1},
	"EvictionReason": {1, 1},
}

// loadRustSources reads the protocol crate files. One sentinel path answers
// "is this a full checkout" and is the only reason to skip; every other read
// is required, so a moved or renamed Rust source fails the suite instead of
// silently turning every parity assertion into a skip.
func loadRustSources(t *testing.T) map[string]string {
	t.Helper()

	workDir, err := os.Getwd()
	require.NoError(t, err)
	root := filepath.Clean(filepath.Join(workDir, "..", "..", "..", ".."))

	sentinel := filepath.Join(root, "core", "binary_protocol", "Cargo.toml")
	if _, err := os.Stat(sentinel); err != nil {
		t.Skipf("not a full checkout, %s is missing: %v", sentinel, err)
	}

	sources := make(map[string]string, len(rustSources))
	for name, relative := range rustSources {
		content, err := os.ReadFile(filepath.Join(root, relative))
		require.NoError(t, err,
			"protocol source %s moved or vanished; update rustSources", relative)
		sources[name] = string(content)
	}
	return sources
}

// rustEnumValues extracts the `Name = value,` pairs of a Rust enum body.
func rustEnumValues(source, enumName string) map[string]uint64 {
	body := captureBlock(source, `pub enum `+enumName+` \{`)
	return namedNumbers(body, regexp.MustCompile(`(?m)^\s+([A-Za-z0-9]+)\s*=\s*([0-9_]+),$`))
}

// captureBlock returns the text between an opening pattern and the first
// line-anchored closing brace.
func captureBlock(source, openPattern string) string {
	pattern := regexp.MustCompile(`(?s)` + openPattern + `(.*?)\n\}`)
	match := pattern.FindStringSubmatch(source)
	if match == nil {
		return ""
	}
	return match[1]
}

func namedNumbers(source string, pattern *regexp.Regexp) map[string]uint64 {
	values := make(map[string]uint64)
	for _, match := range pattern.FindAllStringSubmatch(source, -1) {
		value, err := strconv.ParseUint(strings.ReplaceAll(match[2], "_", ""), 10, 64)
		if err != nil {
			continue
		}
		values[match[1]] = value
	}
	return values
}

// rustCommandCodes maps each Rust command-code constant name to its value.
func rustCommandCodes(source string) map[string]uint64 {
	return namedNumbers(source,
		regexp.MustCompile(`(?m)^pub const ([A-Z0-9_]+_CODE): u32 = ([0-9_]+);$`))
}

// goCommandCodes parses the SDK command-code constants out of their source, so
// a constant added there is covered without touching this test.
func goCommandCodes(t *testing.T) map[string]uint32 {
	t.Helper()

	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, filepath.Join("..", "command", "code.go"), nil, 0)
	require.NoError(t, err)

	codes := make(map[string]uint32)
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, spec := range general.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok || len(value.Names) != 1 || len(value.Values) != 1 {
				continue
			}
			literal, ok := value.Values[0].(*ast.BasicLit)
			if !ok || literal.Kind != token.INT {
				continue
			}
			parsed, err := strconv.ParseUint(literal.Value, 10, 32)
			require.NoError(t, err)
			codes[value.Names[0].Name] = uint32(parsed)
		}
	}
	require.NotEmpty(t, codes, "no command codes were parsed")
	return codes
}

func TestProtocolParity_CommandCodes(t *testing.T) {
	sources := loadRustSources(t)
	rustValues := rustCommandCodes(sources["codes"])
	require.NotEmpty(t, rustValues, "the Rust command registry was not found")

	protocolCodes := make(map[uint32]struct{}, len(rustValues))
	for _, value := range rustValues {
		protocolCodes[uint32(value)] = struct{}{}
	}

	declared := make(map[uint32]struct{})
	for name, code := range goCommandCodes(t) {
		assert.Contains(t, protocolCodes, code,
			"Go %s = %d is not a protocol command code", name, code)
		declared[code] = struct{}{}
	}

	var missing []uint32
	for code := range protocolCodes {
		if _, ok := declared[code]; !ok {
			missing = append(missing, code)
		}
	}
	slices.Sort(missing)
	assert.Equal(t, unimplementedCommandCodes, missing,
		"the set of protocol codes the Go SDK does not declare has changed")
}

func TestProtocolParity_OperationDiscriminants(t *testing.T) {
	sources := loadRustSources(t)
	rustValues := rustEnumValues(sources["operation"], "Operation")
	require.NotEmpty(t, rustValues, "the Rust Operation enum was not found")

	require.Len(t, goOperations, len(allOperations),
		"every declared operation is named in the parity table")
	assert.Len(t, goOperations, len(rustValues),
		"the codec declares every Rust operation")

	for name, want := range rustValues {
		got, ok := goOperations[name]
		if !assert.True(t, ok, "the codec does not declare Operation::%s", name) {
			continue
		}
		assert.Equal(t, want, uint64(got), "Operation::%s", name)
	}
}

func TestProtocolParity_ReplicatedOperationMap(t *testing.T) {
	sources := loadRustSources(t)
	codeValues := rustCommandCodes(sources["codes"])
	operationValues := rustEnumValues(sources["operation"], "Operation")
	require.NotEmpty(t, codeValues)
	require.NotEmpty(t, operationValues)

	pattern := regexp.MustCompile(
		`(?s)CommandMeta::replicated\(\s*([A-Z0-9_]+),.*?Operation::([A-Za-z0-9]+)`)
	entries := pattern.FindAllStringSubmatch(sources["dispatch"], -1)
	require.NotEmpty(t, entries, "the Rust replicated command table was not found")

	want := make(map[uint32]Operation, len(entries))
	for _, entry := range entries {
		code, ok := codeValues[entry[1]]
		require.True(t, ok, "unknown command constant %s", entry[1])
		operation, ok := operationValues[entry[2]]
		require.True(t, ok, "unknown operation %s", entry[2])
		want[uint32(code)] = Operation(operation)
	}

	assert.Equal(t, want, replicatedOperation)
}

func TestProtocolParity_EvictionReasons(t *testing.T) {
	sources := loadRustSources(t)
	rustValues := rustEnumValues(sources["header"], "EvictionReason")
	require.NotEmpty(t, rustValues, "the Rust EvictionReason enum was not found")

	assert.Len(t, goEvictionReasons, len(rustValues))
	for name, want := range rustValues {
		got, ok := goEvictionReasons[name]
		if !assert.True(t, ok, "the codec does not declare EvictionReason::%s", name) {
			continue
		}
		assert.Equal(t, want, uint64(got), "EvictionReason::%s", name)
	}
}

func TestProtocolParity_FrameCommands(t *testing.T) {
	sources := loadRustSources(t)
	rustValues := rustEnumValues(sources["command"], "Command")
	require.NotEmpty(t, rustValues, "the Rust Command enum was not found")

	for name, got := range goFrameCommands {
		want, ok := rustValues[name]
		if !assert.True(t, ok, "Rust does not declare Command::%s", name) {
			continue
		}
		assert.Equal(t, want, uint64(got), "Command::%s", name)
	}
}

func TestProtocolParity_HeaderSize(t *testing.T) {
	sources := loadRustSources(t)
	pattern := regexp.MustCompile(`pub const HEADER_SIZE: usize = ([0-9]+);`)
	match := pattern.FindStringSubmatch(sources["header"])
	require.NotNil(t, match, "the Rust HEADER_SIZE constant was not found")

	assert.Equal(t, strconv.Itoa(HeaderSize), match[1])
}

func TestProtocolParity_HeaderFieldOffsets(t *testing.T) {
	sources := loadRustSources(t)

	for structName, goOffsets := range goHeaderOffsets {
		t.Run(structName, func(t *testing.T) {
			rustOffsets := rustStructOffsets(t, sources["header"], structName)
			for field, want := range goOffsets {
				got, ok := rustOffsets[field]
				if !assert.True(t, ok, "%s has no field %s", structName, field) {
					continue
				}
				assert.Equal(t, got, want, "%s.%s", structName, field)
			}
		})
	}
}

// rustStructOffsets recomputes the repr(C) layout of a header struct, so a
// field inserted ahead of a consumed offset fails here instead of silently
// desyncing the codec's hardcoded tables.
func rustStructOffsets(t *testing.T, source, structName string) map[string]int {
	t.Helper()

	body := captureBlock(source, `pub struct `+structName+` \{`)
	require.NotEmpty(t, body, "the Rust struct %s was not found", structName)

	fieldPattern := regexp.MustCompile(`pub ([a-z_0-9]+): ([A-Za-z0-9_]+|\[u8; [0-9]+\]),`)
	arrayPattern := regexp.MustCompile(`^\[u8; ([0-9]+)\]$`)

	offsets := make(map[string]int)
	offset := 0
	for _, field := range fieldPattern.FindAllStringSubmatch(body, -1) {
		size, align := 0, 1
		if array := arrayPattern.FindStringSubmatch(field[2]); array != nil {
			parsed, err := strconv.Atoi(array[1])
			require.NoError(t, err)
			size = parsed
		} else {
			layout, ok := rustFieldLayout[field[2]]
			require.True(t, ok, "unknown field type %s in %s", field[2], structName)
			size, align = layout[0], layout[1]
		}
		if remainder := offset % align; remainder != 0 {
			offset += align - remainder
		}
		offsets[field[1]] = offset
		offset += size
	}
	require.Equal(t, HeaderSize, offset,
		"the computed %s layout does not fill the header", structName)
	return offsets
}

func TestProtocolParity_OperationClassification(t *testing.T) {
	sources := loadRustSources(t)
	rustValues := rustEnumValues(sources["operation"], "Operation")
	require.NotEmpty(t, rustValues)

	metadataNames := rustMatchesAllowlist(t, sources["operation"], "is_metadata")
	resultFramedNames := rustMatchesAllowlist(t, sources["operation"], "is_result_framed")

	internalStart := rustValues["CreateTopicWithAssignments"]
	metadataStart := rustValues["CreateStream"]
	require.NotZero(t, internalStart)
	require.NotZero(t, metadataStart)

	for name, value := range rustValues {
		operation := Operation(value)
		internal := value >= internalStart && value < metadataStart
		_, inMetadataList := metadataNames[name]
		metadata := internal || inMetadataList
		_, inResultFramedList := resultFramedNames[name]

		assert.Equal(t, internal, IsInternal(operation), "IsInternal(%s)", name)
		assert.Equal(t, metadata, IsMetadata(operation), "IsMetadata(%s)", name)
		assert.Equal(t, metadata || inResultFramedList, IsResultFramed(operation),
			"IsResultFramed(%s)", name)
		assert.True(t, IsKnownOperation(operation), "IsKnownOperation(%s)", name)
	}
}

// rustMatchesAllowlist collects the `Self::Name` arms of a `matches!` in a
// named predicate.
func rustMatchesAllowlist(t *testing.T, source, predicate string) map[string]struct{} {
	t.Helper()

	pattern := regexp.MustCompile(
		`(?s)fn ` + predicate + `.*?matches!\(\s*self,(.*?)\)\s*\n\s*\}`)
	match := pattern.FindStringSubmatch(source)
	require.NotNil(t, match, "the Rust %s allowlist was not found", predicate)

	names := make(map[string]struct{})
	for _, arm := range regexp.MustCompile(`Self::([A-Za-z0-9]+)`).FindAllStringSubmatch(match[1], -1) {
		names[arm[1]] = struct{}{}
	}
	require.NotEmpty(t, names)
	return names
}

// rustEvictionErrorCodes maps the IggyError variant names the canonical
// eviction mapping uses to their wire codes.
var rustEvictionErrorCodes = map[string]ierror.Code{
	"InvalidCredentials":         ierror.InvalidCredentialsCode,
	"InvalidPersonalAccessToken": ierror.InvalidPersonalAccessTokenCode,
	"Unauthenticated":            ierror.UnauthenticatedCode,
	"StaleClient":                ierror.StaleClientCode,
	"InvalidFormat":              ierror.InvalidFormatCode,
	"InvalidCommand":             ierror.InvalidCommandCode,
}

func TestProtocolParity_EvictionReasonMapping(t *testing.T) {
	sources := loadRustSources(t)
	body := captureBlock(sources["eviction"], `match reason \{`)
	require.NotEmpty(t, body, "the eviction_reason_to_error match was not found")

	armPattern := regexp.MustCompile(
		`(?s)((?:EvictionReason::[A-Za-z0-9]+\s*\|?\s*)+)=>\s*IggyError::([A-Za-z0-9]+)`)
	reasonPattern := regexp.MustCompile(`EvictionReason::([A-Za-z0-9]+)`)

	checked := 0
	for _, arm := range armPattern.FindAllStringSubmatch(body, -1) {
		wantCode, ok := rustEvictionErrorCodes[arm[2]]
		require.True(t, ok, "IggyError::%s is not in the parity table", arm[2])
		for _, reason := range reasonPattern.FindAllStringSubmatch(arm[1], -1) {
			value, ok := goEvictionReasons[reason[1]]
			require.True(t, ok, "unknown eviction reason %s", reason[1])
			mapped := NewEvictionError(Eviction{Reason: value})
			assert.Equal(t, wantCode, mapped.Code(), "EvictionReason::%s", reason[1])
			checked++
		}
	}
	require.GreaterOrEqual(t, checked, 8, "the mapping arms did not parse")

	// The fallback arm has teeth: a reason byte from a newer server must map
	// the same way on both sides, and never to a reconnectable error.
	fallback := regexp.MustCompile(`_ => IggyError::([A-Za-z0-9]+)`).FindStringSubmatch(body)
	require.NotNil(t, fallback, "the fallback arm was not found")
	wantFallback, ok := rustEvictionErrorCodes[fallback[1]]
	require.True(t, ok, "IggyError::%s is not in the parity table", fallback[1])
	mapped := NewEvictionError(Eviction{Reason: EvictionReason(0xEE)})
	assert.Equal(t, wantFallback, mapped.Code(),
		"the unknown-reason fallback diverges from core/common")
	// IncompatibleProtocol carries its own window logic, covered by the
	// dedicated eviction tests in reply_test.go.
}

// The client wire carries no routing namespace: the server derives the
// consensus group (plane from the operation, partition target from the
// payload) and stamps it into its own internal header, so this SDK has no
// packing rules to mirror. Growing the field back would move every field
// behind it, which is what makes this worth asserting on its own rather than
// leaving to the offset recomputation.
func TestProtocolParity_ClientHeadersCarryNoNamespace(t *testing.T) {
	sources := loadRustSources(t)

	for _, structName := range []string{"RequestHeader", "ReplyHeader"} {
		offsets := rustStructOffsets(t, sources["header"], structName)
		assert.NotContains(t, offsets, "namespace",
			"%s must not carry a routing namespace", structName)
	}
}

func TestProtocolParity_PackedProtocolVersion(t *testing.T) {
	sources := loadRustSources(t)
	pattern := regexp.MustCompile(`(?m)^version = "([0-9]+)\.([0-9]+)\.([0-9]+)`)
	match := pattern.FindStringSubmatch(sources["cargo"])
	require.NotNil(t, match, "the binary protocol crate version was not found")

	major, err := strconv.ParseUint(match[1], 10, 32)
	require.NoError(t, err)
	minor, err := strconv.ParseUint(match[2], 10, 32)
	require.NoError(t, err)

	assert.Equal(t, uint32(major), ProtocolVersion>>20, "protocol major")
	assert.Equal(t, uint32(minor), (ProtocolVersion>>10)&0x3FF, "protocol minor")
}
