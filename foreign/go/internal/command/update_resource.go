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

package command

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"unicode/utf8"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

const (
	// maxOptions and maxOptionsBytes mirror the server's options block bounds.
	maxOptions      = 1024
	maxOptionsBytes = 100_000
	// maxOptionField bounds every key and value of the header codec.
	maxOptionField = 255
	// maxResetKeys is what the one-byte reset count carries.
	maxResetKeys = 255
	// minUsernameLength and maxUsernameLength mirror the server's username bounds.
	minUsernameLength = 3
	maxUsernameLength = 50
	// keyPreviewLength caps how much of a refused key an error repeats.
	keyPreviewLength = 64
)

// optionRule is what this build knows about one catalog key, enough to refuse
// a reset with the category server admission would return.
type optionRule struct {
	key        string
	scopes     []iggcon.OptionsScope
	updatable  bool
	resettable bool
	// readOnly covers the internal keys too: a client may send neither.
	readOnly bool
}

// optionCatalog lists the server catalog in its own order.
var optionCatalog = []optionRule{
	{key: string(iggcon.TopicOptionName), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeStream, iggcon.OptionsScopeTopic}, updatable: true},
	{key: string(iggcon.UserOptionUsername), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeUser}, updatable: true},
	{key: string(iggcon.UserOptionStatus), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeUser}, updatable: true},
	{key: string(iggcon.TopicOptionCompressionAlgorithm), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeTopic}},
	{key: string(iggcon.TopicOptionMessageExpiry), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeTopic}, updatable: true, resettable: true},
	{key: string(iggcon.TopicOptionMaxTopicSize), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeTopic}, updatable: true, resettable: true},
	{key: string(iggcon.TopicOptionSegmentSize), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeTopic}},
	{key: string(iggcon.TopicOptionDurability), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeTopic}},
	{key: string(iggcon.TopicOptionConsumerOffsetDurability), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeTopic}},
	{key: string(iggcon.TopicOptionPartitionResizePolicy), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeTopic}},
	{key: string(iggcon.TopicOptionMessagesRequiredToSave), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeTopic}},
	{key: string(iggcon.TopicOptionSizeOfMessagesRequiredToSave), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeTopic}},
	{key: string(iggcon.TopicOptionPreallocateSegments), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeTopic}},
	{key: string(iggcon.StreamOptionEncryptionKey), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeStream}},
	{key: "encryption_key_wrapped", scopes: []iggcon.OptionsScope{iggcon.OptionsScopeStream}, readOnly: true},
	{key: string(iggcon.StreamOptionEncryptionKeySource), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeStream}, readOnly: true},
	{key: string(iggcon.StreamOptionEncryptionKeyFingerprint), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeStream}, readOnly: true},
	{key: string(iggcon.StreamOptionEncryptionKekFingerprint), scopes: []iggcon.OptionsScope{iggcon.OptionsScopeStream}, readOnly: true},
	{key: "critical_keys", scopes: []iggcon.OptionsScope{iggcon.OptionsScopeStream, iggcon.OptionsScopeTopic, iggcon.OptionsScopeUser}, readOnly: true},
}

// encodeUpdateResource builds an UpdateResource body:
// [scope:u8][target identifiers][reset_count:u8][reset_key:u8 length + bytes]*[options to end].
//
// typed entries come from the update's own fields and win over extra entries
// with the same key. A typed-only patch the server would refuse is refused
// here first, with the same error. A patch with extra entries or raw reset
// names gets only the structural checks, and the server decides the rest.
func encodeUpdateResource(
	scope iggcon.OptionsScope,
	targets []iggcon.Identifier,
	typed, extra []iggcon.HeaderEntry,
	reset, rawReset []string,
) ([]byte, error) {
	set, err := mergeOptions(typed, extra)
	if err != nil {
		return nil, err
	}
	reset, err = validatePatch(scope, set, append(slices.Clip(reset), rawReset...), len(extra) > 0 || len(rawReset) > 0)
	if err != nil {
		return nil, err
	}
	options, err := encodeOptions(set)
	if err != nil {
		return nil, err
	}

	body := []byte{byte(scope)}
	for _, target := range targets {
		if body, err = target.AppendBinary(body); err != nil {
			return nil, err
		}
	}
	body = append(body, byte(len(reset)))
	for _, key := range reset {
		body = append(body, byte(len(key)))
		body = append(body, key...)
	}
	return append(body, options...), nil
}

// validatePatch returns the reset names deduplicated in ascending order, the
// order the server's own encoders write. A typed-only patch gets server
// admission's update checks in the server's order. A raw patch gets only the
// structural ones, because the server's catalog may know keys or flags this
// build does not.
func validatePatch(scope iggcon.OptionsScope, set []iggcon.HeaderEntry, reset []string, raw bool) ([]string, error) {
	names := slices.Compact(slices.Sorted(slices.Values(reset)))
	if len(set) == 0 && len(names) == 0 {
		return nil, ierror.ErrInvalidCommand
	}
	for _, entry := range set {
		if slices.Contains(names, string(entry.Key.Value)) {
			return nil, ierror.ErrInvalidCommand
		}
	}
	if !raw {
		if err := admitUpdate(scope, set, names); err != nil {
			return nil, err
		}
	}
	for _, name := range names {
		if len(name) == 0 || len(name) > maxOptionField {
			return nil, ierror.UnsupportedOptionKey{Key: keyPreview(name)}
		}
	}
	if len(names) > maxResetKeys {
		// The catalog is far smaller than the limit, so such a list always
		// names a key admission would refuse.
		for _, name := range names {
			if _, known := catalogRule(scope, name); !known {
				return nil, ierror.UnsupportedOptionKey{Key: keyPreview(name)}
			}
		}
	}
	return names, nil
}

// admitUpdate mirrors server admission for a patch of typed fields: reset key
// categories first, then the column values.
func admitUpdate(scope iggcon.OptionsScope, set []iggcon.HeaderEntry, names []string) error {
	rules := make([]optionRule, 0, len(names))
	for _, name := range names {
		rule, known := catalogRule(scope, name)
		if !known {
			return ierror.UnsupportedOptionKey{Key: keyPreview(name)}
		}
		rules = append(rules, rule)
	}
	for _, rule := range rules {
		if rule.readOnly {
			return ierror.ReadOnlyOption{Key: rule.key}
		}
	}
	for _, rule := range rules {
		if !rule.updatable {
			return ierror.OptionNotUpdatable{Key: rule.key}
		}
	}
	for _, rule := range rules {
		if !rule.resettable {
			return ierror.OptionNotResettable{Key: rule.key}
		}
	}
	for _, entry := range slices.SortedFunc(slices.Values(set), func(left, right iggcon.HeaderEntry) int {
		return bytes.Compare(left.Key.Value, right.Key.Value)
	}) {
		if err := admitColumn(scope, entry); err != nil {
			return err
		}
	}
	return nil
}

// admitColumn applies the server's bounds to a name or username value.
func admitColumn(scope iggcon.OptionsScope, entry iggcon.HeaderEntry) error {
	text := entry.Value.Value
	isText := entry.Value.Kind == iggcon.String
	switch string(entry.Key.Value) {
	case string(iggcon.TopicOptionName):
		if !isText || len(text) == 0 || len(text) > maxOptionField {
			if scope == iggcon.OptionsScopeStream {
				return ierror.ErrInvalidStreamName
			}
			return ierror.ErrInvalidTopicName
		}
	case string(iggcon.UserOptionUsername):
		if !isText || len(text) < minUsernameLength || len(text) > maxUsernameLength {
			return ierror.ErrInvalidUsername
		}
	}
	return nil
}

// catalogRule finds the rule of a key that applies to scope.
func catalogRule(scope iggcon.OptionsScope, name string) (optionRule, bool) {
	index := slices.IndexFunc(optionCatalog, func(rule optionRule) bool {
		return rule.key == name && slices.Contains(rule.scopes, scope)
	})
	if index < 0 {
		return optionRule{}, false
	}
	return optionCatalog[index], true
}

// mergeOptions appends the caller's entries to the ones the typed fields
// produced, dropping any that would duplicate a typed key.
//
// The block must not carry a key twice: wire validation refuses the whole
// request for a duplicate. The typed field wins because it is the specific
// argument, mirroring how the Rust SDK inserts typed values after raw ones.
func mergeOptions(typed, extra []iggcon.HeaderEntry) ([]iggcon.HeaderEntry, error) {
	if len(extra) == 0 {
		return typed, nil
	}
	seen := make(map[string]struct{}, len(typed)+len(extra))
	for _, entry := range typed {
		seen[string(entry.Key.Value)] = struct{}{}
	}
	merged := slices.Clip(typed)
	for _, entry := range extra {
		if entry.Key.Kind != iggcon.String {
			return nil, ierror.UnsupportedOptionKey{Key: keyPreview(string(entry.Key.Value))}
		}
		if _, duplicate := seen[string(entry.Key.Value)]; duplicate {
			continue
		}
		merged = append(merged, entry)
	}
	return merged, nil
}

// encodeOptions encodes an options block in ascending key order, after the
// checks the server's decoder applies to it.
func encodeOptions(entries []iggcon.HeaderEntry) ([]byte, error) {
	if len(entries) > maxOptions {
		return nil, ierror.OptionsBlockTooLarge{Details: fmt.Sprintf("%d entries, at most %d", len(entries), maxOptions)}
	}
	sorted := slices.Clone(entries)
	slices.SortFunc(sorted, func(left, right iggcon.HeaderEntry) int {
		return bytes.Compare(left.Key.Value, right.Key.Value)
	})
	size := 0
	for index, entry := range sorted {
		key := entry.Key.Value
		if entry.Key.Kind != iggcon.String || len(key) == 0 || len(key) > maxOptionField || !utf8.Valid(key) {
			return nil, ierror.UnsupportedOptionKey{Key: keyPreview(string(key))}
		}
		if index > 0 && bytes.Equal(sorted[index-1].Key.Value, key) {
			return nil, ierror.ErrInvalidCommand
		}
		value := entry.Value
		expected := value.Kind.ExpectedSize()
		if value.Kind <= 0 || value.Kind > 255 || len(value.Value) == 0 || len(value.Value) > maxOptionField ||
			expected != -1 && len(value.Value) != expected {
			return nil, ierror.InvalidOptionValue{Key: keyPreview(string(key))}
		}
		size += 1 + 4 + len(key) + 1 + 4 + len(value.Value)
	}
	if size > maxOptionsBytes {
		return nil, ierror.OptionsBlockTooLarge{Details: fmt.Sprintf("%d bytes, at most %d", size, maxOptionsBytes)}
	}
	return iggcon.GetHeadersBytes(sorted), nil
}

// textOption builds a String entry for a column key. Text no header value
// holds travels as a Bool, a kind the column never accepts, so admission
// reports the column's own error after every check that comes before it.
func textOption(key string, value string) iggcon.HeaderEntry {
	if len(value) == 0 || len(value) > maxOptionField {
		return iggcon.HeaderEntry{
			Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte(key)},
			Value: iggcon.HeaderValue{Kind: iggcon.Bool, Value: []byte{0}},
		}
	}
	return stringOption(key, value)
}

func stringOption(key string, value string) iggcon.HeaderEntry {
	return iggcon.HeaderEntry{
		Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte(key)},
		Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte(value)},
	}
}

func uint64Option(key string, value uint64) iggcon.HeaderEntry {
	return iggcon.HeaderEntry{
		Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte(key)},
		Value: iggcon.HeaderValue{Kind: iggcon.Uint64, Value: binary.LittleEndian.AppendUint64(nil, value)},
	}
}

// keyPreview bounds how much of a refused, caller-sized key an error repeats.
func keyPreview(key string) string {
	runes := []rune(key)
	if len(runes) > keyPreviewLength {
		return string(runes[:keyPreviewLength])
	}
	return key
}
