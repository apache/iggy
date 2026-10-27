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

package iggcon

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// redactedValue stands in for a value diagnostics never show.
const redactedValue = "<redacted>"

// redactedEntry is a raw options entry with its value hidden.
type redactedEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Format writes the options with the key and every raw value redacted.
func (o StreamOptions) Format(state fmt.State, _ rune) { formatRedacted(state, o) }

// MarshalJSON writes the options with the key and every raw value redacted.
func (o StreamOptions) MarshalJSON() ([]byte, error) { return marshalRedacted(o) }

// Format writes the options with every raw value redacted.
func (o TopicOptions) Format(state fmt.State, _ rune) { formatRedacted(state, o) }

// MarshalJSON writes the options with every raw value redacted.
func (o TopicOptions) MarshalJSON() ([]byte, error) { return marshalRedacted(o) }

// Format writes the options with every raw value redacted.
func (o UserOptions) Format(state fmt.State, _ rune) { formatRedacted(state, o) }

// MarshalJSON writes the options with every raw value redacted.
func (o UserOptions) MarshalJSON() ([]byte, error) { return marshalRedacted(o) }

// Format writes the patch with every raw value redacted.
func (u StreamUpdate) Format(state fmt.State, _ rune) { formatRedacted(state, u) }

// MarshalJSON writes the patch with every raw value redacted.
func (u StreamUpdate) MarshalJSON() ([]byte, error) { return marshalRedacted(u) }

// Format writes the patch with every raw value redacted.
func (u TopicUpdate) Format(state fmt.State, _ rune) { formatRedacted(state, u) }

// MarshalJSON writes the patch with every raw value redacted.
func (u TopicUpdate) MarshalJSON() ([]byte, error) { return marshalRedacted(u) }

// Format writes the patch with every raw value redacted.
func (u UserUpdate) Format(state fmt.State, _ rune) { formatRedacted(state, u) }

// MarshalJSON writes the patch with every raw value redacted.
func (u UserUpdate) MarshalJSON() ([]byte, error) { return marshalRedacted(u) }

func (e redactedEntry) String() string { return e.Key + ":" + e.Value }

// formatRedacted prints every exported field, so a new field shows up without
// an edit here, for every verb.
func formatRedacted(state fmt.State, options any) {
	fields := redactedFields(options)
	printed := make([]string, 0, len(fields))
	for _, field := range fields {
		value := field.value
		if value == nil {
			value = "<nil>"
		}
		printed = append(printed, fmt.Sprintf("%s:%v", field.name, value))
	}
	_, _ = fmt.Fprintf(state, "%s{%s}", reflect.TypeOf(options).Name(), strings.Join(printed, " "))
}

func marshalRedacted(options any) ([]byte, error) {
	var buffer bytes.Buffer
	buffer.WriteByte('{')
	for index, field := range redactedFields(options) {
		if index > 0 {
			buffer.WriteByte(',')
		}
		name, err := json.Marshal(field.name)
		if err != nil {
			return nil, err
		}
		value, err := json.Marshal(field.value)
		if err != nil {
			return nil, err
		}
		buffer.Write(name)
		buffer.WriteByte(':')
		buffer.Write(value)
	}
	buffer.WriteByte('}')
	return buffer.Bytes(), nil
}

type redactedField struct {
	name  string
	value any
}

// redactedFields hides byte slices, which hold key material, and every raw
// entry value, because this build cannot tell which newer keys are secret.
func redactedFields(options any) []redactedField {
	value := reflect.ValueOf(options)
	fields := make([]redactedField, 0, value.NumField())
	for index := range value.NumField() {
		if !value.Type().Field(index).IsExported() {
			continue
		}
		field := value.Field(index)
		var shown any
		switch typed := field.Interface().(type) {
		case []byte:
			if typed != nil {
				shown = redactedValue
			}
		case []HeaderEntry:
			entries := make([]redactedEntry, 0, len(typed))
			for _, entry := range typed {
				entries = append(entries, redactedEntry{Key: string(entry.Key.Value), Value: redactedValue})
			}
			shown = entries
		default:
			if field.Kind() != reflect.Pointer {
				shown = field.Interface()
			} else if !field.IsNil() {
				shown = field.Elem().Interface()
			}
		}
		fields = append(fields, redactedField{name: value.Type().Field(index).Name, value: shown})
	}
	return fields
}
