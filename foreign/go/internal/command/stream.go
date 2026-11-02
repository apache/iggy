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
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

const (
	nameLengthOffset = 0
	payloadOffset    = 1
)

type CreateStream struct {
	Name    string
	Options iggcon.StreamOptions
}

func (request *CreateStream) Code() Code {
	return CreateStreamCode
}

// MarshalBinary refuses an invalid key, typed or raw, so it never leaves the
// client.
func (request *CreateStream) MarshalBinary() ([]byte, error) {
	var typed []iggcon.HeaderEntry
	if request.Options.EncryptionKey != nil {
		entry, err := iggcon.EncryptionKeyOption(request.Options.EncryptionKey)
		if err != nil {
			return nil, err
		}
		typed = append(typed, entry)
	}
	options, err := mergeOptions(typed, request.Options.Raw)
	if err != nil {
		return nil, err
	}
	for _, entry := range options {
		if err := validateEncryptionKeyEntry(entry); err != nil {
			return nil, err
		}
	}
	optionsBytes, err := encodeOptions(options)
	if err != nil {
		return nil, err
	}
	nameLength := len(request.Name)
	serialized := make([]byte, payloadOffset+nameLength, payloadOffset+nameLength+len(optionsBytes))
	serialized[nameLengthOffset] = byte(nameLength)
	copy(serialized[payloadOffset:], request.Name)
	return append(serialized, optionsBytes...), nil
}

// validateEncryptionKeyEntry checks an encryption_key entry in either form the
// server takes: 32 raw bytes, or their canonical base64 text.
func validateEncryptionKeyEntry(entry iggcon.HeaderEntry) error {
	if string(entry.Key.Value) != string(iggcon.StreamOptionEncryptionKey) {
		return nil
	}
	switch entry.Value.Kind {
	case iggcon.Raw:
		if len(entry.Value.Value) == iggcon.StreamEncryptionKeyLength {
			return nil
		}
	case iggcon.String:
		if _, err := iggcon.DecodeEncryptionKey(string(entry.Value.Value)); err == nil {
			return nil
		}
	}
	return ierror.ErrInvalidEncryptionKey
}

type GetStream struct {
	StreamId iggcon.Identifier
}

func (g *GetStream) Code() Code {
	return GetStreamCode
}

func (g *GetStream) MarshalBinary() ([]byte, error) {
	return g.StreamId.MarshalBinary()
}

type GetStreams struct{}

func (g *GetStreams) Code() Code {
	return GetStreamsCode
}

func (g *GetStreams) MarshalBinary() ([]byte, error) {
	return []byte{}, nil
}

// UpdateStream patches a stream through the generic UpdateResource command.
type UpdateStream struct {
	StreamId iggcon.Identifier
	Name     *string
	Reset    []iggcon.StreamOptionKey
	RawReset []string
	Options  []iggcon.HeaderEntry
}

func (u *UpdateStream) Code() Code {
	return UpdateResourceCode
}

func (u *UpdateStream) MarshalBinary() ([]byte, error) {
	var typed []iggcon.HeaderEntry
	if u.Name != nil {
		typed = append(typed, textOption(string(iggcon.StreamOptionName), *u.Name))
	}
	reset := make([]string, 0, len(u.Reset))
	for _, key := range u.Reset {
		reset = append(reset, string(key))
	}
	return encodeUpdateResource(iggcon.OptionsScopeStream, []iggcon.Identifier{u.StreamId}, typed, u.Options, reset, u.RawReset)
}

type DeleteStream struct {
	StreamId iggcon.Identifier
}

func (d *DeleteStream) Code() Code {
	return DeleteStreamCode
}

func (d *DeleteStream) MarshalBinary() ([]byte, error) {
	return d.StreamId.MarshalBinary()
}
