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

// Package vsr implements the consensus wire codec the Iggy SDK speaks over
// TCP. Every frame in both directions is a fixed 256-byte header followed by
// the body, with no outer length prefix and no command code in the prologue.
// The layout is a port of core/binary_protocol/src/consensus/header.rs.
package vsr

import "encoding/binary"

// HeaderSize is the length of every consensus frame header, both directions.
// A frame is this header followed by size-HeaderSize body bytes.
const (
	HeaderSize   = 256
	MaxFrameSize = 64 * 1024 * 1024
)

// Request header field offsets the client writes. Fields are addressed by
// byte offset rather than a struct cast because the header leads with
// unaligned u128 values.
const (
	requestOffsetSize      = 48
	requestOffsetCommand   = 60
	requestOffsetClient    = 128
	requestOffsetTimestamp = 160
	requestOffsetRequest   = 168
	requestOffsetOperation = 176
	requestOffsetNamespace = 184
	requestOffsetSession   = 192
	requestOffsetReserved  = 204
)

// Reply header field offsets the client reads.
const (
	replyOffsetSize      = 48
	replyOffsetCommand   = 60
	replyOffsetRequest   = 200
	replyOffsetOperation = 208
	replyOffsetNamespace = 216
	replyOffsetStatus    = 224
)

// Eviction header field offsets the client reads.
const (
	evictionOffsetCommand                  = 60
	evictionOffsetClient                   = 128
	evictionOffsetServerProtocolVersion    = 144
	evictionOffsetServerProtocolVersionMin = 148
	evictionOffsetReason                   = 255
)

// FrameCommand discriminates the frame type. It sits at the same offset in
// every consensus header, so one byte is enough to classify a read frame.
type FrameCommand uint8

// Frame discriminants a client sends or encounters.
const (
	FrameRequest  FrameCommand = 5
	FrameReply    FrameCommand = 8
	FrameEviction FrameCommand = 13
)

// EvictionReason is the terminal reason byte an eviction frame carries.
type EvictionReason uint8

// Eviction reasons the server sends.
const (
	EvictionReserved                EvictionReason = 0
	EvictionNoSession               EvictionReason = 1
	EvictionClientReleaseTooLow     EvictionReason = 2
	EvictionClientReleaseTooHigh    EvictionReason = 3
	EvictionInvalidRequestOperation EvictionReason = 4
	EvictionInvalidRequestBody      EvictionReason = 5
	EvictionInvalidRequestBodySize  EvictionReason = 6
	EvictionSessionTooLow           EvictionReason = 7
	EvictionSessionReleaseMismatch  EvictionReason = 8
	EvictionInvalidCredentials      EvictionReason = 9
	EvictionInvalidToken            EvictionReason = 10
	EvictionUserInactive            EvictionReason = 11
	EvictionSessionError            EvictionReason = 12
	EvictionStaleClient             EvictionReason = 13
	EvictionIncompatibleProtocol    EvictionReason = 14
	EvictionMalformedLogin          EvictionReason = 15
)

// ClientID is the ephemeral u128 client identifier. The low half precedes the
// high half on the wire, both little-endian.
type ClientID struct {
	Lo uint64
	Hi uint64
}

// IsZero reports whether the identifier is the all-zero value the server
// rejects during header validation.
func (c ClientID) IsZero() bool {
	return c.Lo == 0 && c.Hi == 0
}

// RequestFields are the header fields a client fills in. The rest of the 256
// bytes stay zero, including both checksums, which the server does not read.
type RequestFields struct {
	// Size is the header plus body total.
	Size uint32
	// Client is the ephemeral identifier the server's dedup table keys on.
	Client ClientID
	// Request is the request id: 0 for Register, else a session counter value.
	Request uint64
	// Operation is the consensus operation discriminant.
	Operation Operation
	// Namespace routes the request to the owning shard.
	Namespace uint64
	// Session is the bound session fence, or 0 while unbound.
	Session uint64
	// NonReplicatedCode is the raw command code the server reads out of
	// reserved[0..4]. It is written only for non-replicated operations, where
	// 0 is not a valid command code.
	NonReplicatedCode uint32
}

// EncodeRequestHeader writes fields into dst. dst is zeroed first so a reused
// buffer cannot leak bytes of a previous frame into the reserved regions.
func EncodeRequestHeader(dst *[HeaderSize]byte, fields RequestFields) {
	clear(dst[:])
	binary.LittleEndian.PutUint32(dst[requestOffsetSize:], fields.Size)
	dst[requestOffsetCommand] = byte(FrameRequest)
	binary.LittleEndian.PutUint64(dst[requestOffsetClient:], fields.Client.Lo)
	binary.LittleEndian.PutUint64(dst[requestOffsetClient+8:], fields.Client.Hi)
	binary.LittleEndian.PutUint64(dst[requestOffsetRequest:], fields.Request)
	dst[requestOffsetOperation] = byte(fields.Operation)
	binary.LittleEndian.PutUint64(dst[requestOffsetNamespace:], fields.Namespace)
	binary.LittleEndian.PutUint64(dst[requestOffsetSession:], fields.Session)
	if fields.NonReplicatedCode != 0 {
		binary.LittleEndian.PutUint32(dst[requestOffsetReserved:], fields.NonReplicatedCode)
	}
}

// PeekCommand reads the frame discriminant.
func PeekCommand(header *[HeaderSize]byte) FrameCommand {
	return FrameCommand(header[replyOffsetCommand])
}

// ReadSize reads the total frame length, header included.
func ReadSize(header *[HeaderSize]byte) uint32 {
	return binary.LittleEndian.Uint32(header[replyOffsetSize:])
}

// ReadStatus reads the pre-commit denial status. Zero means the reply
// committed and the body is meaningful.
func ReadStatus(header *[HeaderSize]byte) uint32 {
	return binary.LittleEndian.Uint32(header[replyOffsetStatus:])
}

// ReadReplyOperation reads the operation discriminant of a reply. The value is
// server-controlled, so callers must check it with IsKnownOperation.
func ReadReplyOperation(header *[HeaderSize]byte) Operation {
	return Operation(header[replyOffsetOperation])
}

// ReadReplyRequestID reads the request id the reply echoes from the request
// it answers, which is how the transport correlates a reply with the request
// in flight.
func ReadReplyRequestID(header *[HeaderSize]byte) uint64 {
	return binary.LittleEndian.Uint64(header[replyOffsetRequest:])
}

// StampedRequestID reads the request id back out of a stamped request frame.
func StampedRequestID(frame []byte) uint64 {
	return binary.LittleEndian.Uint64(frame[requestOffsetRequest:])
}

// Eviction holds the eviction-header fields the client acts on.
type Eviction struct {
	Reason EvictionReason
	// ServerProtocolVersion is the highest packed protocol version the server
	// accepts.
	ServerProtocolVersion uint32
	// ServerProtocolVersionMin is the lowest packed protocol version the
	// server accepts.
	ServerProtocolVersionMin uint32
}

// ReadEviction reads the eviction fields the client acts on.
func ReadEviction(header *[HeaderSize]byte) Eviction {
	return Eviction{
		Reason:                   EvictionReason(header[evictionOffsetReason]),
		ServerProtocolVersion:    binary.LittleEndian.Uint32(header[evictionOffsetServerProtocolVersion:]),
		ServerProtocolVersionMin: binary.LittleEndian.Uint32(header[evictionOffsetServerProtocolVersionMin:]),
	}
}
