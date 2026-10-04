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
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
)

// Session errors. They are programming or protocol faults rather than server
// rejections, so they carry no IggyError code.
var (
	// ErrSessionAlreadyBound is returned when a second Bind arrives without an
	// intervening reset.
	ErrSessionAlreadyBound = errors.New("vsr: session already bound")
	// ErrInvalidSession is returned when a register reply carries a zero
	// session fence.
	ErrInvalidSession = errors.New("vsr: session must be greater than zero")
	// ErrRequestCounterExhausted is returned when the request watermark would
	// wrap past its u64 range.
	ErrRequestCounterExhausted = errors.New("vsr: request counter exhausted")
	// ErrNotBound is returned when a replicated request id is taken before
	// Register commits.
	ErrNotBound = errors.New("vsr: request id taken before the session is bound")
)

// Session is the consensus session state, a port of core/sdk/src/session.rs.
// It holds the ephemeral client identifier, the session fence the server
// assigns once Register commits, and the request watermark.
//
// Not safe for concurrent use. The transport owns one per connection and
// serializes access through its exchange lock, which the lockstep request
// model already requires.
type Session struct {
	client           ClientID
	session          uint64
	bound            bool
	requestCounter   uint64
	registerConsumed bool
}

// NewSession returns an unbound session with a fresh client identifier.
func NewSession() *Session {
	return NewSessionWithClientID(newClientID())
}

// NewSessionWithClientID returns an unbound session with a caller-supplied
// client identifier.
func NewSessionWithClientID(client ClientID) *Session {
	return &Session{client: client, requestCounter: 1}
}

// ClientID returns the current ephemeral client identifier.
func (s *Session) ClientID() ClientID {
	return s.client
}

// Bound reports whether Register has committed and the fence is set.
func (s *Session) Bound() bool {
	return s.bound
}

// SessionID returns the bound session fence, or 0 while unbound.
func (s *Session) SessionID() uint64 {
	return s.session
}

// HasActivity reports whether the session has registered, bound, or issued a
// replicated request.
func (s *Session) HasActivity() bool {
	return s.registerConsumed || s.bound || s.requestCounter > 1
}

// BeginRegister arms the session for a Register and returns its request id,
// always 0. A session that already registered or bound is replaced wholesale
// with a fresh client identifier, so a re-login encodes a clean Register
// instead of tripping the one-shot guard.
func (s *Session) BeginRegister() uint64 {
	if s.registerConsumed || s.bound {
		s.reset()
	}
	s.registerConsumed = true
	return 0
}

// Bind records the session fence the server assigned when Register committed.
func (s *Session) Bind(session uint64) error {
	if s.bound {
		return ErrSessionAlreadyBound
	}
	if session == 0 {
		return ErrInvalidSession
	}
	s.session = session
	s.bound = true
	return nil
}

// NextRequestID returns the next replicated-metadata request id and advances
// the watermark: 1, 2, 3, and so on.
func (s *Session) NextRequestID() (uint64, error) {
	if !s.bound {
		return 0, ErrNotBound
	}
	if s.requestCounter == math.MaxUint64 {
		return 0, ErrRequestCounterExhausted
	}
	id := s.requestCounter
	s.requestCounter++
	return id, nil
}

// CurrentRequestID returns the watermark without advancing it. Non-replicated
// requests use it because they never consult the client table: they route by
// transport identity, and the table accepts any id above the watermark with
// no contiguity requirement, so reading here gaps nothing.
func (s *Session) CurrentRequestID() uint64 {
	return s.requestCounter
}

// Reset drops the session and mints a fresh client identifier. A disconnect
// invalidates the server-side fence, so the next Register must arrive under a
// new identity.
func (s *Session) Reset() {
	s.reset()
}

func (s *Session) reset() {
	s.client = newClientID()
	s.session = 0
	s.bound = false
	s.requestCounter = 1
	s.registerConsumed = false
}

// newClientID draws an ephemeral u128, retrying the one value the server
// rejects during header validation. crypto/rand.Read does not report an error
// as of Go 1.24. It panics if the system entropy source is unavailable.
func newClientID() ClientID {
	var buf [16]byte
	for {
		_, _ = rand.Read(buf[:])
		client := ClientID{
			Lo: binary.LittleEndian.Uint64(buf[0:8]),
			Hi: binary.LittleEndian.Uint64(buf[8:16]),
		}
		if !client.IsZero() {
			return client
		}
	}
}
