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

package tcp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/internal/command"
)

// fakeMarshalOnlyCmd implements command.Command without AppendBinary; it forces
// encodeWireRequest down the MarshalBinary fallback branch.
type fakeMarshalOnlyCmd struct {
	body    []byte
	code    command.Code
	wantErr error
}

func (f *fakeMarshalOnlyCmd) Code() command.Code { return f.code }
func (f *fakeMarshalOnlyCmd) MarshalBinary() ([]byte, error) {
	if f.wantErr != nil {
		return nil, f.wantErr
	}
	out := make([]byte, len(f.body))
	copy(out, f.body)
	return out, nil
}

func newTestPollCmd() *command.PollMessages {
	consumerId, _ := iggcon.NewIdentifier(uint32(42))
	streamId, _ := iggcon.NewIdentifier("test_stream_id")
	topicId, _ := iggcon.NewIdentifier("test_topic_id")
	pid := uint32(7)
	return &command.PollMessages{
		Consumer:    iggcon.NewSingleConsumer(consumerId),
		StreamId:    streamId,
		TopicId:     topicId,
		PartitionId: &pid,
		Strategy:    iggcon.FirstPollingStrategy(),
		Count:       100,
		AutoCommit:  true,
	}
}

func TestEncodeWireRequest_AppenderPathMatchesCreatePayload(t *testing.T) {
	cmd := newTestPollCmd()

	// reference: the legacy MarshalBinary + createPayload pipeline.
	body, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	want := createPayload(body, cmd.Code())

	// new path through encodeWireRequest, simulating a pool buffer.
	buf := make([]byte, 0, 256)
	got, err := encodeWireRequest(buf, cmd)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(got, want) {
		t.Fatalf("wire bytes diverge\nwant: %v\n got: %v", want, got)
	}
	// length header = body + 4-byte command code
	gotLen := binary.LittleEndian.Uint32(got[:4])
	if int(gotLen) != len(body)+4 {
		t.Errorf("length header = %d, want %d", gotLen, len(body)+4)
	}
	gotCode := binary.LittleEndian.Uint32(got[4:8])
	if command.Code(gotCode) != cmd.Code() {
		t.Errorf("code header = %d, want %d", gotCode, cmd.Code())
	}
}

func TestEncodeWireRequest_FallbackPathMatchesCreatePayload(t *testing.T) {
	cmd := &fakeMarshalOnlyCmd{
		body: []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03},
		code: command.Code(42),
	}
	want := createPayload(cmd.body, cmd.code)

	buf := make([]byte, 0, 256)
	got, err := encodeWireRequest(buf, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("fallback bytes diverge\nwant: %v\n got: %v", want, got)
	}
}

func TestEncodeWireRequest_FallbackPropagatesMarshalError(t *testing.T) {
	sentinel := errors.New("marshal failed")
	cmd := &fakeMarshalOnlyCmd{code: command.Code(1), wantErr: sentinel}
	if _, err := encodeWireRequest(nil, cmd); !errors.Is(err, sentinel) {
		t.Fatalf("got err=%v, want %v", err, sentinel)
	}
}

func TestEncodeWireRequest_GrowsUndersizedBuffer(t *testing.T) {
	cmd := newTestPollCmd()
	// Reference: full wire bytes when starting from a generous buffer.
	want, err := encodeWireRequest(make([]byte, 0, 256), cmd)
	if err != nil {
		t.Fatal(err)
	}
	// Start with a cap=4 buffer — far smaller than the encoded request needs.
	got, err := encodeWireRequest(make([]byte, 0, 4), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("undersized buf produced different bytes\nwant: %v\n got: %v", want, got)
	}
	// Fallback branch should also grow.
	fb := &fakeMarshalOnlyCmd{body: bytes.Repeat([]byte{0xAB}, 64), code: command.Code(9)}
	got2, err := encodeWireRequest(make([]byte, 0, 4), fb)
	if err != nil {
		t.Fatal(err)
	}
	if len(got2) != 8+len(fb.body) {
		t.Errorf("fallback grown len=%d, want %d", len(got2), 8+len(fb.body))
	}
}

func TestRequestBufPool_AcquireReleaseRoundTrip(t *testing.T) {
	bp := acquireRequestBuf()
	if bp == nil || *bp == nil {
		t.Fatalf("acquireRequestBuf returned nil")
	}
	// Use the buffer.
	*bp = append(*bp, []byte("hello")...)
	releaseRequestBuf(bp)

	// Next acquire may or may not return the same slice (sync.Pool semantics),
	// but the returned slice should be usable and zero-length.
	bp2 := acquireRequestBuf()
	if bp2 == nil || len(*bp2) != 0 {
		t.Fatalf("acquireRequestBuf returned bp=%p len=%d, want non-nil len=0", bp2, len(*bp2))
	}
	releaseRequestBuf(bp2)
}

func TestRequestBufPool_OversizedBufferDropped(t *testing.T) {
	// A buffer grown past the maxPooled threshold should NOT be put back.
	huge := make([]byte, 0, 128*1024)
	releaseRequestBuf(&huge)
	// Next acquire should give us a fresh small buffer, not the huge one.
	bp := acquireRequestBuf()
	if cap(*bp) > 64*1024 {
		t.Errorf("oversized buffer leaked into pool: cap=%d", cap(*bp))
	}
	releaseRequestBuf(bp)
}

func TestDo_PollMessagesEndToEnd(t *testing.T) {
	c, serverConn := newTestClient(t)
	cmd := newTestPollCmd()

	captured := make(chan []byte, 1)
	go func() {
		captured <- serverRespondCapture(t, serverConn, 0, nil)
	}()

	out, err := c.do(context.Background(), cmd)
	if err != nil {
		t.Fatalf("do returned err: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected empty response body, got %d bytes", len(out))
	}

	got := <-captured
	if got == nil {
		t.Fatalf("server side failed to receive request")
	}

	// The body the server saw must equal cmd.MarshalBinary().
	want, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	// First 4 bytes of `got` are the command code; the rest is the body.
	gotCode := command.Code(binary.LittleEndian.Uint32(got[:4]))
	if gotCode != cmd.Code() {
		t.Errorf("server saw code=%d, want %d", gotCode, cmd.Code())
	}
	if !bytes.Equal(got[4:], want) {
		t.Errorf("server saw body diverging from cmd.MarshalBinary()\nwant: %v\n got: %v", want, got[4:])
	}
}
