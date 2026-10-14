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
	"errors"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// marshalOnlyCmd implements command.Command without AppendBinary; it forces
// appendCommandFrame down the MarshalBinary fallback branch.
type marshalOnlyCmd struct {
	body    []byte
	code    command.Code
	wantErr error
}

func (f *marshalOnlyCmd) Code() command.Code { return f.code }
func (f *marshalOnlyCmd) MarshalBinary() ([]byte, error) {
	if f.wantErr != nil {
		return nil, f.wantErr
	}
	return bytes.Clone(f.body), nil
}

func testPollCmd(t *testing.T) *command.PollMessages {
	t.Helper()
	partitionId := uint32(7)
	return &command.PollMessages{
		Consumer:    iggcon.NewSingleConsumer(numericIdentifier(t, 42)),
		StreamId:    numericIdentifier(t, 1),
		TopicId:     numericIdentifier(t, 2),
		PartitionId: &partitionId,
		Strategy:    iggcon.FirstPollingStrategy(),
		Count:       100,
		AutoCommit:  true,
	}
}

func TestAppendCommandFrame_AppenderPathMatchesTheFallback(t *testing.T) {
	cmd := testPollCmd(t)
	body, err := cmd.MarshalBinary()
	require.NoError(t, err)
	fallback := &marshalOnlyCmd{body: body, code: cmd.Code()}

	want, err := appendCommandFrame(nil, fallback)
	require.NoError(t, err)
	got, err := appendCommandFrame(nil, cmd)
	require.NoError(t, err)

	assert.Equal(t, want[vsr.HeaderSize:], got[vsr.HeaderSize:],
		"the fast path diverges from the MarshalBinary fallback")
	assert.Len(t, got, vsr.HeaderSize+len(body),
		"the frame is the reserved prologue followed by the payload")
}

func TestAppendCommandFrame_FallbackPropagatesTheMarshalError(t *testing.T) {
	sentinel := errors.New("marshal failed")
	cmd := &marshalOnlyCmd{code: command.Code(1), wantErr: sentinel}

	frame, err := appendCommandFrame(make([]byte, 0, 4), cmd)
	assert.ErrorIs(t, err, sentinel)
	assert.NotNil(t, frame, "the grown buffer survives the error so the pool keeps it")
}

func TestAppendCommandFrame_GrowsAnUndersizedBuffer(t *testing.T) {
	cmd := testPollCmd(t)
	want, err := appendCommandFrame(make([]byte, 0, 4096), cmd)
	require.NoError(t, err)

	got, err := appendCommandFrame(make([]byte, 0, 4), cmd)
	require.NoError(t, err)
	assert.Equal(t, want[vsr.HeaderSize:], got[vsr.HeaderSize:])
}

func TestRequestBufPool_AcquireReleaseRoundTrip(t *testing.T) {
	bp := acquireRequestBuf()
	require.NotNil(t, bp)
	require.NotNil(t, *bp)
	assert.GreaterOrEqual(t, cap(*bp), vsr.HeaderSize,
		"a fresh buffer already holds the header prologue")

	*bp = append(*bp, []byte("hello")...)
	releaseRequestBuf(bp)

	next := acquireRequestBuf()
	require.NotNil(t, next)
	assert.Empty(t, *next, "a pooled buffer comes back empty")
	releaseRequestBuf(next)
}

func TestRequestBufPool_KeepsABatchSizedBuffer(t *testing.T) {
	batch := make([]byte, 0, 1<<20)
	releaseRequestBuf(&batch)
	// sync.Pool gives no delivery guarantee; what must hold is that the
	// release accepted the megabyte buffer instead of dropping it on the
	// floor, which the truncation observes.
	assert.Empty(t, batch)
}

func TestRequestBufPool_DropsAPathologicalBuffer(t *testing.T) {
	huge := make([]byte, 0, 8<<20)
	huge = append(huge, 1)
	releaseRequestBuf(&huge)
	assert.Len(t, huge, 1, "an over-ceiling buffer is not truncated, it is dropped")
}
