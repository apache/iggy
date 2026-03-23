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

package codec

import (
	"bytes"
	"math"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// TestWriter_writes verifies that every write method encodes the expected bytes.
func TestWriter_writes(t *testing.T) {
	cases := []struct {
		name  string
		write func(*Writer)
		want  []byte
	}{
		{"U8", func(w *Writer) { w.U8(0xAB) }, []byte{0xAB}},
		{"U16", func(w *Writer) { w.U16(0x0102) }, []byte{0x02, 0x01}},
		{"U32", func(w *Writer) { w.U32(0x01020304) }, []byte{0x04, 0x03, 0x02, 0x01}},
		{"U64", func(w *Writer) { w.U64(0x0102030405060708) }, []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}},
		{"F32", func(w *Writer) { w.F32(1.0) }, []byte{0x00, 0x00, 0x80, 0x3F}},
		{"Str", func(w *Writer) { w.Str("ab") }, []byte{'a', 'b'}},
		{"U32LenStr", func(w *Writer) { w.U32LenStr("ab") }, []byte{0x02, 0x00, 0x00, 0x00, 'a', 'b'}},
		{"U8LenStr", func(w *Writer) { w.U8LenStr("ab") }, []byte{0x02, 'a', 'b'}},
		{"Raw", func(w *Writer) { w.Raw([]byte{0xDE, 0xAD}) }, []byte{0xDE, 0xAD}},
		{"Obj", func(w *Writer) { w.Obj(&testPoint{1, 2}) }, []byte{0x01, 0x02}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := NewWriter()
			tc.write(w)
			if err := w.Err(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := w.Bytes(); !bytes.Equal(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestWriter_errSentinel verifies that once an error is set, all subsequent
// write methods are no-ops and the buffer remains empty.
func TestWriter_errSentinel(t *testing.T) {
	w := NewWriter()
	w.Obj(errMarshaler{})
	if w.Err() == nil {
		t.Fatal("expected error after errMarshaler, got nil")
	}
	err := w.Err()
	w.U8(1)
	w.U16(2)
	w.U32(3)
	w.U64(4)
	w.F32(5)
	w.Str("x")
	w.U32LenStr("y")
	w.U8LenStr("z")
	w.Raw([]byte{0xFF})
	w.Obj(&testPoint{1, 2})
	if got := w.Bytes(); len(got) != 0 {
		t.Errorf("expected empty buffer after error, got %d bytes: %v", len(got), got)
	}
	if w.Err() != err {
		t.Errorf("error was overwritten: got %v, want %v", w.Err(), err)
	}
}

// TestWriter_U8LenStr_overflow verifies that U8LenStr sets w.err for strings
// longer than 255 bytes.
func TestWriter_U8LenStr_overflow(t *testing.T) {
	w := NewWriter()
	_, file, line, _ := runtime.Caller(0)
	w.U8LenStr(strings.Repeat("a", math.MaxUint8+1))
	err := w.Err()
	if err == nil {
		t.Fatal("expected error for string > 255 bytes, got nil")
	}
	re := regexp.MustCompile(`^string length (\d+) exceeds 255`)
	if !re.MatchString(err.Error()) {
		t.Errorf("unexpected error message: %v", err)
	}
	checkLoc(t, err, file, line+1)
}

// TestWriterCap_noAlloc verifies that NewWriterCap avoids
// reallocations when the provided capacity is sufficient.
func TestWriterCap_noAlloc(t *testing.T) {
	const n = 4 + 1 + len("name") // U32 + U8LenStr("name")
	w := NewWriterCap(n)
	capBefore := cap(w.p)
	w.U32(42)
	w.U8LenStr("name")
	if cap(w.p) != capBefore {
		t.Errorf("reallocation occurred: cap before=%d, after=%d", capBefore, cap(w.p))
	}
	if len(w.p) != n {
		t.Errorf("unexpected length: got %d, want %d", len(w.p), n)
	}
}

// TestWriter_Obj_error_location verifies that the error message contains
// the file and line of the call site that trigger the error.
func TestWriter_Obj_error_location(t *testing.T) {
	w := NewWriter()
	_, file, line, _ := runtime.Caller(0)
	w.Obj(&errMarshaler{})
	checkLoc(t, w.Err(), file, line+1)
}
