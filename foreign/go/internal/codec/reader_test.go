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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"testing"
)

// --- byte-construction helpers ---

func u16le(v uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, v)
	return b
}

func u32le(v uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return b
}

func u64le(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

func cat(slices ...[]byte) []byte {
	var out []byte
	for _, s := range slices {
		out = append(out, s...)
	}
	return out
}

// testPoint is a simple type that implements BinaryMarshaler/BinaryUnmarshaler
// as [x, y] two-byte encoding.
type testPoint struct {
	x, y uint8
}

func (p *testPoint) MarshalBinary() ([]byte, error) {
	return []byte{p.x, p.y}, nil
}

func (p *testPoint) UnmarshalBinary(b []byte) error {
	if len(b) < 2 {
		return fmt.Errorf("testPoint: need 2 bytes, got %d", len(b))
	}
	p.x = b[0]
	p.y = b[1]
	return nil
}

// errMarshaler always returns an error from MarshalBinary.
type errMarshaler struct{}

func (errMarshaler) MarshalBinary() ([]byte, error) {
	return nil, errors.New("marshal error")
}

// errUnmarshaler always returns an error from UnmarshalBinary.
type errUnmarshaler struct{}

func (e *errUnmarshaler) UnmarshalBinary(_ []byte) error {
	return errors.New("unmarshal error")
}

// TestReader_reads exercises every read method in sequence.
func TestReader_reads(t *testing.T) {
	const wantU8 uint8 = math.MaxUint8
	const wantU16 uint16 = math.MaxUint16
	const wantU32 uint32 = math.MaxUint32
	const wantU64 uint64 = math.MaxUint64
	const wantF32 float32 = math.Pi
	const wantRem = 1

	wantStr := "str"
	wantU32LenStr := "uint32"
	wantU8LenStr := "uint8"
	wantRaw := []byte{0xDE, 0xAD}
	wantObj := testPoint{1, 2}

	payload := cat(
		[]byte{wantU8},                                           // U8
		u16le(wantU16),                                           // U16
		u32le(wantU32),                                           // U32
		u64le(wantU64),                                           // U64
		u32le(math.Float32bits(wantF32)),                         // F32
		[]byte(wantStr),                                          // Str(len(wantStr))
		u32le(uint32(len(wantU32LenStr))), []byte(wantU32LenStr), // U32LenStr
		[]byte{uint8(len(wantU8LenStr))}, []byte(wantU8LenStr), // U8LenStr
		wantRaw,      // Raw(len(wantRaw))
		[]byte{1, 2}, // Obj(testPoint{1, 2})
		[]byte{0xFF}, // wantRem trailing bytes for Remaining()
	)

	r := NewReader(payload)
	u8 := r.U8()
	u16 := r.U16()
	u32 := r.U32()
	u64 := r.U64()
	f32 := r.F32()
	str := r.Str(len(wantStr))
	u32LenStr := r.U32LenStr()
	u8LenStr := r.U8LenStr()
	raw := r.Raw(len(wantRaw))
	var obj testPoint
	r.Obj(2, &obj)
	rem := r.Remaining()
	if err := r.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if u8 != wantU8 {
		t.Errorf("U8: got %#x, want %#x", u8, wantU8)
	}
	if u16 != wantU16 {
		t.Errorf("U16: got %#x, want %#x", u16, wantU16)
	}
	if u32 != wantU32 {
		t.Errorf("U32: got %#x, want %#x", u32, wantU32)
	}
	if u64 != wantU64 {
		t.Errorf("U64: got %#x, want %#x", u64, wantU64)
	}
	if f32 != wantF32 {
		t.Errorf("F32: got %v, want %v", f32, wantF32)
	}
	if str != wantStr {
		t.Errorf("Str: got %q, want %q", str, wantStr)
	}
	if u32LenStr != wantU32LenStr {
		t.Errorf("U32LenStr: got %q, want %q", u32LenStr, wantU32LenStr)
	}
	if u8LenStr != wantU8LenStr {
		t.Errorf("U8LenStr: got %q, want %q", u8LenStr, wantU8LenStr)
	}
	if !bytes.Equal(raw, wantRaw) {
		t.Errorf("Raw: got %v, want %v", raw, wantRaw)
	}
	if obj != wantObj {
		t.Errorf("Obj: got %v, want %v", obj, wantObj)
	}
	if rem != wantRem {
		t.Errorf("Remaining: got %d, want %d", rem, wantRem)
	}
}

// TestReader_truncation verifies that every read method returns a descriptive error
// when the buffer is too short, including mid-sequence truncation.
func TestReader_truncation(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		read    func(*Reader)
	}{
		{"U8", []byte{}, func(r *Reader) { r.U8() }},
		{"U16", []byte{0x01}, func(r *Reader) { r.U16() }},                   // 1 byte, need 2
		{"U32", []byte{0x01, 0x02, 0x03}, func(r *Reader) { r.U32() }},       // 3 bytes, need 4
		{"U64", []byte{0x01, 0x02, 0x03, 0x04}, func(r *Reader) { r.U64() }}, // 4 bytes, need 8
		{"F32", []byte{0x01, 0x02, 0x03}, func(r *Reader) { r.F32() }},       // 3 bytes, need 4
		{"Str", []byte("hi"), func(r *Reader) { r.Str(5) }},                  // claims 5, has 2
		{"Raw", []byte("hi"), func(r *Reader) { r.Raw(5) }},                  // claims 5, has 2
		{"Obj", []byte{1}, func(r *Reader) {
			var p testPoint
			r.Obj(2, &p)
		}},
		{"U32LenStr/short-len-prefix", []byte{0x05, 0x00}, func(r *Reader) { r.U32LenStr() }},        // len prefix needs 4 bytes, got 2
		{"U32LenStr/short-body", cat(u32le(10), []byte("short")), func(r *Reader) { r.U32LenStr() }}, // claims 10, has 5
		{"U8LenStr/short-len-prefix", []byte{}, func(r *Reader) { r.U8LenStr() }},                    // len prefix needs 1 byte, got 0
		{"U8LenStr/short-body", cat([]byte{10}, []byte("short")), func(r *Reader) { r.U8LenStr() }},  // claims 10, has 5
		{"mid-sequence", cat(u32le(1), []byte{0xFF}), func(r *Reader) { r.U32(); r.U32() }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewReader(tc.payload)
			tc.read(r)
			err := r.Err()
			if err == nil || !strings.HasPrefix(err.Error(), "reader: need ") {
				t.Fatalf("got %v, want overrun error", err)
			}
		})
	}
}

// TestReader_errSentinel verifies that once an error is set, all subsequent
// read methods are no-ops and return zero values without overwriting the error.
func TestReader_errSentinel(t *testing.T) {
	r := NewReader([]byte{})
	r.U8() // triggers overrun, sets r.err
	if r.Err() == nil {
		t.Fatal("expected error after overrun, got nil")
	}
	err := r.Err()

	if v := r.U8(); v != 0 {
		t.Errorf("U8: got %v, want 0", v)
	}
	if v := r.U16(); v != 0 {
		t.Errorf("U16: got %v, want 0", v)
	}
	if v := r.U32(); v != 0 {
		t.Errorf("U32: got %v, want 0", v)
	}
	if v := r.U64(); v != 0 {
		t.Errorf("U64: got %v, want 0", v)
	}
	if v := r.F32(); v != 0 {
		t.Errorf("F32: got %v, want 0", v)
	}
	if v := r.Str(1); v != "" {
		t.Errorf("Str: got %q, want empty", v)
	}
	if v := r.Raw(1); v != nil {
		t.Errorf("Raw: got %v, want nil", v)
	}
	if v := r.U32LenStr(); v != "" {
		t.Errorf("U32LenStr: got %q, want empty", v)
	}
	if v := r.U8LenStr(); v != "" {
		t.Errorf("U8LenStr: got %q, want empty", v)
	}
	var p testPoint
	r.Obj(2, &p)
	if p != (testPoint{}) {
		t.Errorf("Obj: got %v, want zero value", p)
	}
	if r.Err() != err {
		t.Errorf("error was overwritten: got %v, want %v", r.Err(), err)
	}
}

// TestReader_Obj_unmarshalError verifies that Reader.Obj propagates an error
// returned by UnmarshalBinary.
func TestReader_Obj_unmarshalError(t *testing.T) {
	r := NewReader([]byte{1, 2})
	_, file, line, _ := runtime.Caller(0)
	r.Obj(2, &errUnmarshaler{})
	checkLoc(t, r.Err(), file, line+1)
}

// TestReader_overrun_error_location verifies that the error message contains
// the file and line of the call site that triggered the overrun, for every
// public read method.
func TestReader_overrun_error_location(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		fn      func(r *Reader) (wantFile string, wantLine int)
	}{
		{"U8", []byte{}, func(r *Reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.U8()
			return file, line + 1
		}},
		{"U16", []byte{}, func(r *Reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.U16()
			return file, line + 1
		}},
		{"U32", []byte{}, func(r *Reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.U32()
			return file, line + 1
		}},
		{"U64", []byte{}, func(r *Reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.U64()
			return file, line + 1
		}},
		{"F32", []byte{}, func(r *Reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.F32()
			return file, line + 1
		}},
		{"Str", []byte{}, func(r *Reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.Str(1)
			return file, line + 1
		}},
		{"Raw", []byte{}, func(r *Reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.Raw(1)
			return file, line + 1
		}},
		{"Obj", []byte{1}, func(r *Reader) (string, int) {
			var p testPoint
			_, file, line, _ := runtime.Caller(0)
			r.Obj(2, &p)
			return file, line + 1
		}},
		{"U32LenStr/prefix", []byte{}, func(r *Reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.U32LenStr()
			return file, line + 1
		}},
		{"U32LenStr/body", cat(u32le(100), []byte("short")), func(r *Reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.U32LenStr()
			return file, line + 1
		}},
		{"U8LenStr/prefix", []byte{}, func(r *Reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.U8LenStr()
			return file, line + 1
		}},
		{"U8LenStr/body", cat([]byte{100}, []byte("short")), func(r *Reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.U8LenStr()
			return file, line + 1
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewReader(tc.payload)
			wantFile, wantLine := tc.fn(r)
			checkLoc(t, r.Err(), wantFile, wantLine)
		})
	}
}

func checkLoc(t *testing.T, err error, wantFile string, wantLine int) {
	t.Helper()
	if err == nil {
		t.Error("expected error, got nil")
		return
	}
	wantLoc := fmt.Sprintf("%s:%d", wantFile, wantLine)
	if !strings.Contains(err.Error(), wantLoc) {
		t.Errorf("error %q does not contain location %q", err.Error(), wantLoc)
	}
}
