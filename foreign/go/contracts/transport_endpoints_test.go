package iggcon

import (
	"bytes"
	"reflect"
	"testing"
)

func TestTransportEndpoints_MatchLittleEndian(t *testing.T) {
	tsep := NewTransportEndpoints(8090, 8080, 3000, 8092)
	got, err := tsep.MarshalBinary()
	if err != nil {
		t.Fatalf("TransportEndpoints.MarshalBinary() error = %v", err)
	}
	want := []byte{0x9A, 0x1F, 0x90, 0x1F, 0xB8, 0x0B, 0x9C, 0x1F}
	if !bytes.Equal(got, want) {
		t.Errorf("TransportEndpoints.MatchLittleEndian() = %v, want %v", got, want)
	}
}

func TestTransportEndpoints_UnmarshalBinaryLengthShouldNotBeSmallerThanEight(t *testing.T) {
	tsep := NewTransportEndpoints(8090, 8080, 3000, 8092)
	ans1 := []byte{0x11, 0x45, 0x14, 0x19, 0x19, 0x81, 0x9C} // having length of seven, expected to return error, so we can pass the Errorf below, which is anticipated
	ans2 := []byte{}                                         // a zero-length byte slice, expected to return error, so we can pass the Errorf below, which is anticipated
	err1 := tsep.UnmarshalBinary(ans1)
	if err1 == nil {
		t.Errorf("UnmarshalBinary() expected an error for 7-byte input, got nil")
	}
	err2 := tsep.UnmarshalBinary(ans2)
	if err2 == nil {
		t.Errorf("UnmarshalBinary() expected an error for 0-byte input, got nil")
	}
}

func TestTransportEndpoints_UnmarshalBinaryAlwaysDecodesTheFirstEightBytes(t *testing.T) {
	want := NewTransportEndpoints(8090, 8080, 3000, 8092)
	input, err := want.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	input = append(input, 0xCA, 0xFE, 0xBA, 0xBE) // appending four extra bytes to the input slice, which should be ignored by UnmarshalBinary
	got := NewTransportEndpoints(0, 0, 0, 0)
	err = got.UnmarshalBinary(input)
	if err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("UnmarshalBinary() = %v, want %v", got, want)
	}
}

func TestTransportEndpoints_RoundTrip(t *testing.T) {
	original := NewTransportEndpoints(8090, 8080, 3000, 8092)
	if original.GetBufferSize() != 8 {
		t.Errorf("GetBufferSize() = %d, want %d", original.GetBufferSize(), 8)
	}

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	// data is anticipated to be 8 bytes long, so we can check that here
	if len(data) != 8 {
		t.Errorf("MarshalBinary() returned %d bytes, want 8", len(data))
	}

	restored := NewTransportEndpoints(0, 0, 0, 0)
	err = restored.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}
	if !reflect.DeepEqual(original, restored) {
		t.Errorf("UnmarshalBinary() = %v, want %v", restored, original)
	}
}
