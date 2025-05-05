package binaryserialization

import (
	"testing"

	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
)

func TestSerializeIdentifier_StringId(t *testing.T) {
	// Test case for StringId
	identifier := iggcon.NewIdentifier("Hello")

	// Serialize the identifier
	serialized := SerializeIdentifier(identifier)

	// Expected serialized bytes for StringId
	expected := []byte{
		0x02,                         // Kind (StringId)
		0x05,                         // Length (5)
		0x48, 0x65, 0x6C, 0x6C, 0x6F, // Value ("Hello")
	}

	// Check if the serialized bytes match the expected bytes
	if !areBytesEqual(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect for StringId. \nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

func TestSerializeIdentifier_NumericId(t *testing.T) {
	// Test case for NumericId
	identifier := iggcon.NewIdentifier(123)

	// Serialize the identifier
	serialized := SerializeIdentifier(identifier)

	// Expected serialized bytes for NumericId
	expected := []byte{
		0x01,                   // Kind (NumericId)
		0x04,                   // Length (4)
		0x7B, 0x00, 0x00, 0x00, // Value (123)
	}

	// Check if the serialized bytes match the expected bytes
	if !areBytesEqual(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect for NumericId. \nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

func TestSerializeIdentifier_EmptyStringId(t *testing.T) {
	// Test case for an empty StringId
	identifier := iggcon.NewIdentifier("")

	// Serialize the identifier
	serialized := SerializeIdentifier(identifier)

	// Expected serialized bytes for an empty StringId
	expected := []byte{
		0x02, // Kind (StringId)
		0x00, // Length (0)
	}

	// Check if the serialized bytes match the expected bytes
	if !areBytesEqual(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect for an empty StringId. \nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}
