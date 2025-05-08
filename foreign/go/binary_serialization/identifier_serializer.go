package binaryserialization

import (
	"encoding/binary"

	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
)

const (
	idKindOffset    = 0
	idLengthOffset  = 1
	stringIdLength  = 2
	numericIdLength = 4
)

func SerializeIdentifier(identifier iggcon.Identifier) []byte {
	bytes := make([]byte, int(identifier.Length)+2)
	bytes[idKindOffset] = byte(identifier.Kind)
	bytes[idLengthOffset] = byte(identifier.Length)

	if identifier.Kind == iggcon.StringId {
		valAsString := identifier.Value.(string)
		copy(bytes[stringIdLength:], []byte(valAsString))
	} else if identifier.Kind == iggcon.NumericId {
		valAsInt := identifier.Value.(int)
		binary.LittleEndian.PutUint32(bytes[stringIdLength:stringIdLength+numericIdLength], uint32(valAsInt))
	}
	return bytes
}

func SerializeIdentifiers(identifiers ...iggcon.Identifier) []byte {
	size := 0
	for i := 0; i < len(identifiers); i++ {
		size += 2 + identifiers[i].Length
	}
	bytes := make([]byte, size)
	position := 0

	for i := 0; i < len(identifiers); i++ {
		copy(bytes[position:position+2+identifiers[i].Length], SerializeIdentifier(identifiers[i]))
		position += 2 + identifiers[i].Length
	}

	return bytes
}
