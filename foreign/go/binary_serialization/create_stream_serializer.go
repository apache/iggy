package binaryserialization

import (
	"encoding/binary"
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
)

type TcpCreateStreamRequest struct {
	iggcon.CreateStreamRequest
}

const (
	streamIDOffset   = 0
	nameLengthOffset = 4
	payloadOffset    = 5
)

func (request *TcpCreateStreamRequest) Serialize() []byte {
	nameLength := len(request.Name)
	serialized := make([]byte, payloadOffset+nameLength)

	binary.LittleEndian.PutUint32(serialized[streamIDOffset:], uint32(request.StreamId))
	serialized[nameLengthOffset] = byte(nameLength)
	copy(serialized[payloadOffset:], []byte(request.Name))

	return serialized
}
