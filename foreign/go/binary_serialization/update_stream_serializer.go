package binaryserialization

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
)

type TcpUpdateStreamRequest struct {
	iggcon.UpdateStreamRequest
}

func (request *TcpUpdateStreamRequest) Serialize() []byte {
	nameLength := len(request.Name)
	bytes := make([]byte, nameLength+request.StreamId.Length+3)
	copy(bytes[0:2+request.StreamId.Length], SerializeIdentifier(request.StreamId))
	position := 2 + request.StreamId.Length
	bytes[position] = byte(nameLength)
	copy(bytes[position+1:], []byte(request.Name))
	return bytes
}
