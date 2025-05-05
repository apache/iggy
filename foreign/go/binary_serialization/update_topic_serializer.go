package binaryserialization

import (
	"encoding/binary"

	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
)

type TcpUpdateTopicRequest struct {
	iggcon.UpdateTopicRequest
}

func (request *TcpUpdateTopicRequest) Serialize() []byte {
	streamIdBytes := SerializeIdentifier(request.StreamId)
	topicIdBytes := SerializeIdentifier(request.TopicId)

	buffer := make([]byte, 19+len(streamIdBytes)+len(topicIdBytes)+len(request.Name))

	offset := 0

	offset += copy(buffer[offset:], streamIdBytes)
	offset += copy(buffer[offset:], topicIdBytes)

	buffer[offset] = request.CompressionAlgorithm
	offset++

	binary.LittleEndian.PutUint64(buffer[offset:], uint64(request.MessageExpiry.Microseconds()))
	offset += 8

	binary.LittleEndian.PutUint64(buffer[offset:], uint64(request.MaxTopicSize))
	offset += 8

	buffer[offset] = request.ReplicationFactor
	offset++

	buffer[offset] = uint8(len(request.Name))
	offset++

	copy(buffer[offset:], request.Name)

	return buffer
}
