package binaryserialization

import (
	"encoding/binary"
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
)

const (
	partitionStrategySize = 5
	offsetSize            = 12
	commitFlagSize        = 1
)

type TcpFetchMessagesRequest struct {
	iggcon.FetchMessagesRequest
}

func (request *TcpFetchMessagesRequest) Serialize() []byte {
	streamTopicIdLength := 2 + request.StreamId.Length + 2 + request.TopicId.Length
	messageSize := 2 + request.Consumer.Id.Length + streamTopicIdLength + partitionStrategySize + offsetSize + commitFlagSize + 1
	bytes := make([]byte, messageSize)

	bytes[0] = byte(request.Consumer.Kind)
	copy(bytes[1:3+request.Consumer.Id.Length], SerializeIdentifier(request.Consumer.Id))

	position := 3 + request.Consumer.Id.Length

	copy(bytes[position:position+streamTopicIdLength], SerializeIdentifiers(request.StreamId, request.TopicId))

	position += streamTopicIdLength
	binary.LittleEndian.PutUint32(bytes[position:position+4], uint32(request.PartitionId))
	bytes[position+4] = byte(request.PollingStrategy.Kind)

	position += partitionStrategySize
	binary.LittleEndian.PutUint64(bytes[position:position+8], uint64(request.PollingStrategy.Value))
	binary.LittleEndian.PutUint32(bytes[position+8:position+12], uint32(request.Count))

	position += offsetSize

	if request.AutoCommit {
		bytes[position] = 1
	} else {
		bytes[position] = 0
	}

	return bytes
}
