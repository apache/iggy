package iggcon

import (
	"github.com/google/uuid"
)

type FetchMessagesRequest struct {
	StreamId        Identifier      `json:"streamId"`
	TopicId         Identifier      `json:"topicId"`
	Consumer        Consumer        `json:"consumer"`
	PartitionId     int             `json:"partitionId"`
	PollingStrategy PollingStrategy `json:"pollingStrategy"`
	Count           int             `json:"count"`
	AutoCommit      bool            `json:"autoCommit"`
}

type FetchMessagesResponse struct {
	PartitionId   int
	CurrentOffset uint64
	Messages      []MessageResponse
	MessageCount  int
}

type MessageResponse struct {
	Offset    uint64                    `json:"offset"`
	Timestamp uint64                    `json:"timestamp"`
	Checksum  uint32                    `json:"checksum"`
	Id        uuid.UUID                 `json:"id"`
	Payload   []byte                    `json:"payload"`
	Headers   map[HeaderKey]HeaderValue `json:"headers,omitempty"`
	State     MessageState              `json:"state"`
}

type MessageState int

const (
	MessageStateAvailable MessageState = iota
	MessageStateUnavailable
	MessageStatePoisoned
	MessageStateMarkedForDeletion
)

type SendMessagesRequest struct {
	StreamId     Identifier   `json:"streamId"`
	TopicId      Identifier   `json:"topicId"`
	Partitioning Partitioning `json:"partitioning"`
	Messages     []Message    `json:"messages"`
}

type Message struct {
	Id      uuid.UUID
	Payload []byte
	Headers map[HeaderKey]HeaderValue
}

func NewMessage(payload []byte, headers map[HeaderKey]HeaderValue) Message {
	return Message{
		Id:      uuid.New(),
		Payload: payload,
		Headers: headers,
	}
}
