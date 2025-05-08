package iggcon

import (
	"encoding/binary"
	"errors"

	"github.com/google/uuid"
)

type PartitionContract struct {
	Id            int    `json:"id"`
	MessagesCount uint64 `json:"messagesCount"`
	CreatedAt     uint64 `json:"createdAt"`
	SegmentsCount int    `json:"segmentsCount"`
	CurrentOffset uint64 `json:"currentOffset"`
	SizeBytes     uint64 `json:"sizeBytes"`
}

type CreatePartitionsRequest struct {
	StreamId        Identifier `json:"streamId"`
	TopicId         Identifier `json:"topicId"`
	PartitionsCount int        `json:"partitionsCount"`
}

type DeletePartitionRequest struct {
	StreamId        Identifier `json:"streamId"`
	TopicId         Identifier `json:"topicId"`
	PartitionsCount int        `json:"partitionsCount"`
}

type PartitioningKind int

const (
	Balanced        PartitioningKind = 1
	PartitionIdKind                  = 2
	MessageKey                       = 3
)

type Partitioning struct {
	Kind   PartitioningKind
	Length int
	Value  []byte
}

func None() Partitioning {
	return Partitioning{
		Kind:   Balanced,
		Length: 0,
		Value:  make([]byte, 0),
	}
}

func PartitionId(value int) Partitioning {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, uint32(value))

	return Partitioning{
		Kind:   PartitionIdKind,
		Length: 4,
		Value:  bytes,
	}
}

func EntityIdString(value string) (Partitioning, error) {
	if len(value) == 0 || len(value) > 255 {
		return Partitioning{}, errors.New("Value has incorrect size, must be between 1 and 255")
	}

	return Partitioning{
		Kind:   MessageKey,
		Length: len(value),
		Value:  []byte(value),
	}, nil
}

func EntityIdBytes(value []byte) (Partitioning, error) {
	if len(value) == 0 || len(value) > 255 {
		return Partitioning{}, errors.New("Value has incorrect size, must be between 1 and 255")
	}

	return Partitioning{
		Kind:   MessageKey,
		Length: len(value),
		Value:  value,
	}, nil
}

func EntityIdInt(value int) Partitioning {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, uint32(value))
	return Partitioning{
		Kind:   MessageKey,
		Length: 4,
		Value:  bytes,
	}
}

func EntityIdUlong(value uint64) Partitioning {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, value)
	return Partitioning{
		Kind:   MessageKey,
		Length: 8,
		Value:  bytes,
	}
}

func EntityIdGuid(value uuid.UUID) Partitioning {
	bytes := value[:]
	return Partitioning{
		Kind:   MessageKey,
		Length: len(bytes),
		Value:  bytes,
	}
}
