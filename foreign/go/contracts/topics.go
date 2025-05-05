package iggcon

import "time"

type CreateTopicRequest struct {
	StreamId             Identifier    `json:"streamId"`
	TopicId              int           `json:"topicId"`
	PartitionsCount      int           `json:"partitionsCount"`
	CompressionAlgorithm uint8         `json:"compressionAlgorithm"`
	MessageExpiry        time.Duration `json:"messageExpiry"`
	MaxTopicSize         uint64        `json:"maxTopicSize"`
	ReplicationFactor    uint8         `json:"replicationFactor"`
	Name                 string        `json:"name"`
}

type UpdateTopicRequest struct {
	StreamId             Identifier    `json:"streamId"`
	TopicId              Identifier    `json:"topicId"`
	CompressionAlgorithm uint8         `json:"compressionAlgorithm"`
	MessageExpiry        time.Duration `json:"messageExpiry"`
	MaxTopicSize         uint64        `json:"maxTopicSize"`
	ReplicationFactor    uint8         `json:"replicationFactor"`
	Name                 string        `json:"name"`
}

type TopicResponse struct {
	Id                   int                 `json:"id"`
	CreatedAt            int                 `json:"createdAt"`
	Name                 string              `json:"name"`
	SizeBytes            uint64              `json:"sizeBytes"`
	MessageExpiry        time.Duration       `json:"messageExpiry"`
	CompressionAlgorithm uint8               `json:"compressionAlgorithm"`
	MaxTopicSize         uint64              `json:"maxTopicSize"`
	ReplicationFactor    uint8               `json:"replicationFactor"`
	MessagesCount        uint64              `json:"messagesCount"`
	PartitionsCount      int                 `json:"partitionsCount"`
	Partitions           []PartitionContract `json:"partitions,omitempty"`
}
