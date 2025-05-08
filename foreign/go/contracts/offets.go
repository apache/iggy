package iggcon

type StoreOffsetRequest struct {
	StreamId    Identifier `json:"streamId"`
	TopicId     Identifier `json:"topicId"`
	Consumer    Consumer   `json:"consumer"`
	PartitionId int        `json:"partitionId"`
	Offset      uint64     `json:"offset"`
}

type GetOffsetRequest struct {
	StreamId    Identifier `json:"streamId"`
	TopicId     Identifier `json:"topicId"`
	Consumer    Consumer   `json:"consumer"`
	PartitionId int        `json:"partitionId"`
}

type OffsetResponse struct {
	PartitionId   int    `json:"partitionId"`
	CurrentOffset uint64 `json:"currentOffset"`
	StoredOffset  uint64 `json:"storedOffset"`
}
