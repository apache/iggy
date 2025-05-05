package iggcon

type CreateStreamRequest struct {
	StreamId int    `json:"streamId"`
	Name     string `json:"name"`
}

type UpdateStreamRequest struct {
	StreamId Identifier `json:"streamId"`
	Name     string     `json:"name"`
}

type StreamResponse struct {
	Id            int             `json:"id"`
	Name          string          `json:"name"`
	SizeBytes     uint64          `json:"sizeBytes"`
	CreatedAt     uint64          `json:"createdAt"`
	MessagesCount uint64          `json:"messagesCount"`
	TopicsCount   int             `json:"topicsCount"`
	Topics        []TopicResponse `json:"topics,omitempty"`
}

type GetStreamRequest struct {
	StreamID Identifier
}
