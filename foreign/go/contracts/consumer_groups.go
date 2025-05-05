package iggcon

type ConsumerGroupResponse struct {
	Id              int    `json:"id"`
	Name            string `json:"name"`
	PartitionsCount int    `json:"partitionsCount"`
	MembersCount    int    `json:"membersCount"`
}

type CreateConsumerGroupRequest struct {
	StreamId        Identifier `json:"streamId"`
	TopicId         Identifier `json:"topicId"`
	ConsumerGroupId int        `json:"consumerGroupId"`
	Name            string     `json:"name"`
}

type DeleteConsumerGroupRequest struct {
	StreamId        Identifier `json:"streamId"`
	TopicId         Identifier `json:"topicId"`
	ConsumerGroupId Identifier `json:"consumerGroupId"`
}

type JoinConsumerGroupRequest struct {
	StreamId        Identifier `json:"streamId"`
	TopicId         Identifier `json:"topicId"`
	ConsumerGroupId Identifier `json:"consumerGroupId"`
}

type LeaveConsumerGroupRequest struct {
	StreamId        Identifier `json:"streamId"`
	TopicId         Identifier `json:"topicId"`
	ConsumerGroupId Identifier `json:"consumerGroupId"`
}

type ConsumerGroupInfo struct {
	StreamId        int `json:"streamId"`
	TopicId         int `json:"topicId"`
	ConsumerGroupId int `json:"consumerGroupId"`
}
