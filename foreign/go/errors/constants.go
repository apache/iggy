package ierror

var (
	StreamIdNotFound = &IggyError{
		Code:    1009,
		Message: "stream_id_not_found",
	}
	TopicIdNotFound = &IggyError{
		Code:    2010,
		Message: "topic_id_not_found",
	}
	ConsumerGroupIdNotFound = &IggyError{
		Code:    5000,
		Message: "consumer_group_not_found",
	}
	ResourceNotFound = &IggyError{
		Code:    20,
		Message: "resource_not_found",
	}
)
