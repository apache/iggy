package tcp

import (
	binaryserialization "github.com/iggy-rs/iggy-go-client/binary_serialization"
	. "github.com/iggy-rs/iggy-go-client/contracts"
	ierror "github.com/iggy-rs/iggy-go-client/errors"
)

func (tms *IggyTcpClient) GetConsumerGroups(streamId, topicId Identifier) ([]ConsumerGroupResponse, error) {
	message := binaryserialization.SerializeIdentifiers(streamId, topicId)
	buffer, err := tms.sendAndFetchResponse(message, GetGroupsCode)
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeConsumerGroups(buffer), err
}

func (tms *IggyTcpClient) GetConsumerGroupById(streamId, topicId, groupId Identifier) (*ConsumerGroupResponse, error) {
	message := binaryserialization.SerializeIdentifiers(streamId, topicId, groupId)
	buffer, err := tms.sendAndFetchResponse(message, GetGroupCode)
	if err != nil {
		return nil, err
	}
	if len(buffer) == 0 {
		return nil, ierror.ConsumerGroupIdNotFound
	}

	return binaryserialization.DeserializeConsumerGroup(buffer)
}

func (tms *IggyTcpClient) CreateConsumerGroup(request CreateConsumerGroupRequest) error {
	if MaxStringLength < len(request.Name) {
		return ierror.TextTooLong("consumer_group_name")
	}
	message := binaryserialization.CreateGroup(request)
	_, err := tms.sendAndFetchResponse(message, CreateGroupCode)
	return err
}

func (tms *IggyTcpClient) DeleteConsumerGroup(request DeleteConsumerGroupRequest) error {
	message := binaryserialization.SerializeIdentifiers(request.StreamId, request.TopicId, request.ConsumerGroupId)
	_, err := tms.sendAndFetchResponse(message, DeleteGroupCode)
	return err
}

func (tms *IggyTcpClient) JoinConsumerGroup(request JoinConsumerGroupRequest) error {
	message := binaryserialization.SerializeIdentifiers(request.StreamId, request.TopicId, request.ConsumerGroupId)
	_, err := tms.sendAndFetchResponse(message, JoinGroupCode)
	return err
}

func (tms *IggyTcpClient) LeaveConsumerGroup(request LeaveConsumerGroupRequest) error {
	message := binaryserialization.SerializeIdentifiers(request.StreamId, request.TopicId, request.ConsumerGroupId)
	_, err := tms.sendAndFetchResponse(message, LeaveGroupCode)
	return err
}
