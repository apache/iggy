package tcp_test

import (
	"strconv"

	"github.com/iggy-rs/iggy-go-client"
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// operations
func successfullyCreateConsumer(streamId int, topicId int, client iggy.MessageStream) (int, string) {
	request := iggcon.CreateConsumerGroupRequest{
		StreamId:        iggcon.NewIdentifier(streamId),
		TopicId:         iggcon.NewIdentifier(topicId),
		ConsumerGroupId: int(createRandomUInt32()),
		Name:            createRandomString(16),
	}
	err := client.CreateConsumerGroup(request)

	itShouldSuccessfullyCreateConsumer(streamId, topicId, request.ConsumerGroupId, request.Name, client)
	itShouldNotReturnError(err)
	return request.ConsumerGroupId, request.Name
}

func successfullyJoinConsumer(streamId int, topicId int, groupId int, client iggy.MessageStream) {
	request := iggcon.JoinConsumerGroupRequest{
		StreamId:        iggcon.NewIdentifier(streamId),
		TopicId:         iggcon.NewIdentifier(topicId),
		ConsumerGroupId: iggcon.NewIdentifier(groupId),
	}
	err := client.JoinConsumerGroup(request)

	itShouldSuccessfullyJoinConsumer(streamId, topicId, groupId, client)
	itShouldNotReturnError(err)
}

//assertions

func itShouldReturnSpecificConsumer(id int, name string, consumer *iggcon.ConsumerGroupResponse) {
	It("should fetch consumer with id "+string(rune(id)), func() {
		Expect(consumer).NotTo(BeNil())
		Expect(consumer.Id).To(Equal(id))
	})

	It("should fetch consumer with name "+name, func() {
		Expect(consumer).NotTo(BeNil())
		Expect(consumer.Name).To(Equal(name))
	})
}

func itShouldContainSpecificConsumer(id int, name string, consumers []iggcon.ConsumerGroupResponse) {
	It("should fetch at least one consumer", func() {
		Expect(len(consumers)).NotTo(Equal(0))
	})

	var consumer iggcon.ConsumerGroupResponse
	found := false

	for _, s := range consumers {
		if s.Id == id && s.Name == name {
			consumer = s
			found = true
			break
		}
	}

	It("should fetch consumer with id "+strconv.Itoa(id), func() {
		Expect(found).To(BeTrue(), "Consumer with id %d and name %s not found", id, name)
		Expect(consumer.Id).To(Equal(id))
	})

	It("should fetch consumer with name "+name, func() {
		Expect(found).To(BeTrue(), "Consumer with id %d and name %s not found", id, name)
		Expect(consumer.Name).To(Equal(name))
	})
}

func itShouldSuccessfullyCreateConsumer(streamId int, topicId int, groupId int, expectedName string, client iggy.MessageStream) {
	consumer, err := client.GetConsumerGroupById(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId), iggcon.NewIdentifier(groupId))

	It("should create consumer with id "+string(rune(groupId)), func() {
		Expect(consumer).NotTo(BeNil())
		Expect(consumer.Id).To(Equal(groupId))
	})

	It("should create consumer with name "+expectedName, func() {
		Expect(consumer).NotTo(BeNil())
		Expect(consumer.Name).To(Equal(expectedName))
	})
	itShouldNotReturnError(err)
}

func itShouldSuccessfullyDeletedConsumer(streamId int, topicId int, groupId int, client iggy.MessageStream) {
	consumer, err := client.GetConsumerGroupById(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId), iggcon.NewIdentifier(groupId))

	itShouldReturnSpecificError(err, "consumer_group_not_found")
	It("should not return consumer", func() {
		Expect(consumer).To(BeNil())
	})
}

func itShouldSuccessfullyJoinConsumer(streamId int, topicId int, groupId int, client iggy.MessageStream) {
	consumer, err := client.GetConsumerGroupById(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId), iggcon.NewIdentifier(groupId))

	It("should join consumer with id "+string(rune(groupId)), func() {
		Expect(consumer).NotTo(BeNil())
		Expect(consumer.MembersCount).ToNot(Equal(0))
	})

	itShouldNotReturnError(err)
}

func itShouldSuccessfullyLeaveConsumer(streamId int, topicId int, groupId int, client iggy.MessageStream) {
	consumer, err := client.GetConsumerGroupById(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId), iggcon.NewIdentifier(groupId))

	It("should leave consumer with id "+string(rune(groupId)), func() {
		Expect(consumer).NotTo(BeNil())
		Expect(consumer.MembersCount).To(Equal(0))
	})

	itShouldNotReturnError(err)
}
