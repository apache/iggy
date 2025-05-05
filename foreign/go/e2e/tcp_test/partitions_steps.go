package tcp_test

import (
	"github.com/iggy-rs/iggy-go-client"
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func itShouldHaveExpectedNumberOfPartitions(streamId int, topicId int, expectedPartitions int, client iggy.MessageStream) {
	topic, err := client.GetTopicById(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(topicId))

	It("should have "+string(rune(expectedPartitions))+" partitions", func() {
		Expect(topic).NotTo(BeNil())
		Expect(topic.PartitionsCount).To(Equal(expectedPartitions))
		Expect(len(topic.Partitions)).To(Equal(expectedPartitions))
	})

	itShouldNotReturnError(err)
}
