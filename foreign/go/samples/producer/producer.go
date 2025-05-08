package main

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	. "github.com/iggy-rs/iggy-go-client"
	. "github.com/iggy-rs/iggy-go-client/contracts"
	sharedDemoContracts "github.com/iggy-rs/iggy-go-client/samples/shared"
)

const (
	StreamId          = 1
	TopicId           = 1
	MessageBatchCount = 1
	Partition         = 1
	Interval          = 1000
)

func main() {
	factory := &IggyClientFactory{}
	config := IggyConfiguration{
		BaseAddress: "127.0.0.1:8090",
		Protocol:    Tcp,
	}

	messageStream, err := factory.CreateMessageStream(config)
	if err != nil {
		panic(err)
	}
	_, err = messageStream.LogIn(LogInRequest{
		Username: "iggy",
		Password: "iggy",
	})
	if err != nil {
		panic("COULD NOT LOG IN")
	}

	if err = EnsureInsfrastructureIsInitialized(messageStream); err != nil {
		panic(err)
	}

	if err = PublishMessages(messageStream); err != nil {
		panic(err)
	}
}

func EnsureInsfrastructureIsInitialized(messageStream MessageStream) error {
	if _, streamErr := messageStream.GetStreamById(GetStreamRequest{
		StreamID: NewIdentifier(StreamId)}); streamErr != nil {
		streamErr = messageStream.CreateStream(CreateStreamRequest{
			StreamId: StreamId,
			Name:     "Test Producer Stream",
		})

		fmt.Println(StreamId)

		if streamErr != nil {
			panic(streamErr)
		}

		fmt.Printf("Created stream with ID: %d.\n", StreamId)
	}

	fmt.Printf("Stream with ID: %d exists.\n", StreamId)

	if _, topicErr := messageStream.GetTopicById(NewIdentifier(StreamId), NewIdentifier(TopicId)); topicErr != nil {
		topicErr = messageStream.CreateTopic(CreateTopicRequest{
			TopicId:         TopicId,
			Name:            "Test Topic From Producer Sample",
			PartitionsCount: 12,
			StreamId:        NewIdentifier(StreamId),
		})

		if topicErr != nil {
			panic(topicErr)
		}

		fmt.Printf("Created topic with ID: %d.\n", TopicId)
	}

	fmt.Printf("Topic with ID: %d exists.\n", TopicId)

	return nil
}

func PublishMessages(messageStream MessageStream) error {
	fmt.Printf("Messages will be sent to stream '%d', topic '%d', partition '%d' with interval %d ms.\n", StreamId, TopicId, Partition, Interval)
	messageGenerator := NewMessageGenerator()

	for {
		var debugMessages []sharedDemoContracts.ISerializableMessage
		var messages []Message

		for i := 0; i < MessageBatchCount; i++ {
			message := messageGenerator.GenerateMessage()
			json := message.ToBytes()

			debugMessages = append(debugMessages, message)
			messages = append(messages, Message{
				Id:      uuid.New(),
				Payload: json,
			})
		}

		err := messageStream.SendMessages(SendMessagesRequest{
			StreamId:     NewIdentifier(StreamId),
			TopicId:      NewIdentifier(TopicId),
			Messages:     messages,
			Partitioning: PartitionId(Partition),
		})
		if err != nil {
			return nil
		}

		for _, m := range debugMessages {
			fmt.Println("Sent messages:", m.ToJson())
		}

		time.Sleep(time.Millisecond * time.Duration(Interval))
	}
}
