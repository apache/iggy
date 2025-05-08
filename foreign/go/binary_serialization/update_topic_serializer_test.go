package binaryserialization

import (
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
	"testing"
)

func TestSerialize_UpdateTopic(t *testing.T) {
	request := TcpUpdateTopicRequest{
		iggcon.UpdateTopicRequest{
			StreamId:      iggcon.NewIdentifier("stream"),
			TopicId:       iggcon.NewIdentifier(1),
			Name:          "update_topic",
			MessageExpiry: 100,
		},
	}

	serialized1 := request.Serialize()

	expected := []byte{
		0x02,                               // StreamId Kind (StringId)
		0x06,                               // StreamId Length (2)
		0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, // StreamId Value ("stream")
		0x01,                   // TopicId Kind (NumericId)
		0x04,                   // TopicId Length (4)
		0x01, 0x00, 0x00, 0x00, // TopicId Value (1)
		0x64, 0x00, 0x00, 0x00, // Message Expiry (100)
		0x0C,                                                                   // Name Length (12)
		0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x5F, 0x74, 0x6F, 0x70, 0x69, 0x63, // Name ("update_topic")
	}

	if !areBytesEqual(serialized1, expected) {
		t.Errorf("Test case 1 failed. \nExpected:\t%v\nGot:\t\t%v", expected, serialized1)
	}
}
