package binaryserialization

//func TestSerialize_TcpFetchMessagesRequest(t *testing.T) {
//	// Create a sample TcpFetchMessagesRequest
//	request := TcpFetchMessagesRequest{
//		FetchMessagesRequest: iggcon.FetchMessagesRequest{
//			Consumer: iggcon.Consumer{
//				Kind: iggcon.ConsumerSingle,
//				Id:   42,
//			},
//			StreamId:        iggcon.NewIdentifier("test_stream_id"),
//			TopicId:         iggcon.NewIdentifier("test_topic_id"),
//			PartitionId:     123,
//			PollingStrategy: iggcon.FirstPollingStrategy(),
//			Count:           100,
//			AutoCommit:      true,
//		},
//	}
//
//	// Serialize the request
//	serialized := request.Serialize()
//
//	// Expected serialized bytes based on the provided sample request
//	expected := []byte{
//		0x01,                   // Consumer Kind
//		0x2A, 0x00, 0x00, 0x00, // Consumer ID
//		0x02,                                                                               // StreamId Kind (StringId)
//		0x0E,                                                                               // StreamId Length (14)
//		0x74, 0x65, 0x73, 0x74, 0x5F, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x5F, 0x69, 0x64, // StreamId
//		0x02,                                                                         // TopicId Kind (StringId)
//		0x0D,                                                                         // TopicId Length (13)
//		0x74, 0x65, 0x73, 0x74, 0x5F, 0x74, 0x6F, 0x70, 0x69, 0x63, 0x5F, 0x69, 0x64, // TopicId
//		0x7B, 0x00, 0x00, 0x00, // PartitionId (123)
//		0x03,                                           // PollingStrategy Kind
//		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // PollingStrategy Value (0)
//		0x64, 0x00, 0x00, 0x00, // Count (100)
//		0x01, // AutoCommit
//	}
//
//	// Check if the serialized bytes match the expected bytes
//	if !areBytesEqual(serialized, expected) {
//		t.Errorf("Serialized bytes are incorrect. \nExpected:\t%v\nGot:\t\t%v", expected, serialized)
//	}
//}
//
//func areBytesEqual(a, b []byte) bool {
//	if len(a) != len(b) {
//		return false
//	}
//	for i := range a {
//		if a[i] != b[i] {
//			return false
//		}
//	}
//	return true
//}
