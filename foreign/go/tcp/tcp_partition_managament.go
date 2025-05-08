package tcp

import (
	binaryserialization "github.com/iggy-rs/iggy-go-client/binary_serialization"
	. "github.com/iggy-rs/iggy-go-client/contracts"
)

func (tms *IggyTcpClient) CreatePartition(request CreatePartitionsRequest) error {
	message := binaryserialization.CreatePartitions(request)
	_, err := tms.sendAndFetchResponse(message, CreatePartitionsCode)
	return err
}

func (tms *IggyTcpClient) DeletePartition(request DeletePartitionRequest) error {
	message := binaryserialization.DeletePartitions(request)
	_, err := tms.sendAndFetchResponse(message, DeletePartitionsCode)
	return err
}
