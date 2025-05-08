package tcp

import (
	binaryserialization "github.com/iggy-rs/iggy-go-client/binary_serialization"
	. "github.com/iggy-rs/iggy-go-client/contracts"
)

func (tms *IggyTcpClient) GetOffset(request GetOffsetRequest) (*OffsetResponse, error) {
	message := binaryserialization.GetOffset(request)
	buffer, err := tms.sendAndFetchResponse(message, GetOffsetCode)
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeOffset(buffer), nil
}

func (tms *IggyTcpClient) StoreOffset(request StoreOffsetRequest) error {
	message := binaryserialization.UpdateOffset(request)
	_, err := tms.sendAndFetchResponse(message, StoreOffsetCode)
	return err
}
