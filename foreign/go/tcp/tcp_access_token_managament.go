package tcp

import (
	binaryserialization "github.com/iggy-rs/iggy-go-client/binary_serialization"
	. "github.com/iggy-rs/iggy-go-client/contracts"
)

func (tms *IggyTcpClient) CreateAccessToken(request CreateAccessTokenRequest) (*AccessToken, error) {
	message := binaryserialization.SerializeCreatePersonalAccessToken(request)
	buffer, err := tms.sendAndFetchResponse(message, CreateAccessTokenCode)
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeAccessToken(buffer)
}

func (tms *IggyTcpClient) DeleteAccessToken(request DeleteAccessTokenRequest) error {
	message := binaryserialization.SerializeDeletePersonalAccessToken(request)
	_, err := tms.sendAndFetchResponse(message, DeleteAccessTokenCode)
	return err
}

func (tms *IggyTcpClient) GetAccessTokens() ([]AccessTokenResponse, error) {
	buffer, err := tms.sendAndFetchResponse([]byte{}, GetAccessTokensCode)
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeAccessTokens(buffer)
}
