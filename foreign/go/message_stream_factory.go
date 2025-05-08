package iggy

import (
	"context"
	"errors"

	"github.com/iggy-rs/iggy-go-client/tcp"

	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
)

type IMessageStreamFactory interface {
	CreateStream(config iggcon.IggyConfiguration) (MessageStream, error)
}

type IggyClientFactory struct{}

func (msf *IggyClientFactory) CreateMessageStream(config iggcon.IggyConfiguration) (MessageStream, error) {
	// Support previous behaviour
	if config.Context == nil {
		config.Context = context.Background()
	}

	if config.Protocol == iggcon.Tcp {
		tcpMessageStream, err := tcp.NewTcpMessageStream(
			config.Context,
			config.BaseAddress,
			config.MessageCompression,
			config.HeartbeatInterval,
		)
		if err != nil {
			return nil, err
		}
		return tcpMessageStream, nil
	}

	return nil, errors.New("unsupported protocol")
}
