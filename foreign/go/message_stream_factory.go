// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package iggy

import (
	"context"
	"errors"

	"github.com/apache/iggy/foreign/go/tcp"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
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
