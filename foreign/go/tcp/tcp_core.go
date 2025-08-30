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

package tcp

import (
	"context"
	"encoding/binary"
	"log"
	"net"
	"sync"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

type Option func(config *Options)

type Options struct {
	Ctx               context.Context
	ServerAddress     string
	HeartbeatInterval time.Duration
}

func GetDefaultOptions() Options {
	return Options{
		Ctx:               context.Background(),
		ServerAddress:     "127.0.0.1:8090",
		HeartbeatInterval: time.Second * 5,
	}
}

type IggyTcpClient struct {
	conn               *net.TCPConn
	mtx                sync.Mutex
	MessageCompression iggcon.IggyMessageCompression
}

// WithServerAddress Sets the server address for the TCP client.
func WithServerAddress(address string) Option {
	return func(opts *Options) {
		opts.ServerAddress = address
	}
}

// WithContext sets context
func WithContext(ctx context.Context) Option {
	return func(opts *Options) {
		opts.Ctx = ctx
	}
}

func NewIggyTcpClient(options ...Option) (*IggyTcpClient, error) {
	opts := GetDefaultOptions()
	for _, opt := range options {
		if opt != nil {
			opt(&opts)
		}
	}
	addr, err := net.ResolveTCPAddr("tcp", opts.ServerAddress)
	if err != nil {
		return nil, err
	}
	ctx := opts.Ctx
	var d = net.Dialer{
		KeepAlive: -1,
	}
	conn, err := d.DialContext(ctx, "tcp", addr.String())
	if err != nil {
		return nil, err
	}

	client := &IggyTcpClient{
		conn: conn.(*net.TCPConn),
	}

	heartbeatInterval := opts.HeartbeatInterval
	if heartbeatInterval > 0 {
		go func() {
			ticker := time.NewTicker(heartbeatInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err = client.Ping(); err != nil {
						log.Printf("[WARN] heartbeat failed: %v", err)
					}
				}
			}
		}()
	}

	return client, nil
}

const (
	InitialBytesLength   = 4
	ExpectedResponseSize = 8
	MaxStringLength      = 255
)

func (tms *IggyTcpClient) read(expectedSize int) (int, []byte, error) {
	var totalRead int
	buffer := make([]byte, expectedSize)

	for totalRead < expectedSize {
		readSize := expectedSize - totalRead
		n, err := tms.conn.Read(buffer[totalRead : totalRead+readSize])
		if err != nil {
			return totalRead, buffer[:totalRead], err
		}
		totalRead += n
	}

	return totalRead, buffer, nil
}

func (tms *IggyTcpClient) write(payload []byte) (int, error) {
	var totalWritten int
	for totalWritten < len(payload) {
		n, err := tms.conn.Write(payload[totalWritten:])
		if err != nil {
			return totalWritten, err
		}
		totalWritten += n
	}

	return totalWritten, nil
}

func (tms *IggyTcpClient) sendAndFetchResponse(message []byte, command iggcon.CommandCode) ([]byte, error) {
	tms.mtx.Lock()
	defer tms.mtx.Unlock()

	payload := createPayload(message, command)
	if _, err := tms.write(payload); err != nil {
		return nil, err
	}

	_, buffer, err := tms.read(ExpectedResponseSize)
	if err != nil {
		return nil, err
	}

	length := int(binary.LittleEndian.Uint32(buffer[4:]))
	if responseCode := getResponseCode(buffer); responseCode != 0 {
		// TEMP: See https://github.com/apache/iggy/pull/604 for context.
		// from: https://github.com/apache/iggy/blob/master/sdk/src/tcp/client.rs#L326
		if responseCode == 2012 ||
			responseCode == 2013 ||
			responseCode == 1011 ||
			responseCode == 1012 ||
			responseCode == 46 ||
			responseCode == 51 ||
			responseCode == 5001 ||
			responseCode == 5004 {
			// do nothing
		} else {
			return nil, ierror.MapFromCode(responseCode)
		}

		return buffer, ierror.MapFromCode(responseCode)
	}

	if length <= 1 {
		return []byte{}, nil
	}

	_, buffer, err = tms.read(length)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func createPayload(message []byte, command iggcon.CommandCode) []byte {
	messageLength := len(message) + 4
	messageBytes := make([]byte, InitialBytesLength+messageLength)
	binary.LittleEndian.PutUint32(messageBytes[:4], uint32(messageLength))
	binary.LittleEndian.PutUint32(messageBytes[4:8], uint32(command))
	copy(messageBytes[8:], message)
	return messageBytes
}

func getResponseCode(buffer []byte) int {
	return int(binary.LittleEndian.Uint32(buffer[:4]))
}
