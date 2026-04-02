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

package tests

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/cucumber/godog"
)

type streamOpsCtxKey struct{}

type streamOpsCtx struct {
	serverAddr *string
	client     iggcon.Client
	streamID   *uint32
	streamName *string
}

func getStreamOpsCtx(ctx context.Context) *streamOpsCtx {
	return ctx.Value(streamOpsCtxKey{}).(*streamOpsCtx)
}

type streamOpsSteps struct{}

func (s streamOpsSteps) givenRunningServer(ctx context.Context) error {
	c := getStreamOpsCtx(ctx)
	addr := os.Getenv("IGGY_TCP_ADDRESS")
	if addr == "" {
		addr = "127.0.0.1:8090"
	}
	c.serverAddr = &addr
	return nil
}

func (s streamOpsSteps) givenAuthenticationAsRoot(ctx context.Context) error {
	c := getStreamOpsCtx(ctx)
	serverAddr := *c.serverAddr

	cli, err := client.NewIggyClient(
		client.WithTcp(
			tcp.WithServerAddress(serverAddr),
		),
	)
	if err != nil {
		return fmt.Errorf("error creating client: %w", err)
	}

	if err = cli.Ping(); err != nil {
		return fmt.Errorf("error pinging client: %w", err)
	}

	if _, err = cli.LoginUser("iggy", "iggy"); err != nil {
		return fmt.Errorf("error logging in: %v", err)
	}

	c.client = cli
	return nil
}

func (s streamOpsSteps) whenCreateStreamWithUniqueName(ctx context.Context) error {
	c := getStreamOpsCtx(ctx)

	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 32)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	name := string(b)

	stream, err := c.client.CreateStream(name)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	c.streamID = &stream.Id
	c.streamName = &stream.Name
	return nil
}

func (s streamOpsSteps) thenStreamCreatedSuccessfully(ctx context.Context) error {
	c := getStreamOpsCtx(ctx)
	if c.streamID == nil {
		return fmt.Errorf("stream should have been created")
	}
	return nil
}

func (s streamOpsSteps) thenStreamRetrievableByID(ctx context.Context) error {
	c := getStreamOpsCtx(ctx)
	streamIdentifier, _ := iggcon.NewIdentifier(*c.streamID)
	stream, err := c.client.GetStream(streamIdentifier)
	if err != nil {
		return fmt.Errorf("failed to get stream by ID: %w", err)
	}
	if stream == nil {
		return fmt.Errorf("stream should not be nil")
	}
	return nil
}

func (s streamOpsSteps) thenStreamNameMatchesCreated(ctx context.Context) error {
	c := getStreamOpsCtx(ctx)
	streamIdentifier, _ := iggcon.NewIdentifier(*c.streamID)
	stream, err := c.client.GetStream(streamIdentifier)
	if err != nil {
		return fmt.Errorf("failed to get stream: %w", err)
	}
	if stream.Name != *c.streamName {
		return fmt.Errorf("expected stream name %s, got %s", *c.streamName, stream.Name)
	}
	return nil
}

func initStreamOpsScenario(sc *godog.ScenarioContext) {
	sc.Before(func(ctx context.Context, sc *godog.Scenario) (context.Context, error) {
		return context.WithValue(context.Background(), streamOpsCtxKey{}, &streamOpsCtx{}), nil
	})

	s := &streamOpsSteps{}
	sc.Step(`I have a running Iggy server`, s.givenRunningServer)
	sc.Step(`I am authenticated as the root user`, s.givenAuthenticationAsRoot)
	sc.Step(`I create a stream with a unique name`, s.whenCreateStreamWithUniqueName)
	sc.Step(`the stream should be created successfully`, s.thenStreamCreatedSuccessfully)
	sc.Step(`the stream should be retrievable by its ID`, s.thenStreamRetrievableByID)
	sc.Step(`the stream name should match the created name`, s.thenStreamNameMatchesCreated)

	sc.After(func(ctx context.Context, sc *godog.Scenario, scErr error) (context.Context, error) {
		c := getStreamOpsCtx(ctx)
		if c.client != nil && c.streamID != nil {
			streamIdentifier, _ := iggcon.NewIdentifier(*c.streamID)
			_ = c.client.DeleteStream(streamIdentifier)
		}
		if c.client != nil {
			if err := c.client.Close(); err != nil {
				scErr = errors.Join(scErr, fmt.Errorf("error closing client: %w", err))
			}
		}
		return ctx, scErr
	})
}
