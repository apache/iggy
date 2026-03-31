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
	"fmt"
	"math/rand"
	"os"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/iggycli"
	"github.com/apache/iggy/foreign/go/tcp"
	"github.com/cucumber/godog"
)

type streamOpsCtxKey struct{}

type streamOpsCtx struct {
	serverAddr *string
	client     iggycli.Client
	streamID   *uint32
	streamName *string
}

func getStreamOpsCtx(ctx context.Context) *streamOpsCtx {
	return ctx.Value(streamOpsCtxKey{}).(*streamOpsCtx)
}

func streamGivenRunningServer(ctx context.Context) error {
	c := getStreamOpsCtx(ctx)
	addr := os.Getenv("IGGY_TCP_ADDRESS")
	if addr == "" {
		addr = "127.0.0.1:8090"
	}
	c.serverAddr = &addr
	return nil
}

func streamGivenAuthenticationAsRoot(ctx context.Context) error {
	c := getStreamOpsCtx(ctx)
	serverAddr := *c.serverAddr

	client, err := iggycli.NewIggyClient(
		iggycli.WithTcp(
			tcp.WithServerAddress(serverAddr),
		),
	)
	if err != nil {
		return fmt.Errorf("error creating client: %w", err)
	}

	if err = client.Ping(); err != nil {
		return fmt.Errorf("error pinging client: %w", err)
	}

	if _, err = client.LoginUser("iggy", "iggy"); err != nil {
		return fmt.Errorf("error logging in: %v", err)
	}

	c.client = client
	return nil
}

func whenCreateStreamWithUniqueName(ctx context.Context) error {
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

func thenStreamCreatedSuccessfullyOps(ctx context.Context) error {
	c := getStreamOpsCtx(ctx)
	if c.streamID == nil {
		return fmt.Errorf("stream should have been created")
	}
	return nil
}

func thenStreamRetrievableByID(ctx context.Context) error {
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

func thenStreamNameMatchesCreated(ctx context.Context) error {
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

func initStreamScenarios(sc *godog.ScenarioContext) {
	sc.Before(func(ctx context.Context, sc *godog.Scenario) (context.Context, error) {
		return context.WithValue(context.Background(), streamOpsCtxKey{}, &streamOpsCtx{}), nil
	})

	sc.After(func(ctx context.Context, sc *godog.Scenario, err error) (context.Context, error) {
		c := getStreamOpsCtx(ctx)
		if c.client != nil && c.streamID != nil {
			streamIdentifier, _ := iggcon.NewIdentifier(*c.streamID)
			_ = c.client.DeleteStream(streamIdentifier)
		}
		return ctx, nil
	})

	// Background steps
	sc.Step(`I have a running Iggy server`, streamGivenRunningServer)
	sc.Step(`I am authenticated as the root user`, streamGivenAuthenticationAsRoot)

	// Stream operation steps
	sc.Step(`I create a stream with a unique name`, whenCreateStreamWithUniqueName)
	sc.Step(`the stream should be created successfully`, thenStreamCreatedSuccessfullyOps)
	sc.Step(`the stream should be retrievable by its ID`, thenStreamRetrievableByID)
	sc.Step(`the stream name should match the created name`, thenStreamNameMatchesCreated)
}

func TestStreamFeatures(t *testing.T) {
	suite := godog.TestSuite{
		ScenarioInitializer: initStreamScenarios,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"../../scenarios/stream_operations.feature"},
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("failing stream feature tests")
	}
}
