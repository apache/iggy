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
	"os"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/cucumber/godog"
)

type rawCommandCtxKey struct{}

type rawCommandCtx struct {
	client       iggcon.Client
	serverAddr   string
	lastResponse []byte
	lastError    error
}

func getRawCommandCtx(ctx context.Context) *rawCommandCtx {
	return ctx.Value(rawCommandCtxKey{}).(*rawCommandCtx)
}

type rawCommandSteps struct{}

func (rawCommandSteps) givenRunningServer(ctx context.Context) error {
	address := os.Getenv("IGGY_TCP_ADDRESS")
	if address == "" {
		address = "127.0.0.1:8090"
	}
	getRawCommandCtx(ctx).serverAddr = address
	return nil
}

func (rawCommandSteps) givenAuthenticationAsRoot(ctx context.Context) error {
	state := getRawCommandCtx(ctx)
	iggyClient, err := client.NewIggyClient(client.WithTcp(tcp.WithServerAddress(state.serverAddr)))
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	if err = iggyClient.Connect(ctx); err != nil {
		return fmt.Errorf("connect client: %w", err)
	}
	if _, err = iggyClient.LoginUser(ctx, "iggy", "iggy"); err != nil {
		return fmt.Errorf("authenticate client: %w", err)
	}
	state.client = iggyClient
	return nil
}

func (rawCommandSteps) whenSendRawCommand(ctx context.Context, code uint32) error {
	state := getRawCommandCtx(ctx)
	state.lastResponse, state.lastError = state.client.SendBinaryRequest(ctx, code, nil)
	return nil
}

func (rawCommandSteps) thenEmptyResponse(ctx context.Context) error {
	state := getRawCommandCtx(ctx)
	if state.lastError != nil {
		return fmt.Errorf("raw command failed: %w", state.lastError)
	}
	if len(state.lastResponse) != 0 {
		return fmt.Errorf("expected empty response, got %d bytes", len(state.lastResponse))
	}
	return nil
}

func (rawCommandSteps) thenNonEmptyResponse(ctx context.Context) error {
	state := getRawCommandCtx(ctx)
	if state.lastError != nil {
		return fmt.Errorf("raw command failed: %w", state.lastError)
	}
	if len(state.lastResponse) == 0 {
		return errors.New("expected non-empty response")
	}
	return nil
}

func (rawCommandSteps) thenInvalidCommand(ctx context.Context) error {
	if !errors.Is(getRawCommandCtx(ctx).lastError, ierror.ErrInvalidCommand) {
		return fmt.Errorf("expected invalid command error, got %v", getRawCommandCtx(ctx).lastError)
	}
	return nil
}

func initRawCommandScenario(sc *godog.ScenarioContext) {
	sc.Before(func(context.Context, *godog.Scenario) (context.Context, error) {
		return context.WithValue(context.Background(), rawCommandCtxKey{}, &rawCommandCtx{}), nil
	})
	steps := rawCommandSteps{}
	sc.Step(`I have a running Iggy server`, steps.givenRunningServer)
	sc.Step(`I am authenticated as the root user`, steps.givenAuthenticationAsRoot)
	sc.Step(`^I send a raw command with code (\d+) and an empty payload$`, steps.whenSendRawCommand)
	sc.Step(`the raw command should succeed with an empty response`, steps.thenEmptyResponse)
	sc.Step(`the raw command should succeed with a non-empty response`, steps.thenNonEmptyResponse)
	sc.Step(`the raw command should fail with an invalid command error`, steps.thenInvalidCommand)
	sc.After(func(ctx context.Context, _ *godog.Scenario, scenarioError error) (context.Context, error) {
		state := getRawCommandCtx(ctx)
		if state.client != nil {
			scenarioError = errors.Join(scenarioError, state.client.Close())
		}
		return ctx, scenarioError
	})
}
