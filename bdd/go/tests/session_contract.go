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

	"github.com/apache/iggy/bdd/go/tests/env"
	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/cucumber/godog"
)

type sessionContractCtxKey struct{}

type sessionContractCtx struct {
	client     iggcon.Client
	serverAddr string
	lastError  error
}

func getSessionContractCtx(ctx context.Context) *sessionContractCtx {
	return ctx.Value(sessionContractCtxKey{}).(*sessionContractCtx)
}

type sessionContractSteps struct{}

func (sessionContractSteps) givenRunningServer(ctx context.Context) error {
	getSessionContractCtx(ctx).serverAddr = env.ServerAddress()
	return nil
}

func (sessionContractSteps) givenNodeWithoutQuorum(ctx context.Context) error {
	getSessionContractCtx(ctx).serverAddr = env.NoQuorumAddress()
	return nil
}

func (sessionContractSteps) givenAuthenticationAsRoot(ctx context.Context) error {
	state := getSessionContractCtx(ctx)
	if err := state.connect(ctx); err != nil {
		return err
	}
	username, password := env.RootCredentials()
	if _, err := state.client.LoginUser(ctx, username, password); err != nil {
		return fmt.Errorf("authenticate client: %w", err)
	}
	return nil
}

func (sessionContractSteps) whenSignOut(ctx context.Context) error {
	if err := getSessionContractCtx(ctx).client.LogoutUser(ctx); err != nil {
		return fmt.Errorf("sign out: %w", err)
	}
	return nil
}

func (sessionContractSteps) whenRequestStream(ctx context.Context, streamID uint32) error {
	state := getSessionContractCtx(ctx)
	identifier, err := iggcon.NewIdentifier(streamID)
	if err != nil {
		return fmt.Errorf("stream identifier: %w", err)
	}
	_, state.lastError = state.client.GetStream(ctx, identifier)
	return nil
}

func (sessionContractSteps) whenSignInToNode(ctx context.Context) error {
	state := getSessionContractCtx(ctx)
	if err := state.connect(ctx); err != nil {
		return err
	}
	username, password := env.RootCredentials()
	_, state.lastError = state.client.LoginUser(ctx, username, password)
	return nil
}

func (sessionContractSteps) thenCallFailsWithCode(ctx context.Context, code uint32) error {
	lastError := getSessionContractCtx(ctx).lastError
	var iggyError ierror.IggyError
	if !errors.As(lastError, &iggyError) {
		return fmt.Errorf("expected error code %d, got %v", code, lastError)
	}
	if uint32(iggyError.Code()) != code {
		return fmt.Errorf("expected error code %d, got %d (%v)", code, iggyError.Code(), lastError)
	}
	return nil
}

func (state *sessionContractCtx) connect(ctx context.Context) error {
	iggyClient, err := client.NewIggyClient(client.WithTcp(tcp.WithServerAddress(state.serverAddr)))
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	if err = iggyClient.Connect(ctx); err != nil {
		return fmt.Errorf("connect client: %w", err)
	}
	state.client = iggyClient
	return nil
}

func initSessionContractScenario(sc *godog.ScenarioContext) {
	sc.Before(func(context.Context, *godog.Scenario) (context.Context, error) {
		return context.WithValue(context.Background(), sessionContractCtxKey{}, &sessionContractCtx{}), nil
	})
	steps := sessionContractSteps{}
	sc.Step(`^I have a running Iggy server$`, steps.givenRunningServer)
	sc.Step(`^I have a cluster node that cannot reach quorum$`, steps.givenNodeWithoutQuorum)
	sc.Step(`^I am authenticated as the root user$`, steps.givenAuthenticationAsRoot)
	sc.Step(`^I sign out$`, steps.whenSignOut)
	sc.Step(`^I request the stream with ID (\d+)$`, steps.whenRequestStream)
	sc.Step(`^I sign in to that node as the root user$`, steps.whenSignInToNode)
	sc.Step(`^the call should fail with error code (\d+)$`, steps.thenCallFailsWithCode)
	sc.After(func(ctx context.Context, _ *godog.Scenario, scenarioError error) (context.Context, error) {
		state := getSessionContractCtx(ctx)
		if state.client != nil {
			scenarioError = errors.Join(scenarioError, state.client.Close())
		}
		return ctx, scenarioError
	})
}
