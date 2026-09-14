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
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
)

const (
	maxPollRoutes         = 4096
	maxPollConnections    = 256
	consumerSessionSize   = 32
	pollParametersSize    = 1 + 8 + 4 + 1
	pollHeartbeatInterval = 5 * time.Second
)

// Primary polls have no deduplication key, including within one VSR session.
type singlePollExchange struct{}

type pollExchangeState struct {
	written  bool
	reusable bool
}

// Published under c.mtx after authentication, then immutable. Warm data polls
// must not wait behind unrelated coordinator I/O just to read their identity.
type activePollSession struct {
	parent        consumerSession
	configuration config
}

func (c *IggyTcpClient) publishPollSession() {
	if c.session == nil || !c.session.Bound() || !c.rememberedLogin.enabled {
		return
	}
	configuration := c.config
	configuration.autoLogin = c.rememberedLogin
	c.pollSession.Store(&activePollSession{
		parent:        consumerSession{client: c.session.ClientID(), session: c.session.SessionID()},
		configuration: configuration,
	})
}

func (c *IggyTcpClient) clearPollSession() {
	c.pollSession.Store(nil)
	c.polls.clear()
}

type consumerSession struct {
	client    vsr.ClientID
	session   uint64
	watermark uint64
}

func (s consumerSession) bytes() []byte {
	body := make([]byte, consumerSessionSize)
	binary.LittleEndian.PutUint64(body, s.client.Lo)
	binary.LittleEndian.PutUint64(body[8:], s.client.Hi)
	binary.LittleEndian.PutUint64(body[16:], s.session)
	binary.LittleEndian.PutUint64(body[24:], s.watermark)
	return body
}

type pollRoute struct {
	endpoint string
	parent   consumerSession
}

// The gate serializes a data connection without making callers wait beyond
// their context. Retirement closes its socket without waiting for an exchange.
type pollConnection struct {
	gate     chan struct{}
	mu       sync.Mutex
	ctx      context.Context
	cancel   context.CancelFunc
	conn     net.Conn
	retired  bool
	client   *IggyTcpClient
	parent   consumerSession
	attached bool
}

func (p *pollConnection) retire() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.retired = true
	p.cancel()
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
}

type pollRouter struct {
	mu            sync.Mutex
	routes        map[string]pollRoute
	connections   map[string]*pollConnection
	nextHeartbeat time.Time
}

func (p *pollRouter) clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	clear(p.routes)
	for _, connection := range p.connections {
		connection.retire()
	}
	clear(p.connections)
	p.nextHeartbeat = time.Time{}
}

func (p *pollRouter) dropRoute(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.routes, key)
}

func (p *pollRouter) dropConnection(endpoint string, connection *pollConnection) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.connections[endpoint] == connection {
		delete(p.connections, endpoint)
	}
	connection.retire()
}

func (c *IggyTcpClient) pollPrimary(caller context.Context, request *command.PollMessages) (response []byte, err error) {
	if caller == nil {
		return nil, ierror.ErrNilContext
	}
	ctx, cancel := context.WithTimeout(caller, responseReadTimeout)
	defer func() {
		if err != nil && ctx.Err() != nil {
			if caller.Err() != nil {
				err = caller.Err()
			} else if !errors.Is(err, ierror.ErrTransientNotAccepted) {
				err = ierror.ErrTransientNotCommitted
			}
		}
		cancel()
	}()
	payload, err := request.MarshalBinary()
	if err != nil {
		return nil, err
	}
	key := string(payload[:len(payload)-pollParametersSize])

	c.polls.mu.Lock()
	now := time.Now()
	heartbeatDue := !c.polls.nextHeartbeat.IsZero() && !now.Before(c.polls.nextHeartbeat)
	if c.polls.nextHeartbeat.IsZero() || heartbeatDue {
		c.polls.nextHeartbeat = now.Add(pollHeartbeatInterval)
	}
	c.polls.mu.Unlock()
	if heartbeatDue {
		if _, _, err := c.sendPollRequest(ctx, uint32(command.PingCode), nil); err != nil {
			if !isReconnectable(err) {
				return nil, err
			}
			if _, err := c.do(ctx, &command.Ping{}); err != nil {
				return nil, err
			}
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		route, err := c.pollRoute(ctx, key, payload)
		var response []byte
		if err == nil {
			response, err = c.pollOnRoute(ctx, key, payload, route)
		}
		if !errors.Is(err, ierror.ErrTransientNotAccepted) {
			return response, err
		}
		c.polls.dropRoute(key)
		deadline, _ := ctx.Deadline()
		if time.Until(deadline) <= replayInterval {
			return nil, err
		}
		if waitErr := c.waitBeforeReplay(ctx, deadline); waitErr != nil {
			if caller.Err() == nil && ctx.Err() != nil {
				return nil, err
			}
			return nil, waitErr
		}
	}
}

func (c *IggyTcpClient) pollOnRoute(ctx context.Context, key string, payload []byte, route pollRoute) ([]byte, error) {
	c.polls.mu.Lock()
	if !c.matchesPollParent(route.parent) {
		c.polls.mu.Unlock()
		return nil, ierror.ErrTransientNotAccepted
	}
	slot := c.polls.connections[route.endpoint]
	if slot == nil {
		if len(c.polls.connections) >= maxPollConnections {
			for endpoint, candidate := range c.polls.connections {
				select {
				case candidate.gate <- struct{}{}:
					delete(c.polls.connections, endpoint)
					candidate.retire()
					<-candidate.gate
				default:
				}
				if len(c.polls.connections) < maxPollConnections {
					break
				}
			}
			if len(c.polls.connections) >= maxPollConnections {
				c.polls.mu.Unlock()
				return nil, ierror.ErrTransientNotAccepted
			}
		}
		if c.polls.connections == nil {
			c.polls.connections = make(map[string]*pollConnection)
		}
		lifetime, cancel := context.WithCancel(context.Background())
		slot = &pollConnection{gate: make(chan struct{}, 1), ctx: lifetime, cancel: cancel}
		c.polls.connections[route.endpoint] = slot
	}
	c.polls.mu.Unlock()
	select {
	case slot.gate <- struct{}{}:
		defer func() { <-slot.gate }()
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-slot.ctx.Done():
		return nil, ierror.ErrTransientNotAccepted
	}
	exchangeCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(slot.ctx, cancel)
	defer stop()
	defer cancel()
	if slot.ctx.Err() != nil {
		return nil, ierror.ErrTransientNotAccepted
	}
	if !c.pollParentCurrent(route.parent) {
		return nil, ierror.ErrTransientNotAccepted
	}

	if slot.client == nil {
		client, err := c.connectPollClient(exchangeCtx, route)
		if err != nil {
			err = slot.exchangeError(ctx, pollExchangeState{}, err)
			c.polls.dropConnection(route.endpoint, slot)
			return nil, err
		}
		slot.mu.Lock()
		if slot.retired {
			slot.mu.Unlock()
			_ = client.Close()
			return nil, ierror.ErrTransientNotAccepted
		}
		slot.conn = client.conn
		slot.client = client
		slot.mu.Unlock()
	}
	if !slot.attached || slot.parent.client != route.parent.client ||
		slot.parent.session != route.parent.session || slot.parent.watermark < route.parent.watermark {
		if _, state, err := slot.client.sendPollRequest(exchangeCtx, uint32(command.AttachConsumerSessionCode), route.parent.bytes()); err != nil {
			// An attach cannot advance an offset, even if its reply is lost.
			state.written = false
			err = slot.exchangeError(ctx, state, err)
			if !state.reusable {
				c.polls.dropConnection(route.endpoint, slot)
			}
			return nil, err
		}
		slot.parent = route.parent
		slot.attached = true
	}
	if !c.pollParentCurrent(route.parent) {
		return nil, ierror.ErrTransientNotAccepted
	}
	response, state, err := slot.client.sendPollRequest(exchangeCtx, uint32(command.PollMessagesOnPrimaryCode), payload)
	err = slot.exchangeError(ctx, state, err)
	if errors.Is(err, ierror.ErrTransientNotAccepted) {
		slot.attached = false
	}
	if err != nil {
		c.polls.dropRoute(key)
		if !state.reusable {
			c.polls.dropConnection(route.endpoint, slot)
		}
	}
	return response, err
}

// Cache insertion checks this under the router lock; teardown clears the
// atomic snapshot before taking that lock and retiring old connections.
func (c *IggyTcpClient) matchesPollParent(parent consumerSession) bool {
	current := c.pollSession.Load()
	return current != nil && current.parent.client == parent.client &&
		current.parent.session == parent.session
}

func (c *IggyTcpClient) pollParentCurrent(parent consumerSession) bool {
	return c.matchesPollParent(parent) && parent.watermark >= c.metadataWatermark.Load()
}

func (c *IggyTcpClient) pollRoute(ctx context.Context, key string, payload []byte) (pollRoute, error) {
	c.polls.mu.Lock()
	route, cached := c.polls.routes[key]
	c.polls.mu.Unlock()
	if cached && c.pollParentCurrent(route.parent) {
		return route, nil
	}
	// This control command can recover a failed coordinator, but its one-exchange
	// sendFrame path never moves a healthy coordinator after partition refusal.
	body, err := c.SendBinaryRequest(ctx, uint32(command.GetPollRoutingCode), payload)
	if err != nil {
		return pollRoute{}, err
	}
	route, err = decodePollRoute(body)
	if err != nil {
		return pollRoute{}, err
	}
	c.polls.mu.Lock()
	defer c.polls.mu.Unlock()
	if !c.matchesPollParent(route.parent) {
		return pollRoute{}, ierror.ErrTransientNotAccepted
	}
	route.parent.watermark = max(route.parent.watermark, c.metadataWatermark.Load())
	if c.polls.routes == nil {
		c.polls.routes = make(map[string]pollRoute)
	}
	if len(c.polls.routes) >= maxPollRoutes {
		clear(c.polls.routes)
	}
	c.polls.routes[key] = route
	return route, nil
}

func decodePollRoute(body []byte) (pollRoute, error) {
	if len(body) < consumerSessionSize {
		return pollRoute{}, ierror.ErrInvalidCommand
	}
	var node iggcon.ClusterNode
	if err := node.UnmarshalBinary(body[consumerSessionSize:]); err != nil {
		return pollRoute{}, err
	}
	if node.Endpoints.Tcp == 0 {
		return pollRoute{}, ierror.ErrFeatureUnavailable
	}
	return pollRoute{
		endpoint: net.JoinHostPort(strings.Trim(node.IP, "[]"), strconv.Itoa(int(node.Endpoints.Tcp))),
		parent: consumerSession{
			client:    vsr.ClientID{Lo: binary.LittleEndian.Uint64(body), Hi: binary.LittleEndian.Uint64(body[8:])},
			session:   binary.LittleEndian.Uint64(body[16:]),
			watermark: binary.LittleEndian.Uint64(body[24:]),
		},
	}, nil
}

func (c *IggyTcpClient) connectPollClient(ctx context.Context, route pollRoute) (*IggyTcpClient, error) {
	snapshot := c.pollSession.Load()
	if snapshot == nil || snapshot.parent.client != route.parent.client || snapshot.parent.session != route.parent.session {
		return nil, ierror.ErrUnauthenticated
	}
	configuration := snapshot.configuration
	configuration.serverAddress = route.endpoint
	configuration.reconnection.enabled = false
	client := NewIggyTcpClient(c.logger, func(options *Options) { options.config = configuration })
	if err := client.Connect(suppressLeaderSettlement(ctx)); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

func (c *IggyTcpClient) sendPollRequest(ctx context.Context, code uint32, payload []byte) ([]byte, pollExchangeState, error) {
	bp := acquireRequestBuf()
	defer releaseRequestBuf(bp)
	frame := append(reserveHeader(*bp), payload...)
	*bp = frame
	response, _, state, err := c.sendPollFrame(ctx, code, frame)
	return response, state, err
}

func (c *IggyTcpClient) sendPollFrame(ctx context.Context, code uint32, frame []byte) ([]byte, uint64, pollExchangeState, error) {
	state := pollExchangeState{}
	if ctx == nil {
		return nil, 0, state, ierror.ErrNilContext
	}
	response, _, generation, err := c.attempt(context.WithValue(ctx, singlePollExchange{}, &state),
		code, frame, false, time.Now(), time.Now().Add(responseReadTimeout))
	return response, generation, state, err
}

func (p *pollConnection) exchangeError(ctx context.Context, state pollExchangeState, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if p.ctx.Err() != nil || isReconnectable(err) || (state.written && !state.reusable) {
		if state.written {
			return ierror.ErrTransientNotCommitted
		}
		return ierror.ErrTransientNotAccepted
	}
	return err
}
