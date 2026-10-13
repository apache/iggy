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
	// Offset writes end with the offset and the ack level, a delete with the
	// ack level only.
	storeOffsetSuffixSize  = 8 + 1
	deleteOffsetSuffixSize = 1
)

// Primary polls have no deduplication key, including within one VSR session.
type singlePollExchange struct{}
type capturedPartitionContext struct{}

type pollExchangeState struct {
	stamped  bool
	written  bool
	reusable bool
	replay   bool
	parent   *consumerSession
}

// Published under c.mtx after authentication, then immutable. Warm data polls
// must not wait behind unrelated coordinator I/O just to read their identity.
type activePollSession struct {
	bindSecret    [vsr.BindSecretBytes]byte
	parent        consumerSession
	configuration config
	session       *vsr.Session
}

func (c *IggyTcpClient) publishPollSession() {
	if c.session == nil || !c.session.Bound() || !c.rememberedLogin.enabled {
		return
	}
	configuration := c.config
	configuration.autoLogin = c.rememberedLogin
	c.pollSession.Store(&activePollSession{
		bindSecret:    c.session.BindSecret(),
		parent:        consumerSession{client: c.session.ClientID(), session: c.session.SessionID()},
		configuration: configuration,
		session:       c.session.Fork(),
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
	context  iggcon.PartitionContext
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
	endpoints     map[uint8]string
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

func (p *pollRouter) clearRoutes() {
	p.mu.Lock()
	defer p.mu.Unlock()
	clear(p.routes)
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

// sendRouted keeps the coordinator and its membership in place. Retries after
// a primary move retain the stamped identity and captured history context.
func (c *IggyTcpClient) sendRouted(caller context.Context, code command.Code, query, frame []byte, captured *iggcon.PartitionContext) (response []byte, err error) {
	if caller == nil {
		return nil, ierror.ErrNilContext
	}
	ctx, cancel := context.WithTimeout(caller, responseReadTimeout)
	defer func() {
		if err != nil && ctx.Err() != nil {
			if caller.Err() != nil {
				err = caller.Err()
			} else if !errors.Is(err, ierror.ErrTransientNotAccepted) && !errors.Is(err, ierror.ErrNotPrimary) {
				err = ierror.ErrTransientNotCommitted
			}
		}
		cancel()
	}()
	if err := c.ensureTopology(ctx); err != nil {
		return nil, err
	}
	key := string(query)
	c.polls.mu.Lock()
	now := time.Now()
	heartbeatDue := !c.polls.nextHeartbeat.IsZero() && !now.Before(c.polls.nextHeartbeat)
	if c.polls.nextHeartbeat.IsZero() || heartbeatDue {
		c.polls.nextHeartbeat = now.Add(pollHeartbeatInterval)
	}
	c.polls.mu.Unlock()
	if heartbeatDue {
		if _, _, err := c.sendPollRequest(ctx, uint32(command.PingCode), nil); err != nil {
			return nil, err
		}
	}
	stamped, refreshed, repeatedPrimary := false, false, false
	lifecycleRetryInterval := replayInterval
	var requestParent *consumerSession
	exact := captured != nil
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if requestParent != nil && !c.matchesPollParent(*requestParent) {
			return nil, ierror.ErrUnauthenticated
		}
		state := pollExchangeState{stamped: stamped, replay: isPartitionMutation(code)}
		route, err := c.consumerRoute(ctx, query)
		if err == nil {
			if requestParent != nil && (requestParent.client != route.parent.client || requestParent.session != route.parent.session) {
				return nil, ierror.ErrUnauthenticated
			}
			if captured == nil {
				value := route.context
				captured = &value
			}
			route.context = *captured
			if !exact {
				route.context.MetadataOp = max(route.context.MetadataOp, route.parent.watermark)
			}
			if !stamped {
				snapshot := c.pollSession.Load()
				if snapshot == nil || snapshot.parent.client != route.parent.client || snapshot.parent.session != route.parent.session {
					return nil, ierror.ErrUnauthenticated
				}
				parent := route.parent
				requestParent = &parent
			}
			vsr.StampPartitionContext((*[vsr.HeaderSize]byte)(frame[:vsr.HeaderSize]), route.context)
			if c.clustered.Load() {
				response, err = c.pollOnRoute(ctx, code, key, frame, route, &state)
				stamped = state.stamped
			} else {
				state.parent = requestParent
				exchangeCtx := context.WithValue(ctx, capturedPartitionContext{}, route.context)
				response, stamped, _, err = c.attempt(context.WithValue(exchangeCtx, singlePollExchange{}, &state), uint32(code), frame, stamped, time.Now(), time.Now().Add(responseReadTimeout))
				if state.written && !state.reusable && err != nil {
					err = ierror.ErrTransientNotCommitted
				}
			}
		}
		if errors.Is(err, ierror.ErrHistoryUnavailable) && code == command.SendMessagesCode && !exact && !refreshed {
			refreshed = true
			captured = nil
			stamped = false
			c.polls.dropRoute(key)
			continue
		}
		if !errors.Is(err, ierror.ErrTransientNotAccepted) && !errors.Is(err, ierror.ErrNotPrimary) && !errors.Is(err, ierror.ErrLifecycleBusy) {
			if errors.Is(err, ierror.ErrHistoryUnavailable) {
				c.polls.dropRoute(key)
			}
			return response, err
		}
		c.polls.dropRoute(key)
		if errors.Is(err, ierror.ErrLifecycleBusy) {
			if !state.written {
				return nil, err
			}
			deadline, _ := ctx.Deadline()
			if time.Until(deadline) <= lifecycleRetryInterval {
				return nil, err
			}
			if waitErr := c.pause(ctx, lifecycleRetryInterval); waitErr != nil {
				return nil, waitErr
			}
			lifecycleRetryInterval = min(2*lifecycleRetryInterval, lifecycleRetryMaxInterval)
			stamped = false
			continue
		}
		if errors.Is(err, ierror.ErrNotPrimary) && !repeatedPrimary {
			repeatedPrimary = true
			continue
		}
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

func isPartitionMutation(code command.Code) bool {
	return code == command.SendMessagesCode || code == command.StoreOffsetCode || code == command.DeleteConsumerOffsetCode
}

func (c *IggyTcpClient) pollOnRoute(ctx context.Context, code command.Code, key string, frame []byte, route pollRoute, state *pollExchangeState) ([]byte, error) {
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
		current := c.pollSession.Load()
		if current == nil || current.parent.client != route.parent.client || current.parent.session != route.parent.session {
			return nil, ierror.ErrTransientNotAccepted
		}
		binding, err := vsr.SerializeBindSession(route.parent.bytes(), iggcon.Version, current.bindSecret)
		if err != nil {
			return nil, err
		}
		response, state, err := slot.client.sendPollRequest(exchangeCtx, uint32(command.BindSessionCode), binding)
		if err != nil {
			// An attach cannot advance an offset, even if its reply is lost.
			state.written = false
			err = slot.exchangeError(ctx, state, err)
			if !state.reusable {
				c.polls.dropConnection(route.endpoint, slot)
			}
			return nil, err
		}
		bound, err := vsr.DecodeLoginRegister(response)
		if err != nil {
			c.polls.dropConnection(route.endpoint, slot)
			return nil, err
		}
		if bound.Session != route.parent.session {
			c.polls.dropConnection(route.endpoint, slot)
			return nil, ierror.SessionMismatch{Requested: route.parent.session, Bound: bound.Session}
		}
		slot.parent = route.parent
		slot.attached = true
	}
	if !c.pollParentCurrent(route.parent) {
		return nil, ierror.ErrTransientNotAccepted
	}
	exchangeCtx = context.WithValue(exchangeCtx, capturedPartitionContext{}, route.context)
	response, stamped, _, err := slot.client.attempt(context.WithValue(exchangeCtx, singlePollExchange{}, state),
		uint32(code), frame, state.stamped, time.Now(), time.Now().Add(responseReadTimeout))
	state.stamped = stamped
	err = slot.exchangeError(ctx, *state, err)
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

func (c *IggyTcpClient) consumerRoute(ctx context.Context, query []byte) (pollRoute, error) {
	key := string(query)
	c.polls.mu.Lock()
	route, cached := c.polls.routes[key]
	c.polls.mu.Unlock()
	if cached && c.pollParentCurrent(route.parent) {
		return route, nil
	}
	snapshot := c.pollSession.Load()
	if snapshot == nil {
		return pollRoute{}, ierror.ErrUnauthenticated
	}
	body, _, err := c.sendPollRequest(ctx, uint32(command.GetPartitionRouteCode), query)
	if err != nil {
		return pollRoute{}, err
	}
	var decoded iggcon.PartitionRoute
	if err := decoded.UnmarshalBinary(body); err != nil {
		return pollRoute{}, err
	}
	if decoded.PartitionsCount == 0 {
		return pollRoute{}, ierror.ErrInvalidCommand
	}
	c.polls.mu.Lock()
	endpoint, known := c.polls.endpoints[decoded.PrimaryReplica]
	c.polls.mu.Unlock()
	if c.clustered.Load() && !known {
		if _, err := c.GetClusterMetadata(ctx); err != nil {
			return pollRoute{}, err
		}
		c.polls.mu.Lock()
		endpoint, known = c.polls.endpoints[decoded.PrimaryReplica]
		c.polls.mu.Unlock()
		if !known {
			return pollRoute{}, ierror.ErrTransientNotAccepted
		}
	}
	parent := snapshot.parent
	parent.watermark = decoded.MetadataOp
	route = pollRoute{context: decoded.Context(), endpoint: endpoint, parent: parent}
	c.polls.mu.Lock()
	defer c.polls.mu.Unlock()
	if !c.matchesPollParent(parent) {
		return pollRoute{}, ierror.ErrTransientNotAccepted
	}
	if c.polls.routes == nil {
		c.polls.routes = make(map[string]pollRoute)
	}
	if len(c.polls.routes) >= maxPollRoutes {
		clear(c.polls.routes)
	}
	c.polls.routes[key] = route
	streamEnd := fieldPrefixSize + int(query[1])
	topicEnd := len(query) - partitionIdSize
	c.topics.setPartitionsCount(topicKey{stream: string(query[:streamEnd]), topic: string(query[streamEnd:topicEnd])}, decoded.PartitionsCount)
	return route, nil
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
	if err := client.Connect(suppressAutoLogin(suppressLeaderSettlement(ctx))); err != nil {
		_ = client.Close()
		return nil, err
	}
	client.mtx.Lock()
	client.session = snapshot.session.Fork()
	client.mtx.Unlock()
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
