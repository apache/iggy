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
	"math"
	"sync"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
)

const (
	maxPollRoutes          = 4096
	pollParametersSize     = 1 + 8 + 4 + 1
	storeOffsetSuffixSize  = 8 + 1
	deleteOffsetSuffixSize = 1
	partitionLookupTimeout = 2 * time.Second
)

type capturedPartitionContext struct{}
type pollRoute struct {
	stale   bool
	context iggcon.PartitionContext
	primary uint8
	view    uint32
	fence   uint64
	age     uint64
	key     laneKey
}
type routeHint struct {
	fence        uint64
	floor        uint32
	view         uint32
	primary      uint8
	preferred    uint8
	hasPreferred bool
	lastFailed   uint8
	hasFailed    bool
}
type pollRouter struct {
	mu                sync.Mutex
	routes            map[string]pollRoute
	hints             map[laneKey]routeHint
	clock             uint64
	lastLookupFailure uint8
	hasLookupFailure  bool
}

func (p *pollRouter) clearRoutes() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, route := range p.routes {
		route.stale = true
		p.routes[key] = route
	}
}
func (p *pollRouter) reset() {
	p.mu.Lock()
	clear(p.routes)
	clear(p.hints)
	p.hasLookupFailure = false
	p.mu.Unlock()
}
func (p *pollRouter) dropRoute(key string) { p.mu.Lock(); delete(p.routes, key); p.mu.Unlock() }
func (p *pollRouter) invalidate(route pollRoute, refused bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	hint, known := p.hints[route.key]
	if !known || hint.fence != route.fence {
		return
	}
	hint.lastFailed, hint.hasFailed = route.primary, true
	if refused {
		floor := route.view
		if floor < math.MaxUint32 {
			floor++
		}
		hint.floor = max(hint.floor, floor)
		hint.preferred = route.primary
		hint.hasPreferred = true
	}
	p.hints[route.key] = hint
	for key, cached := range p.routes {
		if cached.key == route.key {
			cached.stale = true
			p.routes[key] = cached
		}
	}
}

func (c *IggyTcpClient) consumerRoute(ctx context.Context, query []byte) (pollRoute, error) {
	if len(query) < partitionIdSize {
		return pollRoute{}, ierror.ErrInvalidCommand
	}
	c.mtx.Lock()
	fence := c.sessionFence
	bound := c.session.Bound()
	c.mtx.Unlock()
	if !bound {
		return pollRoute{}, ierror.ErrUnauthenticated
	}
	c.links.mu.Lock()
	knownRoster := c.links.hasRoster
	c.links.mu.Unlock()
	if !knownRoster {
		if err := c.refreshRoster(ctx, c.links.any()); err != nil {
			return pollRoute{}, err
		}
	}
	key := string(query)
	watermark := c.metadataWatermark.Load()
	c.polls.mu.Lock()
	cached, found := c.polls.routes[key]
	if found && cached.fence == fence {
		watermark = max(watermark, cached.context.MetadataOp)
	}
	hint := c.polls.hints[cached.key]
	if found && !cached.stale && cached.fence == fence && cached.context.MetadataOp >= watermark && cached.view >= hint.floor && cached.view == hint.view && cached.primary == hint.primary {
		c.polls.mu.Unlock()
		return cached, nil
	}
	var preferred, after *uint8
	if c.polls.hasLookupFailure {
		replica := c.polls.lastLookupFailure
		after = &replica
	}
	if found {
		if hint, ok := c.polls.hints[cached.key]; ok {
			if hint.hasPreferred {
				replica := hint.preferred
				preferred = &replica
				hint.hasPreferred = false
				c.polls.hints[cached.key] = hint
			}
			if hint.hasFailed {
				replica := hint.lastFailed
				after = &replica
			}
		}
	}
	c.polls.mu.Unlock()
	metadataPrimary := c.links.metadata()
	for _, link := range c.links.lookupCandidates(preferred, after) {
		if ctx.Err() != nil {
			return pollRoute{}, ierror.ErrTransientNotAccepted
		}
		attempt, cancel := context.WithTimeout(ctx, partitionLookupTimeout)
		if err := c.bindLink(attempt, link); err != nil {
			cancel()
			if isSessionError(err) {
				c.recoverSession(fence)
				return pollRoute{}, err
			}
			if retryBeforeAdmission(err) {
				c.recordLookupFailure(fence, cached, found, link)
				continue
			}
			return pollRoute{}, err
		}
		body, err := c.readOnLink(attempt, link, command.GetPartitionRouteCode, query, &iggcon.PartitionContext{MetadataOp: watermark})
		cancel()
		if err != nil {
			// A deny with StaleClient closed only this link, and its bind decides.
			if isSessionError(err) && !errors.Is(err, ierror.ErrStaleClient) {
				c.recoverSession(fence)
				return pollRoute{}, err
			}
			if retryBeforeAdmission(err) {
				c.recordLookupFailure(fence, cached, found, link)
				continue
			}
			// Only the metadata primary can confirm a miss; backups may lag a new namespace.
			if link != metadataPrimary && (errors.Is(err, ierror.ErrStreamIdNotFound) ||
				errors.Is(err, ierror.ErrStreamNameNotFound) || errors.Is(err, ierror.ErrTopicIdNotFound) ||
				errors.Is(err, ierror.ErrTopicNameNotFound) || errors.Is(err, ierror.ErrPartitionNotFound)) {
				c.recordLookupFailure(fence, cached, found, link)
				continue
			}
			return pollRoute{}, err
		}
		var decoded iggcon.PartitionRoute
		if err := decoded.UnmarshalBinary(body); err != nil {
			return pollRoute{}, err
		}
		if decoded.PartitionsCount == 0 || decoded.MetadataOp < watermark {
			c.recordLookupFailure(fence, cached, found, link)
			continue
		}
		route, err := c.acceptRoute(fence, key, query, decoded)
		if err == nil {
			return route, nil
		}
		if !errors.Is(err, ierror.ErrTransientNotAccepted) {
			return pollRoute{}, err
		}
		c.recordLookupFailure(fence, cached, found, link)
	}
	return pollRoute{}, ierror.ErrTransientNotAccepted
}

func (c *IggyTcpClient) recordLookupFailure(fence uint64, cached pollRoute, found bool, link *tcpLink) {
	replica, known := c.links.replicaID(link)
	if !known {
		return
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.sessionFence != fence {
		return
	}
	c.polls.mu.Lock()
	defer c.polls.mu.Unlock()
	c.polls.lastLookupFailure, c.polls.hasLookupFailure = replica, true
	if !found {
		return
	}
	if hint, ok := c.polls.hints[cached.key]; ok {
		hint.lastFailed, hint.hasFailed = replica, true
		c.polls.hints[cached.key] = hint
	}
}

func (c *IggyTcpClient) acceptRoute(fence uint64, key string, query []byte, decoded iggcon.PartitionRoute) (pollRoute, error) {
	route := pollRoute{context: decoded.Context(), primary: decoded.PrimaryReplica, view: decoded.View, fence: fence, key: laneKey{incarnation: decoded.Incarnation, partition: binary.LittleEndian.Uint32(query[len(query)-partitionIdSize:])}}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.sessionFence != fence || decoded.MetadataOp < c.metadataWatermark.Load() {
		return pollRoute{}, ierror.ErrTransientNotAccepted
	}
	c.polls.mu.Lock()
	defer c.polls.mu.Unlock()
	if c.polls.routes == nil {
		c.polls.routes = make(map[string]pollRoute)
		c.polls.hints = make(map[laneKey]routeHint)
	}
	hint, known := c.polls.hints[route.key]
	if route.view < hint.floor || known && (route.view < hint.view || route.view == hint.view && route.primary != hint.primary) {
		return pollRoute{}, ierror.ErrTransientNotAccepted
	}
	if len(c.polls.routes) >= maxPollRoutes {
		var oldest string
		age := uint64(math.MaxUint64)
		for candidate, entry := range c.polls.routes {
			if entry.age < age {
				oldest = candidate
				age = entry.age
			}
		}
		delete(c.polls.routes, oldest)
	}
	// Hints outlive eviction of an alias and remain bounded by the same group limit.
	if !known && len(c.polls.hints) >= maxPollRoutes {
		for group := range c.polls.hints {
			active := false
			for _, entry := range c.polls.routes {
				if entry.key == group {
					active = true
					break
				}
			}
			if !active {
				delete(c.polls.hints, group)
				break
			}
		}
		if len(c.polls.hints) >= maxPollRoutes {
			return pollRoute{}, ierror.ErrTransientNotAccepted
		}
	}
	c.polls.hints[route.key] = routeHint{fence: route.fence, floor: hint.floor, view: route.view, primary: route.primary}
	c.polls.clock++
	route.age = c.polls.clock
	c.polls.routes[key] = route
	streamEnd := fieldPrefixSize + int(query[1])
	topicEnd := len(query) - partitionIdSize
	c.topics.setPartitionsCount(topicKey{stream: string(query[:streamEnd]), topic: string(query[streamEnd:topicEnd])}, decoded.PartitionsCount)
	return route, nil
}

func (c *IggyTcpClient) sendRouted(ctx context.Context, code command.Code, query, frame []byte, captured *iggcon.PartitionContext) ([]byte, error) {
	return c.drive(ctx, code, query, frame, captured)
}
