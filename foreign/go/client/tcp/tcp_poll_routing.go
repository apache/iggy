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
	"slices"
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

type pollRoute struct {
	context iggcon.PartitionContext
	primary uint8
	view    uint32
	fence   uint64
	age     uint64
	key     laneKey
	// generation is the generation of the hint that the route was accepted
	// under. The route is stale once the hint moves past it.
	generation uint64
	// cleared is the count of route clears when the route was accepted. Any
	// later clear makes the route stale.
	cleared uint64
}
type routeHint struct {
	fence         uint64
	floor         uint32
	view          uint32
	primary       uint8
	preferred     uint8
	hasPreferred  bool
	lastFailed    uint8
	hasFailed     bool
	metadataFirst bool
	generation    uint64
}
type routeAge struct {
	key string
	age uint64
}
type pollRouter struct {
	mu     sync.Mutex
	routes map[string]pollRoute
	hints  map[laneKey]routeHint
	// aliases counts the routes of each partition history. A hint whose
	// history has none is idle, and only an idle hint makes room for another.
	aliases map[laneKey]int
	idle    map[laneKey]struct{}
	// ages holds the route keys oldest first, from agesHead. A replaced or
	// dropped route leaves its entry until an eviction or a compaction skips it.
	ages              []routeAge
	agesHead          int
	clock             uint64
	cleared           uint64
	lastLookupFailure uint8
	hasLookupFailure  bool
}

// clearRoutes marks every cached route stale. A stale route still gives its
// lookup hints and its metadata floor.
func (p *pollRouter) clearRoutes() {
	p.mu.Lock()
	p.cleared++
	p.mu.Unlock()
}
func (p *pollRouter) reset() {
	p.mu.Lock()
	clear(p.routes)
	clear(p.hints)
	clear(p.aliases)
	clear(p.idle)
	p.ages, p.agesHead = nil, 0
	p.hasLookupFailure = false
	p.mu.Unlock()
}
func (p *pollRouter) dropRoute(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if route, cached := p.routes[key]; cached {
		p.deleteRouteLocked(key, route)
	}
}

// storeLocked caches route under key, in place of a route that key had.
func (p *pollRouter) storeLocked(key string, route pollRoute) {
	replaced, cached := p.routes[key]
	p.routes[key] = route
	if !cached || replaced.key != route.key {
		if cached {
			p.retireAliasLocked(replaced.key)
		}
		p.aliases[route.key]++
		delete(p.idle, route.key)
	}
	if len(p.ages) == cap(p.ages) && len(p.ages) > 0 {
		p.compactAgesLocked()
	}
	p.ages = append(p.ages, routeAge{key: key, age: route.age})
}

func (p *pollRouter) deleteRouteLocked(key string, route pollRoute) {
	delete(p.routes, key)
	p.retireAliasLocked(route.key)
}

func (p *pollRouter) retireAliasLocked(group laneKey) {
	if aliases := p.aliases[group] - 1; aliases > 0 {
		p.aliases[group] = aliases
		return
	}
	delete(p.aliases, group)
	p.idle[group] = struct{}{}
}

// evictOldestLocked drops the route that was accepted first.
func (p *pollRouter) evictOldestLocked() {
	for p.agesHead < len(p.ages) {
		oldest := p.ages[p.agesHead]
		p.ages[p.agesHead] = routeAge{}
		p.agesHead++
		if route, cached := p.routes[oldest.key]; cached && route.age == oldest.age {
			p.deleteRouteLocked(oldest.key, route)
			return
		}
	}
}

// compactAgesLocked drops the entries of replaced and dropped routes. It grows
// the buffer when that frees less than half of it, so each accepted route
// costs constant amortized work.
func (p *pollRouter) compactAgesLocked() {
	live := p.ages[:0]
	for _, entry := range p.ages[p.agesHead:] {
		if route, cached := p.routes[entry.key]; cached && route.age == entry.age {
			live = append(live, entry)
		}
	}
	clear(p.ages[len(live):])
	p.ages, p.agesHead = live, 0
	if len(live) > cap(live)/2 {
		p.ages = slices.Grow(live, len(live))
	}
}

// evictIdleHintLocked drops the hint of a history that has no route left.
func (p *pollRouter) evictIdleHintLocked() {
	for group := range p.idle {
		delete(p.idle, group)
		delete(p.hints, group)
		return
	}
}
func (p *pollRouter) invalidate(route pollRoute, refused bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	hint, known := p.staleLocked(route)
	if !known {
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
}

// relookup starts again at the metadata primary after a missing namespace,
// a lost partition history or a change of consumer group ownership.
func (p *pollRouter) relookup(route pollRoute) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if hint, known := p.staleLocked(route); known {
		hint.metadataFirst = true
		hint.hasPreferred = false
		p.hints[route.key] = hint
	}
}

// staleLocked marks every alias of the partition history of route stale and
// returns its hint, unless route belongs to another session.
func (p *pollRouter) staleLocked(route pollRoute) (routeHint, bool) {
	hint, known := p.hints[route.key]
	if !known || hint.fence != route.fence {
		return routeHint{}, false
	}
	hint.generation++
	p.hints[route.key] = hint
	return hint, true
}

// consumerRoute returns the route of the partition that query names, cached or
// looked up. An ended caller epoch prevents the partition request from being
// stamped under a later sign-in. The epoch checks between lookup steps do not
// fence the auxiliary roster reads themselves.
func (c *IggyTcpClient) consumerRoute(ctx context.Context, query []byte) (_ pollRoute, err error) {
	if len(query) < partitionIdSize {
		return pollRoute{}, ierror.ErrInvalidCommand
	}
	epoch := c.callEpoch(ctx)
	defer func() {
		if ended := epoch.err(); err != nil && ended != nil {
			err = ended
		}
	}()
	c.mtx.Lock()
	fence := c.sessionFence
	bound := c.session.Bound()
	c.mtx.Unlock()
	if err := epoch.err(); err != nil {
		return pollRoute{}, err
	}
	if !bound {
		return pollRoute{}, ierror.ErrUnauthenticated
	}
	if !c.links.hasRoster.Load() {
		if err := c.refreshRoster(ctx, c.links.any()); err != nil {
			return pollRoute{}, err
		}
	}
	watermark := c.metadataWatermark.Load()
	c.polls.mu.Lock()
	cached, found := c.polls.routes[string(query)]
	if found && cached.fence == fence {
		watermark = max(watermark, cached.context.MetadataOp)
	}
	hint := c.polls.hints[cached.key]
	if found && cached.cleared == c.polls.cleared && cached.generation == hint.generation && cached.fence == fence && cached.context.MetadataOp >= watermark && cached.view >= hint.floor && cached.view == hint.view && cached.primary == hint.primary {
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
			if hint.metadataFirst {
				after = nil
			}
		}
	}
	c.polls.mu.Unlock()
	for _, link := range c.links.lookupCandidates(preferred, after) {
		if ctx.Err() != nil {
			return pollRoute{}, ierror.ErrTransientNotAccepted
		}
		if err := epoch.err(); err != nil {
			return pollRoute{}, err
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
			if isNamespaceMiss(err) && !c.isMetadataPrimary(ctx, link) {
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
		// Only an accepted route stores its key, so a cached lookup copies nothing.
		route, err := c.acceptRoute(fence, string(query), query, decoded)
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

// isNamespaceMiss reports whether err tells that a replica has no such stream,
// topic or partition.
func isNamespaceMiss(err error) bool {
	return errors.Is(err, ierror.ErrStreamIdNotFound) || errors.Is(err, ierror.ErrStreamNameNotFound) ||
		errors.Is(err, ierror.ErrTopicIdNotFound) || errors.Is(err, ierror.ErrTopicNameNotFound) ||
		errors.Is(err, ierror.ErrPartitionNotFound)
}

// isMetadataPrimary reports whether link reaches the metadata primary. Only its
// namespace miss is final, because a backup can lag a namespace that another
// client just created. The belief can predate a view change, so a roster read
// on link must still name the replica of link. No lookup reaches a believed
// metadata primary without a ready link, so then the roster on link is read
// to update the belief.
func (c *IggyTcpClient) isMetadataPrimary(ctx context.Context, link *tcpLink) bool {
	replica, known := c.links.replicaID(link)
	if !known {
		return false
	}
	if believed, named := c.links.metadataPrimary(); named && believed != replica && c.links.replica(believed) != nil {
		return false
	}
	if c.refreshRoster(ctx, link) != nil {
		return false
	}
	believed, named := c.links.metadataPrimary()
	return named && believed == replica
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
		c.polls.aliases = make(map[laneKey]int)
		c.polls.idle = make(map[laneKey]struct{})
	}
	hint, known := c.polls.hints[route.key]
	if route.view < hint.floor || known && (route.view < hint.view || route.view == hint.view && route.primary != hint.primary) {
		return pollRoute{}, ierror.ErrTransientNotAccepted
	}
	if len(c.polls.routes) >= maxPollRoutes {
		c.polls.evictOldestLocked()
	}
	// Hints outlive eviction of an alias and remain bounded by the same group limit.
	if !known && len(c.polls.hints) >= maxPollRoutes {
		c.polls.evictIdleHintLocked()
		if len(c.polls.hints) >= maxPollRoutes {
			return pollRoute{}, ierror.ErrTransientNotAccepted
		}
	}
	c.polls.hints[route.key] = routeHint{fence: route.fence, floor: hint.floor, view: route.view, primary: route.primary, generation: hint.generation}
	c.polls.clock++
	route.age = c.polls.clock
	route.generation = hint.generation
	route.cleared = c.polls.cleared
	c.polls.storeLocked(key, route)
	streamEnd := fieldPrefixSize + int(query[1])
	topicEnd := len(query) - partitionIdSize
	c.topics.setPartitionsCount(topicKey{stream: string(query[:streamEnd]), topic: string(query[streamEnd:topicEnd])}, decoded.PartitionsCount)
	return route, nil
}
