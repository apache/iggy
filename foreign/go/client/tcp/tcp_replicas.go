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
	"errors"
	"math/rand/v2"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
)

const (
	maxReplicaSlots     = 256
	maxConcurrentDials  = 4
	reconnectBackoffCap = 10 * time.Second
)

type connectAttempt struct {
	done chan struct{}
	link *tcpLink
	err  error
}
type replicaSlot struct {
	endpoint         string
	link             *tcpLink
	cancel           context.CancelFunc
	retriesExhausted bool
}

type replicaLinks struct {
	client          *IggyTcpClient
	mu              sync.Mutex
	bootstrap       *tcpLink
	slots           map[uint8]*replicaSlot
	primary         uint8
	hasPrimary      bool
	hasRoster       atomic.Bool // changes under mu, read without it by routed calls
	roster          []string
	rosterOrder     []uint8
	ctx             context.Context
	cancel          context.CancelFunc
	heartbeatCancel context.CancelFunc
	connectAttempt  *connectAttempt
	permits         chan struct{}
	tasks           *sync.WaitGroup
}

func newReplicaLinks(client *IggyTcpClient) *replicaLinks {
	return &replicaLinks{client: client, slots: make(map[uint8]*replicaSlot), permits: make(chan struct{}, maxConcurrentDials)}
}

func linkReady(link *tcpLink) bool {
	if link == nil {
		return false
	}
	select {
	case <-link.closed:
		return false
	default:
		return true
	}
}

func (r *replicaLinks) connect(ctx context.Context) (*tcpLink, error) {
	r.mu.Lock()
	if err := ctx.Err(); err != nil {
		r.mu.Unlock()
		return nil, err
	}
	select {
	case <-r.client.closed:
		r.mu.Unlock()
		return nil, ierror.ErrClientShutdown
	default:
	}
	if link := r.anyLocked(); link != nil {
		r.mu.Unlock()
		return link, nil
	}
	if attempt := r.connectAttempt; attempt != nil {
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-attempt.done:
			return attempt.link, attempt.err
		}
	}
	if r.ctx == nil {
		r.ctx, r.cancel = context.WithCancel(context.Background())
		r.tasks = &sync.WaitGroup{}
	}
	lifetime := r.ctx
	candidates := append([]string{r.client.config.serverAddress}, r.roster...)
	attempt := &connectAttempt{done: make(chan struct{})}
	r.connectAttempt = attempt
	r.mu.Unlock()
	dialCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(lifetime, cancel)
	defer cancel()
	defer stop()
	seen := make(map[string]bool)
	addresses := make([]string, 0, len(candidates))
	for _, address := range candidates {
		if address != "" && !seen[address] {
			seen[address] = true
			addresses = append(addresses, address)
		}
	}
	var err error = ierror.ErrCannotEstablishConnection
	var link *tcpLink
	for pass := uint32(0); len(addresses) > 0; pass++ {
		var configurationError error
		for _, address := range addresses {
			if dialCtx.Err() != nil {
				err = dialCtx.Err()
				break
			}
			connection, dialErr := r.dial(dialCtx, address)
			if dialErr == nil {
				link = newTCPLink(connection, address)
				err = nil
				break
			}
			err = dialErr
			if isTLSConfigFault(err) {
				configurationError = err
			}
		}
		if link != nil || dialCtx.Err() != nil {
			break
		}
		config := r.client.config.reconnection
		if configurationError != nil {
			err = configurationError
			break
		}
		if !config.enabled || config.maxRetries > 0 && pass >= config.maxRetries {
			break
		}
		if waitErr := r.client.pause(dialCtx, reconnectDelay(config.interval, pass)); waitErr != nil {
			err = waitErr
			break
		}
	}
	r.mu.Lock()
	if r.ctx != lifetime || lifetime.Err() != nil {
		if link != nil {
			link.close(ierror.ErrClientShutdown)
		}
		link = nil
		err = ierror.ErrNotConnected
	} else if link != nil {
		r.bootstrap = link
	}
	attempt.link, attempt.err = link, err
	if r.connectAttempt == attempt {
		r.connectAttempt = nil
	}
	close(attempt.done)
	r.mu.Unlock()
	return link, err
}

func (r *replicaLinks) dial(ctx context.Context, address string) (net.Conn, error) {
	select {
	case r.permits <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-r.permits }()
	return r.client.dialCandidate(ctx, address)
}

func reconnectDelay(base time.Duration, attempt uint32) time.Duration {
	if base <= 0 {
		base = replayInterval
	}
	cap := min(base, reconnectBackoffCap)
	for range min(attempt, uint32(32)) {
		cap = min(cap*2, reconnectBackoffCap)
	}
	return time.Duration(rand.Int64N(int64(cap) + 1))
}

func (r *replicaLinks) install(metadata *iggcon.ClusterMetadata) error {
	if len(metadata.Nodes) == 0 || len(metadata.Nodes) > maxReplicaSlots {
		return ierror.ErrInvalidCommand
	}
	endpoints := make(map[uint8]string, len(metadata.Nodes))
	order := make([]uint8, 0, len(metadata.Nodes))
	seen := make(map[uint8]bool, len(metadata.Nodes))
	var primary uint8
	hasPrimary := false
	for _, node := range metadata.Nodes {
		if seen[node.ReplicaID] {
			return ierror.ErrInvalidCommand
		}
		seen[node.ReplicaID] = true
		if node.Endpoints.Tcp == 0 {
			continue
		}
		endpoint := net.JoinHostPort(strings.Trim(node.IP, "[]"), strconv.Itoa(int(node.Endpoints.Tcp)))
		endpoints[node.ReplicaID] = endpoint
		order = append(order, node.ReplicaID)
		if node.Role == iggcon.RoleLeader {
			if hasPrimary {
				return ierror.ErrInvalidCommand
			}
			primary = node.ReplicaID
			hasPrimary = true
		}
	}
	// A single node advertises its own view of its address, such as a bind
	// address or a container name. Only the configured endpoint is known to
	// reach it.
	if len(order) == 1 {
		endpoints[order[0]] = r.client.config.serverAddress
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ctx == nil || r.ctx.Err() != nil {
		return ierror.ErrNotConnected
	}
	r.primary, r.hasPrimary = primary, hasPrimary
	r.hasRoster.Store(true)
	r.roster = r.roster[:0]
	r.rosterOrder = order
	for id, slot := range r.slots {
		if endpoints[id] != slot.endpoint {
			slot.cancel()
			if slot.link != nil {
				slot.link.close(ierror.ErrTransientNotCommitted)
			}
			delete(r.slots, id)
		}
	}
	for _, id := range order {
		endpoint := endpoints[id]
		r.roster = append(r.roster, endpoint)
		if r.slots[id] != nil {
			continue
		}
		slot := &replicaSlot{endpoint: endpoint}
		if linkReady(r.bootstrap) && (len(endpoints) == 1 || r.bootstrap.endpoint == endpoint) {
			slot.link = r.bootstrap
			r.bootstrap = nil
		}
		r.slots[id] = slot
		r.startSlotLocked(id, slot)
	}
	r.retireBootstrapLocked()
	return nil
}

func (r *replicaLinks) retireBootstrapLocked() {
	if r.bootstrap != nil && r.hasPrimary {
		if slot := r.slots[r.primary]; slot != nil && linkReady(slot.link) {
			r.bootstrap.close(ierror.ErrTransientNotCommitted)
			r.bootstrap = nil
		}
	}
}

func (r *replicaLinks) runSlot(ctx context.Context, id uint8, slot *replicaSlot) {
	exhausted := false
	defer func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if r.slots[id] == slot && ctx.Err() == nil {
			slot.retriesExhausted = exhausted
		}
	}()
	failures := uint32(0)
	initial := true
	for {
		r.mu.Lock()
		link := slot.link
		r.mu.Unlock()
		if linkReady(link) {
			initial = false
			connectedAt := time.Now()
			select {
			case <-ctx.Done():
				return
			case <-link.closed:
			}
			if time.Since(connectedAt) >= r.client.config.reconnection.reestablishAfter {
				failures = 0
			}
		}
		configuration := r.client.config.reconnection
		if !initial && (!configuration.enabled || configuration.maxRetries > 0 && failures > configuration.maxRetries) {
			exhausted = configuration.enabled
			return
		}
		if err := r.client.pause(ctx, reconnectDelay(configuration.interval, failures)); err != nil {
			return
		}
		initial = false
		connection, err := r.dial(ctx, slot.endpoint)
		failures++
		if err != nil {
			if isTLSConfigFault(err) {
				return
			}
			continue
		}
		link = newTCPLink(connection, slot.endpoint)
		bindCtx, cancel := context.WithTimeout(ctx, responseReadTimeout)
		r.client.mtx.Lock()
		fence := r.client.sessionFence
		r.client.mtx.Unlock()
		err = r.client.bindLink(bindCtx, link)
		cancel()
		if err != nil {
			link.close(err)
			if isSessionError(err) {
				r.client.recoverSession(fence)
				return
			}
			continue
		}
		r.mu.Lock()
		if r.slots[id] != slot || ctx.Err() != nil {
			r.mu.Unlock()
			link.close(ierror.ErrClientShutdown)
			return
		}
		slot.link = link
		r.retireBootstrapLocked()
		r.mu.Unlock()
	}
}

func (r *replicaLinks) startSlotLocked(id uint8, slot *replicaSlot) {
	if slot.cancel != nil {
		slot.cancel()
	}
	ctx, cancel := context.WithCancel(r.ctx)
	slot.cancel = cancel
	slot.retriesExhausted = false
	tasks := r.tasks
	tasks.Add(1)
	go func() { defer tasks.Done(); r.runSlot(ctx, id, slot) }()
}

func (r *replicaLinks) anyLocked() *tcpLink {
	if r.hasPrimary {
		if slot := r.slots[r.primary]; slot != nil && linkReady(slot.link) {
			return slot.link
		}
	}
	if linkReady(r.bootstrap) {
		return r.bootstrap
	}
	for id, slot := range r.slots {
		if id != r.primary && linkReady(slot.link) {
			return slot.link
		}
	}
	if slot := r.slots[r.primary]; slot != nil && linkReady(slot.link) {
		return slot.link
	}
	return nil
}

func (r *replicaLinks) invalidateMetadataPrimary(link *tcpLink) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if slot := r.slots[r.primary]; slot != nil && slot.link == link {
		r.hasPrimary = false
	}
}

func (r *replicaLinks) metadata() *tcpLink {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hasPrimary {
		if slot := r.slots[r.primary]; slot != nil && linkReady(slot.link) {
			return slot.link
		}
		return nil
	}
	if r.hasRoster.Load() {
		return nil
	}
	return r.anyLocked()
}

// metadataPrimary is the replica that the client believes is the metadata
// primary.
func (r *replicaLinks) metadataPrimary() (uint8, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.primary, r.hasPrimary
}

func (r *replicaLinks) any() *tcpLink { r.mu.Lock(); defer r.mu.Unlock(); return r.anyLocked() }
func (r *replicaLinks) replica(id uint8) *tcpLink {
	r.mu.Lock()
	defer r.mu.Unlock()
	if slot := r.slots[id]; slot != nil && linkReady(slot.link) {
		return slot.link
	}
	return nil
}

func (r *replicaLinks) lookupCandidates(preferred, after *uint8) []*tcpLink {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := r.rosterOrder
	links := make([]*tcpLink, 0, len(ids)+1)
	start := 0
	first, known := r.primary, r.hasPrimary
	if preferred != nil {
		first, known = *preferred, true
	} else if after != nil {
		first, known = *after, true
	}
	if known {
		if index := slices.Index(ids, first); index >= 0 {
			start = index
			if preferred == nil && after != nil {
				start++
			}
		}
	}
	for offset := range ids {
		id := ids[(start+offset)%len(ids)]
		if slot := r.slots[id]; slot != nil && linkReady(slot.link) {
			link := slot.link
			links = append(links, link)
		}
	}
	if linkReady(r.bootstrap) && !slices.Contains(links, r.bootstrap) {
		links = append(links, r.bootstrap)
	}
	return links
}

func (r *replicaLinks) replicaID(link *tcpLink) (uint8, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, slot := range r.slots {
		if slot.link == link {
			return id, true
		}
	}
	return 0, false
}

func (r *replicaLinks) contains(id uint8) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.slots[id] != nil
}

func (r *replicaLinks) address() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hasPrimary {
		if slot := r.slots[r.primary]; slot != nil {
			return slot.endpoint
		}
	}
	if r.bootstrap != nil {
		return r.bootstrap.endpoint
	}
	return r.client.config.serverAddress
}

func (r *replicaLinks) startHeartbeat() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.heartbeatCancel != nil || r.ctx == nil || r.client.config.heartbeatInterval <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(r.ctx)
	r.heartbeatCancel = cancel
	tasks := r.tasks
	tasks.Add(1)
	go func() {
		defer tasks.Done()
		ticker := time.NewTicker(r.client.config.heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			r.mu.Lock()
			if ctx.Err() != nil {
				r.mu.Unlock()
				return
			}
			ready := make([]*tcpLink, 0, len(r.slots)+1)
			if linkReady(r.bootstrap) {
				ready = append(ready, r.bootstrap)
			}
			for id, slot := range r.slots {
				if slot.retriesExhausted && r.client.config.reconnection.enabled {
					r.startSlotLocked(id, slot)
				}
				if linkReady(slot.link) {
					ready = append(ready, slot.link)
				}
			}
			r.mu.Unlock()
			var pingTasks sync.WaitGroup
			for _, link := range ready {
				pingTasks.Add(1)
				go func(link *tcpLink) {
					defer pingTasks.Done()
					pingCtx, pingCancel := context.WithTimeout(ctx, r.client.config.heartbeatInterval/2)
					defer pingCancel()
					r.client.mtx.Lock()
					fence := r.client.sessionFence
					r.client.mtx.Unlock()
					_, err := r.client.readOnLink(pingCtx, link, command.PingCode, nil, nil)
					if isSessionError(err) {
						r.client.recoverSession(fence)
					}
				}(link)
			}
			pingTasks.Wait()
		}
	}()
}

func (r *replicaLinks) stopHeartbeat() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.heartbeatCancel != nil {
		r.heartbeatCancel()
		r.heartbeatCancel = nil
	}
}

func (r *replicaLinks) disconnect(wait bool) {
	r.detach()(wait)
}

// detach removes ownership; its result closes sockets outside the session lock.
func (r *replicaLinks) detach() func(bool) {
	r.mu.Lock()
	if r.cancel != nil {
		r.cancel()
	}
	tasks := r.tasks
	attempt := r.connectAttempt
	r.ctx = nil
	r.cancel = nil
	r.heartbeatCancel = nil
	r.tasks = nil
	links := make(map[*tcpLink]struct{}, len(r.slots)+1)
	if r.bootstrap != nil {
		links[r.bootstrap] = struct{}{}
		r.bootstrap = nil
	}
	for _, slot := range r.slots {
		slot.cancel()
		if slot.link != nil {
			links[slot.link] = struct{}{}
		}
	}
	clear(r.slots)
	r.hasPrimary = false
	r.hasRoster.Store(false)
	r.mu.Unlock()
	return func(wait bool) {
		for link := range links {
			link.close(ierror.ErrClientShutdown)
		}
		if !wait {
			return
		}
		if attempt != nil {
			<-attempt.done
		}
		if tasks != nil {
			tasks.Wait()
		}
		for link := range links {
			link.done.Wait()
		}
	}
}

func isSessionError(err error) bool {
	return errors.Is(err, ierror.ErrUnauthenticated) || errors.Is(err, ierror.ErrStaleClient) || errors.Is(err, ierror.ErrSessionMismatch)
}
