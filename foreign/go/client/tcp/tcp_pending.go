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
	"sync"
	"sync/atomic"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
)

const (
	DefaultMaxPendingOperations     = 256
	DefaultMaxPendingOperationBytes = 64 * 1024 * 1024
)

type laneKey struct {
	incarnation uint64
	partition   uint32
}

type mutationLane struct {
	active   bool
	retained *pendingOperation
	waiters  []*laneWaiter
}

// A nonzero size gives each FIFO waiter a distinct pointer identity.
type laneWaiter byte

// The registry owns logical admission. Frames own their physical byte reservation
// until both the operation and every queued writer release their references.
type operationRegistry struct {
	mu            sync.Mutex
	fence         uint64
	lanes         map[laneKey]*mutationLane
	retained      map[uint64]*pendingOperation
	reservations  map[*frameReservation]struct{}
	bytes         int
	maxOperations int
	maxBytes      int
	changed       chan struct{}
	// The outcomes that replays proved, oldest first and at most maxOperations.
	// A reset keeps them, because request IDs never repeat within a client.
	proved []provedOutcome
}

// provedOutcome is the committed result of a replayed operation: nil for a
// success, else the committed rejection.
type provedOutcome struct {
	id  uint64
	err error
}

type frameReservation struct {
	owner        *operationRegistry
	size         int
	slotReleased bool
}

type operationFrame struct {
	data        []byte
	refs        atomic.Int32
	reservation *frameReservation
}

func newOperationFrame(data []byte, reservation *frameReservation) *operationFrame {
	frame := &operationFrame{data: data, reservation: reservation}
	frame.refs.Store(1)
	return frame
}

func (f *operationFrame) acquire() *operationFrame { f.refs.Add(1); return f }

func (f *operationFrame) release() {
	if f.refs.Add(-1) != 0 {
		return
	}
	f.data = nil
	if reservation := f.reservation; reservation != nil {
		owner := reservation.owner
		owner.mu.Lock()
		owner.releaseSlotLocked(reservation)
		owner.bytes -= reservation.size
		owner.notifyLocked()
		owner.mu.Unlock()
	}
}

type pendingOperation struct {
	id    uint64
	code  command.Code
	query []byte
	key   laneKey
	fence uint64
	frame *operationFrame
}

type laneTicket struct {
	owner       *operationRegistry
	key         laneKey
	lane        *mutationLane
	fence       uint64
	operation   *pendingOperation
	reservation *frameReservation
	resumed     bool
	finished    bool
	proved      bool
	outcome     error
}

func newOperationRegistry(maxOperations, maxBytes int) *operationRegistry {
	return &operationRegistry{fence: 1, lanes: make(map[laneKey]*mutationLane), retained: make(map[uint64]*pendingOperation), reservations: make(map[*frameReservation]struct{}), maxOperations: maxOperations, maxBytes: maxBytes, changed: make(chan struct{})}
}

func (r *operationRegistry) notifyLocked() { close(r.changed); r.changed = make(chan struct{}) }

func (r *operationRegistry) enter(ctx context.Context, key laneKey) (*laneTicket, error) {
	r.mu.Lock()
	fence := r.fence
	lane := r.lanes[key]
	if lane == nil {
		lane = &mutationLane{}
		r.lanes[key] = lane
	}
	if lane.retained != nil {
		id := lane.retained.id
		r.mu.Unlock()
		return nil, ierror.LaneBlocked{OperationID: id}
	}
	waiter := new(laneWaiter)
	lane.waiters = append(lane.waiters, waiter)
	for {
		if fence != r.fence {
			r.mu.Unlock()
			return nil, ierror.ErrUnauthenticated
		}
		if lane.retained != nil {
			id := lane.retained.id
			r.removeWaiterLocked(key, lane, waiter)
			r.mu.Unlock()
			return nil, ierror.LaneBlocked{OperationID: id}
		}
		if ctx.Err() != nil {
			r.removeWaiterLocked(key, lane, waiter)
			r.mu.Unlock()
			return nil, ierror.ErrTransientNotAccepted
		}
		if !lane.active && lane.waiters[0] == waiter {
			lane.waiters = lane.waiters[1:]
			lane.active = true
			r.mu.Unlock()
			return &laneTicket{owner: r, key: key, lane: lane, fence: fence}, nil
		}
		changed := r.changed
		r.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		r.mu.Lock()
	}
}

func (r *operationRegistry) removeWaiterLocked(key laneKey, lane *mutationLane, waiter *laneWaiter) {
	for index, candidate := range lane.waiters {
		if candidate == waiter {
			lane.waiters = append(lane.waiters[:index], lane.waiters[index+1:]...)
			break
		}
	}
	r.cleanLaneLocked(key, lane)
	r.notifyLocked()
}

func (r *operationRegistry) cleanLaneLocked(key laneKey, lane *mutationLane) {
	if !lane.active && lane.retained == nil && len(lane.waiters) == 0 && r.lanes[key] == lane {
		delete(r.lanes, key)
	}
}

func (t *laneTicket) reserve(ctx context.Context, size int) error {
	r := t.owner
	r.mu.Lock()
	defer r.mu.Unlock()
	if size < vsr.HeaderSize || size > r.maxBytes || r.maxOperations <= 0 {
		return ierror.ErrInvalidSizeBytes
	}
	for {
		if t.fence != r.fence {
			return ierror.ErrUnauthenticated
		}
		if ctx.Err() != nil {
			return ierror.ErrTransientNotAccepted
		}
		if len(r.reservations) < r.maxOperations && size <= r.maxBytes-r.bytes {
			reservation := &frameReservation{owner: r, size: size}
			r.reservations[reservation] = struct{}{}
			r.bytes += size
			t.reservation = reservation
			return nil
		}
		changed := r.changed
		r.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		r.mu.Lock()
	}
}

func (r *operationRegistry) releaseSlotLocked(reservation *frameReservation) {
	if reservation == nil || reservation.slotReleased {
		return
	}
	reservation.slotReleased = true
	delete(r.reservations, reservation)
}

func (t *laneTicket) freeze(code command.Code, query []byte, frame []byte) *pendingOperation {
	operation := &pendingOperation{id: vsr.StampedRequestID(frame), code: code, query: append([]byte(nil), query...), key: t.key, fence: t.fence, frame: newOperationFrame(frame, t.reservation)}
	t.operation = operation
	return operation
}

func (t *laneTicket) releaseOperation() {
	r := t.owner
	r.mu.Lock()
	operation := t.operation
	r.releaseSlotLocked(t.reservation)
	t.operation = nil
	t.reservation = nil
	r.notifyLocked()
	r.mu.Unlock()
	if operation != nil {
		operation.frame.release()
	}
}

func (t *laneTicket) finish(retain bool) {
	r := t.owner
	r.mu.Lock()
	if t.finished {
		r.mu.Unlock()
		return
	}
	t.finished = true
	operation := t.operation
	if t.proved && t.resumed {
		// Recorded with the release under one lock, so a resolve that no longer
		// finds the operation finds its outcome, also after an abandon.
		r.proved = append(r.proved, provedOutcome{id: operation.id, err: t.outcome})
		if len(r.proved) > r.maxOperations {
			r.proved = r.proved[1:]
		}
	}
	current := t.fence == r.fence && (!t.resumed || t.lane.retained == operation)
	if current {
		t.lane.active = false
		if retain && operation != nil {
			if !t.resumed {
				operation.frame.acquire()
			}
			r.retained[operation.id] = operation
			t.lane.retained = operation
		} else {
			if t.resumed && t.lane.retained == operation {
				delete(r.retained, operation.id)
				t.lane.retained = nil
				operation.frame.releaseRegistryLocked(r)
			}
			r.releaseSlotLocked(t.reservation)
		}
		r.cleanLaneLocked(t.key, t.lane)
	} else if t.fence == r.fence {
		// An abandon leaves the lane to a running replay, so its end frees it.
		t.lane.active = false
		r.cleanLaneLocked(t.key, t.lane)
	}
	if operation == nil && t.reservation != nil {
		r.releaseSlotLocked(t.reservation)
		r.bytes -= t.reservation.size
	}
	r.notifyLocked()
	r.mu.Unlock()
	if operation != nil {
		operation.frame.release()
	}
}

// prove finishes a call with the outcome that it proved, nil for a committed
// success. Only a replay records it, because only a replay settles an operation
// that a caller saw as uncertain.
func (t *laneTicket) prove(outcome error) {
	t.proved, t.outcome = true, outcome
	t.finish(false)
}

// Only registry references are released while holding its lock. Active writers
// and callers use release(), so the last reference returns bytes exactly once.
func (f *operationFrame) releaseRegistryLocked(r *operationRegistry) {
	if f.refs.Add(-1) == 0 {
		f.data = nil
		r.releaseSlotLocked(f.reservation)
		r.bytes -= f.reservation.size
	}
}

func (r *operationRegistry) resume(ctx context.Context, id uint64) (*laneTicket, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		operation := r.retained[id]
		if operation == nil {
			return nil, ierror.OperationNotRetained{OperationID: id}
		}
		lane := r.lanes[operation.key]
		if ctx.Err() != nil {
			return nil, ierror.UncertainOutcome{OperationID: id}
		}
		if !lane.active {
			lane.active = true
			operation.frame.acquire()
			return &laneTicket{owner: r, key: operation.key, lane: lane, fence: r.fence, operation: operation, reservation: operation.frame.reservation, resumed: true}, nil
		}
		changed := r.changed
		r.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		r.mu.Lock()
	}
}

func (r *operationRegistry) abandon(id uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	operation := r.retained[id]
	if operation == nil {
		return ierror.OperationNotRetained{OperationID: id}
	}
	delete(r.retained, id)
	lane := r.lanes[operation.key]
	lane.retained = nil
	r.releaseSlotLocked(operation.frame.reservation)
	operation.frame.releaseRegistryLocked(r)
	r.cleanLaneLocked(operation.key, lane)
	r.notifyLocked()
	return nil
}

// recordedOutcome returns what a replay proved for id while the record keeps it.
func (r *operationRegistry) recordedOutcome(id uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, proved := range r.proved {
		if proved.id == id {
			return proved.err
		}
	}
	return ierror.OperationNotRetained{OperationID: id}
}

func (r *operationRegistry) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fence++
	for reservation := range r.reservations {
		r.releaseSlotLocked(reservation)
	}
	for _, operation := range r.retained {
		operation.frame.releaseRegistryLocked(r)
	}
	clear(r.retained)
	clear(r.lanes)
	r.notifyLocked()
}

// current reports whether no reset replaced the session of the ticket. An
// abandon does not stop a running replay, which keeps the lane and records
// what it proves.
func (t *laneTicket) current() bool {
	r := t.owner
	r.mu.Lock()
	defer r.mu.Unlock()
	return t.fence == r.fence
}
