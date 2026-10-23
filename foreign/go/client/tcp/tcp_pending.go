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
	"slices"
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
	waiters  []registryWaiter
}

// registryWaiter wakes one call that waits for the registry. It buffers one
// wakeup, so a wake never blocks and a wake that comes before the wait counts.
type registryWaiter chan struct{}

func newRegistryWaiter() registryWaiter { return make(registryWaiter, 1) }

func (w registryWaiter) wake() {
	select {
	case w <- struct{}{}:
	default:
	}
}

// budgetWaiter is a reservation that waits for a slot and its bytes. ready
// closes when the registry grants it or when a reset ends the wait.
type budgetWaiter struct {
	size    int
	granted *frameReservation
	ready   chan struct{}
}

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
	// The reservations that wait for a slot or bytes, in arrival order.
	budget []*budgetWaiter
	// The calls that wait to resolve each retained operation.
	resumers map[uint64][]registryWaiter
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
		owner.grantBudgetLocked()
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
	return &operationRegistry{fence: 1, lanes: make(map[laneKey]*mutationLane), retained: make(map[uint64]*pendingOperation), reservations: make(map[*frameReservation]struct{}), maxOperations: maxOperations, maxBytes: maxBytes, resumers: make(map[uint64][]registryWaiter)}
}

// enter waits for the lane of key in arrival order. A reset keeps the queue,
// so the ticket takes the fence of the session it is granted under.
func (r *operationRegistry) enter(ctx context.Context, key laneKey) (*laneTicket, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lane := r.lanes[key]
	if lane == nil {
		lane = &mutationLane{}
		r.lanes[key] = lane
	}
	var waiter registryWaiter
	for {
		if lane.retained != nil {
			id := lane.retained.id
			r.leaveLaneLocked(key, lane, waiter)
			return nil, ierror.LaneBlocked{OperationID: id}
		}
		if ctx.Err() != nil {
			r.leaveLaneLocked(key, lane, waiter)
			return nil, ierror.ErrTransientNotAccepted
		}
		if !lane.active && (len(lane.waiters) == 0 || lane.waiters[0] == waiter) {
			if waiter != nil {
				lane.waiters[0] = nil
				lane.waiters = lane.waiters[1:]
			}
			lane.active = true
			return &laneTicket{owner: r, key: key, lane: lane, fence: r.fence}, nil
		}
		if waiter == nil {
			waiter = newRegistryWaiter()
			lane.waiters = append(lane.waiters, waiter)
		}
		r.waitLocked(ctx, waiter)
	}
}

// waitLocked releases r.mu until waiter wakes or ctx ends.
func (r *operationRegistry) waitLocked(ctx context.Context, waiter registryWaiter) {
	r.mu.Unlock()
	select {
	case <-ctx.Done():
	case <-waiter:
	}
	r.mu.Lock()
}

// leaveLaneLocked takes waiter, if queued, out of lane. When it was the head,
// the next one may take a free lane.
func (r *operationRegistry) leaveLaneLocked(key laneKey, lane *mutationLane, waiter registryWaiter) {
	if index := slices.Index(lane.waiters, waiter); index >= 0 {
		lane.waiters = slices.Delete(lane.waiters, index, index+1)
		if index == 0 {
			r.wakeLaneLocked(lane)
		}
	}
	r.cleanLaneLocked(key, lane)
}

// wakeLaneLocked wakes the waiters that can go on: every waiter of a lane that
// a retained operation blocks, or the head of a free lane. Only a finish makes
// a lane retain an operation, and it wakes every waiter, so an abandon or a
// reset that clears the operation needs no wakeup.
func (r *operationRegistry) wakeLaneLocked(lane *mutationLane) {
	switch {
	case lane.retained != nil:
		for _, waiter := range lane.waiters {
			waiter.wake()
		}
	case !lane.active && len(lane.waiters) > 0:
		lane.waiters[0].wake()
	}
}

func (r *operationRegistry) wakeResumersLocked(id uint64) {
	for _, resumer := range r.resumers[id] {
		resumer.wake()
	}
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
	if ctx.Err() != nil {
		return ierror.ErrTransientNotAccepted
	}
	if r.fitsLocked(size) {
		t.reservation = r.reserveLocked(size)
		return nil
	}
	fence := r.fence
	waiter := &budgetWaiter{size: size, ready: make(chan struct{})}
	r.budget = append(r.budget, waiter)
	r.mu.Unlock()
	select {
	case <-ctx.Done():
	case <-waiter.ready:
	}
	r.mu.Lock()
	switch {
	case fence != r.fence:
		r.cancelBudgetLocked(waiter)
		return ierror.ErrUnauthenticated
	case ctx.Err() != nil:
		r.cancelBudgetLocked(waiter)
		return ierror.ErrTransientNotAccepted
	}
	t.reservation = waiter.granted
	return nil
}

func (r *operationRegistry) fitsLocked(size int) bool {
	return len(r.reservations) < r.maxOperations && size <= r.maxBytes-r.bytes
}

func (r *operationRegistry) reserveLocked(size int) *frameReservation {
	reservation := &frameReservation{owner: r, size: size}
	r.reservations[reservation] = struct{}{}
	r.bytes += size
	return reservation
}

// grantBudgetLocked reserves, in arrival order, for every waiter that fits. A
// waiter that does not fit does not hold back a later one that does.
func (r *operationRegistry) grantBudgetLocked() {
	waiting := r.budget[:0]
	for _, waiter := range r.budget {
		if !r.fitsLocked(waiter.size) {
			waiting = append(waiting, waiter)
			continue
		}
		waiter.granted = r.reserveLocked(waiter.size)
		close(waiter.ready)
	}
	clear(r.budget[len(waiting):])
	r.budget = waiting
}

// cancelBudgetLocked ends the wait of waiter. A grant that came too late goes
// back to the registry, as if the waiter had never received it.
func (r *operationRegistry) cancelBudgetLocked(waiter *budgetWaiter) {
	if granted := waiter.granted; granted != nil {
		r.releaseSlotLocked(granted)
		r.bytes -= granted.size
		r.grantBudgetLocked()
		return
	}
	if index := slices.Index(r.budget, waiter); index >= 0 {
		r.budget = slices.Delete(r.budget, index, index+1)
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

// releaseOperation frees the operation of the ticket and keeps its lane, so the
// call starts a new operation in its place.
func (t *laneTicket) releaseOperation() {
	r := t.owner
	r.mu.Lock()
	operation := t.operation
	t.releaseReservationLocked()
	t.operation = nil
	t.reservation = nil
	r.grantBudgetLocked()
	r.mu.Unlock()
	if operation != nil {
		operation.frame.release()
	}
}

// releaseReservationLocked frees the slot of the ticket. A frozen frame keeps
// the bytes until its last reference ends, else they are free now.
func (t *laneTicket) releaseReservationLocked() {
	r := t.owner
	r.releaseSlotLocked(t.reservation)
	if t.operation == nil && t.reservation != nil {
		r.bytes -= t.reservation.size
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
	// The lane stays with the ticket until it ends, also after a reset or an
	// abandon. Only an operation of the current session can block it, because
	// no other can replay.
	t.lane.active = false
	current := t.fence == r.fence && (!t.resumed || t.lane.retained == operation)
	if current && retain && operation != nil {
		if !t.resumed {
			operation.frame.acquire()
		}
		r.retained[operation.id] = operation
		t.lane.retained = operation
	} else {
		if current && t.resumed {
			delete(r.retained, operation.id)
			t.lane.retained = nil
			operation.frame.releaseRegistryLocked(r)
		}
		t.releaseReservationLocked()
	}
	r.cleanLaneLocked(t.key, t.lane)
	r.wakeLaneLocked(t.lane)
	// While the ticket holds the lane, only its own operation can be retained
	// there, so only the resolvers of that operation wait for the lane.
	if operation != nil {
		r.wakeResumersLocked(operation.id)
	}
	r.grantBudgetLocked()
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
	var waiter registryWaiter
	defer func() { r.leaveResumersLocked(id, waiter) }()
	for {
		operation := r.retained[id]
		if operation == nil {
			return nil, ierror.OperationNotRetained{OperationID: id}
		}
		lane := r.lanes[operation.key]
		if ctx.Err() != nil {
			return nil, ierror.OperationUncertain{OperationID: id}
		}
		if !lane.active {
			lane.active = true
			operation.frame.acquire()
			return &laneTicket{owner: r, key: operation.key, lane: lane, fence: r.fence, operation: operation, reservation: operation.frame.reservation, resumed: true}, nil
		}
		if waiter == nil {
			waiter = newRegistryWaiter()
			r.resumers[id] = append(r.resumers[id], waiter)
		}
		r.waitLocked(ctx, waiter)
	}
}

func (r *operationRegistry) leaveResumersLocked(id uint64, waiter registryWaiter) {
	resumers := r.resumers[id]
	index := slices.Index(resumers, waiter)
	if index < 0 {
		return
	}
	if len(resumers) == 1 {
		delete(r.resumers, id)
		return
	}
	r.resumers[id] = slices.Delete(resumers, index, index+1)
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
	r.wakeResumersLocked(id)
	r.grantBudgetLocked()
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
	// No retained operation can replay under the new session. A lane that a
	// call holds or waits for stays, so the calls that restart under the new
	// session keep their order.
	for key, lane := range r.lanes {
		lane.retained = nil
		r.cleanLaneLocked(key, lane)
	}
	for _, resumers := range r.resumers {
		for _, resumer := range resumers {
			resumer.wake()
		}
	}
	for _, waiter := range r.budget {
		close(waiter.ready)
	}
	clear(r.budget)
	r.budget = r.budget[:0]
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

// renew moves the ticket to the current session for the frame that the call
// stamps next. Only a reset frees the slot of an unfrozen reservation, so a
// freed slot means that the reservation belongs to a replaced session.
func (t *laneTicket) renew() bool {
	r := t.owner
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.reservation != nil && t.reservation.slotReleased {
		return false
	}
	t.fence = r.fence
	return true
}
