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
	"fmt"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
)

type completedResultError struct{ err error }

func (e *completedResultError) Error() string { return e.err.Error() }
func (e *completedResultError) Unwrap() error { return e.err }

func (c *IggyTcpClient) drive(ctx context.Context, code command.Code, query, frame []byte, exact *iggcon.PartitionContext) ([]byte, error) {
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	epoch := c.callEpoch(ctx)
	_, mutation := vsr.ReplicatedOperation(uint32(code))
	progress := isProgressPoll(code, frame[vsr.HeaderSize:])
	// A plain read reports a context that is done before its first attempt.
	// Later, also when a session recovery restarts the call, it ends with 58.
	if err := ctx.Err(); err != nil && !mutation && !progress {
		return nil, err
	}
	historyRefreshed := false
	keptContext := exact
	lifecycleDelay := replayInterval
	lookupDelay := replayInterval
	var ticket *laneTicket
	defer func() {
		if ticket != nil {
			ticket.finish(false)
		}
	}()
	for {
		if ctx.Err() != nil {
			return nil, ierror.ErrTransientNotAccepted
		}
		select {
		case <-c.closed:
			return nil, ierror.ErrClientShutdown
		default:
		}
		if err := c.waitForRecovery(ctx); err != nil {
			return nil, err
		}
		var route pollRoute
		var captured iggcon.PartitionContext
		if query != nil {
			var err error
			route, err = c.consumerRoute(ctx, query)
			if err != nil {
				if retryBeforeAdmission(err) {
					if err = c.pause(ctx, nextLookupBackoff(&lookupDelay)); err == nil {
						continue
					}
					return nil, ierror.ErrTransientNotAccepted
				}
				if c.sessionRecovers(ctx, err) {
					continue
				}
				return nil, err
			}
			captured = route.context
		} else {
			captured.MetadataOp = c.metadataWatermark.Load()
		}
		if keptContext != nil {
			captured = *keptContext
		} else if query != nil {
			saved := captured
			keptContext = &saved
		}
		if ticket == nil && (mutation || progress) {
			key := laneKey{}
			if query != nil {
				key = laneKey{incarnation: captured.Incarnation, partition: route.key.partition}
			}
			var err error
			ticket, err = c.operations.enter(ctx, key)
			if err != nil {
				return nil, err
			}
		}
		if mutation {
			if err := ticket.reserve(ctx, len(frame)); err != nil {
				if c.sessionRecovers(ctx, err) {
					continue
				}
				return nil, err
			}
		}
		frozen := append([]byte(nil), frame...)
		c.mtx.Lock()
		fence := c.sessionFence
		err := epoch.err()
		if err == nil {
			if ticket != nil && !ticket.renew() {
				err = ierror.ErrUnauthenticated
			} else {
				err = vsr.StampRequestHeader(c.session, uint32(code), frozen)
			}
		}
		if err == nil {
			vsr.StampPartitionContext((*[vsr.HeaderSize]byte)(frozen), captured)
		}
		c.mtx.Unlock()
		if err != nil {
			if c.sessionRecovers(ctx, err) {
				if ticket != nil {
					ticket.releaseOperation()
				}
				continue
			}
			if ticket != nil {
				ticket.finish(false)
			}
			return nil, err
		}
		var operation *pendingOperation
		if ticket != nil {
			operation = ticket.freeze(code, query, frozen)
		} else {
			operation = &pendingOperation{id: vsr.StampedRequestID(frozen), code: code, query: append([]byte(nil), query...), fence: fence, frame: newOperationFrame(frozen, nil)}
		}
		response, err := c.driveOperation(ctx, operation, ticket, false, mutation)
		var completed *completedResultError
		if errors.As(err, &completed) {
			return nil, completed.err
		}
		if c.sessionRecovers(ctx, err) {
			continue
		}
		if errors.Is(err, ierror.ErrLifecycleBusy) {
			if waitErr := c.pause(ctx, lifecycleDelay); waitErr != nil {
				return nil, ierror.ErrLifecycleBusy
			}
			lifecycleDelay = min(lifecycleDelay*2, lifecycleRetryMaxInterval)
			continue
		}
		if errors.Is(err, ierror.ErrHistoryUnavailable) && code == command.SendMessagesCode && exact == nil && !historyRefreshed {
			ticket = nil
			historyRefreshed = true
			keptContext = nil
			continue
		}
		return response, err
	}
}

func (c *IggyTcpClient) driveRetained(ctx context.Context, ticket *laneTicket) ([]byte, error) {
	response, err := c.driveOperation(ctx, ticket.operation, ticket, true, true)
	var completed *completedResultError
	if errors.As(err, &completed) {
		err = completed.err
	}
	return response, err
}

func (c *IggyTcpClient) driveOperation(ctx context.Context, operation *pendingOperation, ticket *laneTicket, uncertain, mutation bool) (response []byte, err error) {
	retain := false
	rosterRefreshed := false
	notPrimarySeen := false
	lookupDelay := replayInterval
	progress := isProgressPoll(operation.code, operation.frame.data[vsr.HeaderSize:])
	defer func() {
		if ticket == nil {
			operation.frame.release()
			return
		}
		// A replay returns nil or a completed result only with a committed proof.
		var completed *completedResultError
		switch {
		// The call keeps its lane to start a new operation. Unauthenticated
		// ends only a call that cannot have changed server state, and it can
		// restart under the session that replaces the lost one.
		case !uncertain && (errors.Is(err, ierror.ErrLifecycleBusy) || errors.Is(err, ierror.ErrUnauthenticated)):
			ticket.releaseOperation()
		case err == nil:
			ticket.prove(nil)
		case errors.As(err, &completed):
			ticket.prove(completed.err)
			// Only after the proof: the reset releases the operation, so a
			// concurrent resolver that misses it must find its outcome recorded.
			if isSessionError(completed.err) {
				c.recoverSession(operation.fence)
			}
		default:
			ticket.finish(retain)
		}
	}()
	finishUncertain := func() error { retain = mutation; return ierror.OperationUncertain{OperationID: operation.id} }
	sessionChanged := func(sent bool) error {
		if progress && sent {
			return ierror.ErrTransientNotCommitted
		}
		if mutation && (sent || uncertain) {
			return finishUncertain()
		}
		return ierror.ErrUnauthenticated
	}
	for {
		c.mtx.Lock()
		current := c.sessionFence == operation.fence
		c.mtx.Unlock()
		if ticket != nil {
			current = current && ticket.current()
		}
		if !current {
			return nil, sessionChanged(false)
		}
		if stopped := c.stopped(); stopped != nil {
			if uncertain && mutation {
				return nil, finishUncertain()
			}
			if uncertain {
				return nil, ierror.ErrTransientNotCommitted
			}
			return nil, stopped
		}
		if ctx.Err() != nil {
			if uncertain && mutation {
				return nil, finishUncertain()
			}
			if uncertain {
				return nil, ierror.ErrTransientNotCommitted
			}
			return nil, ierror.ErrTransientNotAccepted
		}
		var link *tcpLink
		var route pollRoute
		if operation.query != nil {
			route, err = c.consumerRoute(ctx, operation.query)
			if err == nil {
				link = c.links.replica(route.primary)
				if link == nil && !rosterRefreshed && !c.links.contains(route.primary) {
					rosterRefreshed = true
					_ = c.refreshRoster(ctx, c.links.any())
					link = c.links.replica(route.primary)
				}
			}
		} else {
			link = c.links.metadata()
			if link == nil && operation.code != command.GetClusterMetadataCode {
				_ = c.refreshRoster(ctx, c.links.any())
				link = c.links.metadata()
			}
			if link == nil && operation.code == command.GetClusterMetadataCode {
				link = c.links.any()
			}
		}
		if err != nil || link == nil {
			if err != nil && !retryBeforeAdmission(err) {
				if uncertain && mutation {
					return nil, finishUncertain()
				}
				return nil, err
			}
			if waitErr := c.pause(ctx, nextLookupBackoff(&lookupDelay)); waitErr != nil {
				if uncertain && mutation {
					return nil, finishUncertain()
				}
				if uncertain {
					return nil, ierror.ErrTransientNotCommitted
				}
				return nil, ierror.ErrTransientNotAccepted
			}
			err = nil
			continue
		}
		c.mtx.Lock()
		bound := c.session.Bound()
		c.mtx.Unlock()
		if bound {
			if err = c.bindLink(ctx, link); err != nil {
				if isSessionError(err) {
					c.recoverSession(operation.fence)
					if uncertain && mutation {
						return nil, finishUncertain()
					}
					return nil, err
				}
				if !retryBeforeAdmission(err) {
					if uncertain && mutation {
						return nil, finishUncertain()
					}
					return nil, err
				}
				if waitErr := c.pause(ctx, nextLookupBackoff(&lookupDelay)); waitErr != nil {
					if uncertain && mutation {
						return nil, finishUncertain()
					}
					if uncertain {
						return nil, ierror.ErrTransientNotCommitted
					}
					return nil, ierror.ErrTransientNotAccepted
				}
				continue
			}
		}
		reply, sent := link.exchange(ctx, operation.frame)
		c.mtx.Lock()
		current = c.sessionFence == operation.fence
		c.mtx.Unlock()
		if ticket != nil {
			current = current && ticket.current()
		}
		if !current {
			return nil, sessionChanged(sent)
		}
		if reply.err != nil {
			if sent {
				uncertain = true
			}
			if bound && isDetachedError(reply.err) {
				reply.err = ierror.ErrTransientNotCommitted
			} else if isSessionError(reply.err) {
				c.recoverSession(operation.fence)
			}
			if progress && sent {
				return nil, ierror.ErrTransientNotCommitted
			}
			if !retryBeforeAdmission(reply.err) {
				if uncertain && mutation {
					return nil, finishUncertain()
				}
				return nil, reply.err
			}
			if operation.query != nil {
				c.polls.invalidate(route, false)
			} else if sent && linkReady(link) {
				c.links.invalidateMetadataPrimary(link)
			}
			if ctx.Err() != nil {
				if uncertain && mutation {
					return nil, finishUncertain()
				}
				if uncertain {
					return nil, ierror.ErrTransientNotCommitted
				}
				return nil, ierror.ErrTransientNotAccepted
			}
			if err = c.pause(ctx, nextLookupBackoff(&lookupDelay)); err != nil {
				if uncertain && mutation {
					return nil, finishUncertain()
				}
				if uncertain {
					return nil, ierror.ErrTransientNotCommitted
				}
				return nil, ierror.ErrTransientNotAccepted
			}
			continue
		}
		outcome, decodeErr := vsr.DecodeReplyFor((*[vsr.HeaderSize]byte)(operation.frame.data), &reply.header, reply.body)
		if decodeErr != nil {
			link.close(decodeErr)
			if mutation {
				uncertain = true
				continue
			}
			if progress {
				return nil, ierror.ErrTransientNotCommitted
			}
			return nil, decodeErr
		}
		if !vsr.ReplyOperationMatches(vsr.Operation(operation.frame.data[frameOffsetOperationValue]), vsr.ReadReplyOperation(&reply.header)) {
			link.close(ierror.ErrInvalidCommand)
			if mutation {
				uncertain = true
				continue
			}
			if progress {
				return nil, ierror.ErrTransientNotCommitted
			}
			return nil, ierror.ErrInvalidCommand
		}
		publishCredentials := outcome.Err == nil && (!uncertain || !mutation || outcome.Committed)
		if !c.acceptOperationReply(c.callEpoch(ctx), operation, &reply.header, publishCredentials) {
			return nil, sessionChanged(sent)
		}
		if outcome.Err == nil {
			if uncertain && mutation && !outcome.Committed {
				if err = c.pause(ctx, nextLookupBackoff(&lookupDelay)); err != nil {
					return nil, finishUncertain()
				}
				continue
			}
			c.refreshMembership(operation.code, operation.frame.data[vsr.HeaderSize:])
			return outcome.Body, nil
		}
		if uncertain && outcome.Committed {
			return nil, &completedResultError{err: outcome.Err}
		}
		err = outcome.Err
		switch {
		case errors.Is(err, ierror.ErrTransientNotCommitted):
			if progress {
				return nil, err
			}
			uncertain = true
		case errors.Is(err, ierror.ErrTransientNotAccepted):
			if operation.query != nil {
				c.polls.invalidate(route, false)
			} else if operation.code != command.GetClusterMetadataCode {
				_ = c.refreshRoster(ctx, link)
			}
		case errors.Is(err, ierror.ErrNotPrimary):
			firstRefusal := !notPrimarySeen
			notPrimarySeen = true
			if operation.query != nil {
				c.polls.invalidate(route, true)
			} else {
				_ = c.refreshRoster(ctx, link)
			}
			if firstRefusal {
				err = nil
				continue
			}
		case errors.Is(err, ierror.ErrRequestTooOld):
			if mutation {
				return nil, finishUncertain()
			}
			return nil, err
		case operation.query != nil && isNamespaceMiss(err) && !c.isMetadataPrimary(ctx, link):
			c.polls.relookup(route)
		default:
			if closeIfDetached(link, outcome, bound) {
				if errors.Is(err, ierror.ErrUnauthenticated) {
					err = nil
					continue
				}
			} else if isSessionError(err) {
				c.recoverSession(operation.fence)
			}
			if operation.query != nil && (errors.Is(err, ierror.ErrHistoryUnavailable) || errors.Is(err, ierror.ErrConsumerGroupPartitionNotOwned) || isNamespaceMiss(err)) {
				c.polls.relookup(route)
			}
			if uncertain && mutation {
				return nil, finishUncertain()
			}
			return nil, err
		}
		if waitErr := c.pause(ctx, nextLookupBackoff(&lookupDelay)); waitErr != nil {
			if uncertain && mutation {
				return nil, finishUncertain()
			}
			if uncertain {
				return nil, ierror.ErrTransientNotCommitted
			}
			return nil, ierror.ErrTransientNotAccepted
		}
		err = nil
	}
}

const frameOffsetOperationValue = 176

func nextLookupBackoff(delay *time.Duration) time.Duration {
	interval := reconnectDelay(*delay, 0)
	*delay = min(*delay*2, lifecycleRetryMaxInterval)
	return interval
}

func retryBeforeAdmission(err error) bool {
	return errors.Is(err, ierror.ErrTransientNotAccepted) || errors.Is(err, ierror.ErrNotPrimary) || errors.Is(err, ierror.ErrNotConnected) || errors.Is(err, ierror.ErrDisconnected) || errors.Is(err, ierror.ErrTransientNotCommitted)
}

func isProgressPoll(code command.Code, body []byte) bool {
	if code != command.PollMessagesCode || len(body) < pollParametersSize {
		return false
	}
	parameters := body[len(body)-pollParametersSize:]
	return parameters[0] == byte(iggcon.POLLING_NEXT) || parameters[len(parameters)-1] != 0
}

func (c *IggyTcpClient) readOnLink(ctx context.Context, link *tcpLink, code command.Code, body []byte, captured *iggcon.PartitionContext) ([]byte, error) {
	if link == nil {
		return nil, ierror.ErrTransientNotAccepted
	}
	frame := append(reserveHeader(nil), body...)
	c.mtx.Lock()
	fence := c.sessionFence
	bound := c.session.Bound() && code != command.BindSessionCode
	err := vsr.StampRequestHeader(c.session, uint32(code), frame)
	if captured == nil {
		captured = &iggcon.PartitionContext{MetadataOp: c.metadataWatermark.Load()}
	}
	if err == nil {
		vsr.StampPartitionContext((*[vsr.HeaderSize]byte)(frame), *captured)
	}
	c.mtx.Unlock()
	if err != nil {
		return nil, err
	}
	owned := newOperationFrame(frame, nil)
	defer owned.release()
	reply, _ := link.exchange(ctx, owned)
	c.mtx.Lock()
	current := c.sessionFence == fence
	c.mtx.Unlock()
	if !current {
		return nil, ierror.ErrUnauthenticated
	}
	if reply.err != nil {
		if bound && isDetachedError(reply.err) {
			return nil, ierror.ErrTransientNotCommitted
		}
		return nil, reply.err
	}
	outcome, err := vsr.DecodeReplyFor((*[vsr.HeaderSize]byte)(frame), &reply.header, reply.body)
	if err != nil {
		return nil, err
	}
	if !c.observeMetadataFor(fence, &reply.header) {
		return nil, ierror.ErrUnauthenticated
	}
	if code != command.BindSessionCode && closeIfDetached(link, outcome, bound) && errors.Is(outcome.Err, ierror.ErrUnauthenticated) {
		return nil, ierror.ErrTransientNotAccepted
	}
	return outcome.Body, outcome.Err
}

// A detached connection can still belong to a live session. Only its next
// bind can decide whether to discard the session's retained operations.
func closeIfDetached(link *tcpLink, outcome vsr.ReplyOutcome, bound bool) bool {
	if outcome.Committed || !errors.Is(outcome.Err, ierror.ErrStaleClient) && (!bound || !errors.Is(outcome.Err, ierror.ErrUnauthenticated)) {
		return false
	}
	link.close(ierror.ErrTransientNotCommitted)
	return true
}

func isDetachedError(err error) bool {
	var eviction *vsr.EvictionError
	return errors.As(err, &eviction) && (eviction.Reason == vsr.EvictionNoSession || eviction.Reason == vsr.EvictionStaleClient)
}

// Acceptance and credential publication share a lock: a concurrent refusal
// must compare the credentials from every successful reply already accepted.
func (c *IggyTcpClient) acceptOperationReply(epoch *signInEpoch, operation *pendingOperation, header *[vsr.HeaderSize]byte, publishCredentials bool) bool {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.sessionFence != operation.fence {
		return false
	}
	c.observeMetadata(header)
	if publishCredentials {
		c.refreshSignInLocked(epoch, operation.code, operation.frame.data[vsr.HeaderSize:])
	}
	return true
}

func (c *IggyTcpClient) observeMetadataFor(fence uint64, header *[vsr.HeaderSize]byte) bool {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.sessionFence != fence {
		return false
	}
	c.observeMetadata(header)
	return true
}

func (c *IggyTcpClient) observeMetadata(header *[vsr.HeaderSize]byte) {
	commit := vsr.MetadataCommit(header)
	for {
		previous := c.metadataWatermark.Load()
		if commit <= previous || c.metadataWatermark.CompareAndSwap(previous, commit) {
			return
		}
	}
}

func (c *IggyTcpClient) waitBeforeReplay(ctx context.Context, deadline time.Time) error {
	return c.pause(ctx, min(replayInterval, time.Until(deadline)))
}

func (c *IggyTcpClient) stopped() error {
	select {
	case <-c.closed:
		return ierror.ErrClientShutdown
	default:
	}
	c.links.mu.Lock()
	stopped := c.links.ctx == nil || c.links.anyLocked() == nil && (!c.config.reconnection.enabled || len(c.links.slots) == 0 && c.links.connectAttempt == nil)
	c.links.mu.Unlock()
	if stopped {
		c.mtx.Lock()
		loggedOut := c.loggedOut
		c.mtx.Unlock()
		if loggedOut {
			return ierror.ErrUnauthenticated
		}
		return ierror.ErrNotConnected
	}
	return nil
}

// sessionRecovers reports whether err ends a call without a current session
// while an automatic recovery runs or after it bound a new session. Such a
// call cannot have changed server state, so it restarts under the new
// session. After a failed recovery, and once the caller signs in, logs out or
// disconnects or the server refuses an automatic sign-in after the call
// started, the call ends with err.
func (c *IggyTcpClient) sessionRecovers(ctx context.Context, err error) bool {
	if !errors.Is(err, ierror.ErrUnauthenticated) || c.callEpoch(ctx).over() {
		return false
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.recovery != nil && !c.loggedOut && (c.recovery.Err() == nil || c.session.Bound())
}

// waitForRecovery waits while an automatic recovery replaces a lost session,
// or a Connect signs in, so a call that starts or restarts meanwhile runs
// under the new session. A sign-in that binds no session by the deadline ends
// the call with 58, and so does a Connect that the server refuses for a reason
// other than its credentials. A sign-in, logout or disconnect of the caller,
// and a refusal of the credentials of an automatic sign-in, end the calls that
// started before them with 40.
func (c *IggyTcpClient) waitForRecovery(ctx context.Context) error {
	epoch := c.callEpoch(ctx)
	for {
		c.mtx.Lock()
		recovery := c.recovery
		unbound := !c.loggedOut && !c.session.Bound()
		_, signsIn := c.signInCredentialsLocked()
		c.mtx.Unlock()
		if err := epoch.err(); err != nil {
			return err
		}
		var signInEnded <-chan struct{}
		var attempt *connectAttempt
		if unbound && recovery != nil && recovery.Err() == nil {
			signInEnded = recovery.Done()
		} else if unbound && signsIn {
			c.connectMu.Lock()
			if attempt = c.connectionAttempt; attempt != nil {
				signInEnded = attempt.done
			}
			c.connectMu.Unlock()
		}
		if signInEnded == nil {
			return nil
		}
		select {
		case <-signInEnded:
			var refusal *signInRefusal
			if attempt != nil && errors.As(attempt.err, &refusal) && !epoch.over() {
				return fmt.Errorf("%w: %w", ierror.ErrTransientNotAccepted, refusal.err)
			}
		case <-epoch.ended:
		case <-ctx.Done():
			return ierror.ErrTransientNotAccepted
		case <-c.closed:
			return ierror.ErrClientShutdown
		}
	}
}
