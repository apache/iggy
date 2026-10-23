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
	"fmt"
	"log/slog"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
)

const consumerSessionSize = 32

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

func (c *IggyTcpClient) LoginUser(ctx context.Context, username string, password string) (*iggcon.IdentityInfo, error) {
	return c.register(ctx, command.LoginRegisterCode, NewUsernamePasswordCredentials(username, password))
}

func (c *IggyTcpClient) LoginWithPersonalAccessToken(ctx context.Context, token string) (*iggcon.IdentityInfo, error) {
	return c.register(ctx, command.LoginRegisterWithPATCode, NewPersonalAccessTokenCredentials(token))
}

func (c *IggyTcpClient) acquireSignIn(ctx context.Context) error {
	if ctx == nil {
		return ierror.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case c.registerGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return ierror.ErrClientShutdown
	}
}

func (c *IggyTcpClient) register(ctx context.Context, code command.Code, credentials Credentials) (identity *iggcon.IdentityInfo, err error) {
	// Even an invalid or cancelled explicit attempt leaves the previous owner.
	c.mtx.Lock()
	select {
	case <-c.closed:
		c.mtx.Unlock()
		return nil, ierror.ErrClientShutdown
	default:
	}
	c.endRecoveryLocked()
	c.endSignInEpochLocked(nil)
	epoch := c.epoch.Load()
	c.autoLoginBlocked = true
	c.rememberedLogin = AutoLogin{}
	c.loggedOut = true
	c.invalidateSessionLocked()
	var stopped func(bool)
	if len(c.registerGate) != 0 {
		c.session.Reset()
		stopped = c.links.detach()
	}
	c.mtx.Unlock()
	if stopped != nil {
		stopped(false)
	}
	callerCtx := ctx
	defer func() {
		if err == nil {
			return
		}
		if ended := epoch.err(); ended != nil {
			err = ended
		} else if callerCtx != nil && callerCtx.Err() != nil && unanswered(err) {
			err = callerCtx.Err()
		} else if callerCtx != nil && callerCtx.Err() == nil && (errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, ierror.ErrTransientNotCommitted) && errors.Is(ctx.Err(), context.DeadlineExceeded)) {
			err = ierror.ErrTransientNotAccepted
		}
		c.mtx.Lock()
		var stopped func(bool)
		if c.epoch.Load() == epoch {
			c.session.Reset()
			c.invalidateSessionLocked()
			c.rememberedLogin = AutoLogin{}
			c.loggedOut = true
			stopped = c.links.detach()
		}
		c.mtx.Unlock()
		if stopped != nil {
			stopped(false)
		}
	}()
	if ctx == nil {
		return nil, ierror.ErrNilContext
	}
	ctx, cancel := signInContext(ctx, epoch)
	defer cancel()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	secret := vsr.NewBindSecret()
	var body []byte
	if code == command.LoginRegisterWithPATCode {
		body, err = vsr.SerializeLoginRegisterWithToken(credentials.personalAccessToken, iggcon.Version, secret)
	} else {
		body, err = vsr.SerializeLoginRegister(credentials.username, credentials.password, iggcon.Version, secret)
	}
	if err != nil {
		return nil, err
	}
	if err = c.acquireSignIn(ctx); err != nil {
		return nil, err
	}
	defer func() { <-c.registerGate }()
	if err = epoch.err(); err != nil {
		return nil, err
	}
	link := c.links.any()
	if link == nil {
		link, err = c.links.connect(ctx)
		if err != nil {
			return nil, err
		}
	}
	c.mtx.Lock()
	bound := c.session.Bound()
	c.mtx.Unlock()
	if bound {
		_, _ = c.readOnLink(ctx, link, command.LogoutUserCode, nil, nil)
		c.mtx.Lock()
		if err = epoch.err(); err == nil {
			err = ctx.Err()
		}
		var stopped func(bool)
		if err == nil {
			c.session.Reset()
			c.invalidateSessionLocked()
			stopped = c.links.detach()
		}
		c.mtx.Unlock()
		if stopped != nil {
			stopped(false)
		}
		if err != nil {
			return nil, err
		}
		link, err = c.links.connect(ctx)
		if err != nil {
			return nil, err
		}
	}
	identity, err = c.signIn(ctx, link, uint32(code), body, secret, credentials, false)
	if err != nil {
		var refusal *signInRefusal
		if errors.As(err, &refusal) {
			err = refusal.err
		}
		return nil, err
	}
	if err = c.refreshRoster(ctx, link); err != nil {
		return nil, err
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	select {
	case <-c.closed:
		return nil, ierror.ErrClientShutdown
	default:
	}
	if err = epoch.err(); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	c.autoLoginBlocked = false
	c.links.startHeartbeat()
	return identity, nil
}

// Registration waits follow their caller owner, including an initial Connect.
func signInContext(ctx context.Context, epoch *signInEpoch) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, responseReadTimeout)
	stop := context.AfterFunc(epoch.ctx, cancel)
	return context.WithValue(ctx, operationScope{}, epoch), func() { stop(); cancel() }
}

func (c *IggyTcpClient) signIn(ctx context.Context, link *tcpLink, code uint32, body []byte, secret [vsr.BindSecretBytes]byte, credentials Credentials, automatic bool) (*iggcon.IdentityInfo, error) {
	frame := append(reserveHeader(nil), body...)
	epoch := c.callEpoch(ctx)
	c.mtx.Lock()
	if err := epoch.err(); err != nil {
		c.mtx.Unlock()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		c.mtx.Unlock()
		return nil, err
	}
	c.invalidateSessionLocked()
	err := vsr.StampRequestHeader(c.session, code, frame)
	fence := c.sessionFence
	c.mtx.Unlock()
	if err != nil {
		return nil, err
	}
	request := newOperationFrame(frame, nil)
	defer request.release()
	for {
		reply, sent := link.exchange(ctx, request)
		if reply.err != nil {
			if !sent {
				return nil, ierror.ErrTransientNotAccepted
			}
			var eviction *vsr.EvictionError
			if errors.As(reply.err, &eviction) {
				return nil, &signInRefusal{err: reply.err}
			}
			return nil, reply.err
		}
		outcome, decodeErr := vsr.DecodeReplyFor((*[vsr.HeaderSize]byte)(frame), &reply.header, reply.body)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if errors.Is(outcome.Err, ierror.ErrTransientNotCommitted) || errors.Is(outcome.Err, ierror.ErrTransientNotAccepted) || errors.Is(outcome.Err, ierror.ErrNotPrimary) {
			if err := c.pause(ctx, replayInterval); err != nil {
				return nil, err
			}
			continue
		}
		if outcome.Err != nil {
			return nil, &signInRefusal{err: outcome.Err}
		}
		registered, err := vsr.DecodeLoginRegister(outcome.Body)
		if err != nil {
			return nil, err
		}
		c.mtx.Lock()
		if c.sessionFence != fence || epoch.over() {
			c.mtx.Unlock()
			return nil, ierror.ErrUnauthenticated
		}
		if err = ctx.Err(); err != nil {
			c.mtx.Unlock()
			return nil, err
		}
		if automatic {
			// A committed password change may have updated this same owner
			// while its Register was in flight.
			credentials, _ = c.signInCredentialsLocked()
		}
		err = c.session.Bind(registered.Session)
		if err == nil {
			c.session.SetBindSecret(secret)
			c.sessionUserID = registered.UserID
			c.rememberedLogin = NewAutoLogin(credentials)
			c.loggedOut = false
			c.observeMetadata(&reply.header)
		}
		c.mtx.Unlock()
		if err != nil {
			return nil, err
		}
		link.markBound(fence)
		return &iggcon.IdentityInfo{UserId: registered.UserID}, nil
	}
}

func (c *IggyTcpClient) establishSession(ctx context.Context, link *tcpLink) error {
	if err := c.acquireSignIn(ctx); err != nil {
		return err
	}
	defer func() { <-c.registerGate }()
	epoch := c.callEpoch(ctx)
	c.mtx.Lock()
	if err := epoch.err(); err != nil {
		c.mtx.Unlock()
		return err
	}
	bound := c.session.Bound()
	fence := c.sessionFence
	blocked := c.loggedOut || c.autoLoginBlocked
	c.mtx.Unlock()
	if blocked {
		return nil
	}
	if bound {
		if !link.isBound(fence) {
			if err := c.bindLink(ctx, link); err != nil {
				return err
			}
		}
	} else {
		for {
			credentials, enabled := c.signInCredentials()
			if !enabled {
				return nil
			}
			secret := vsr.NewBindSecret()
			var body []byte
			var err error
			code := command.LoginRegisterCode
			if credentials.personalAccessToken != "" {
				code = command.LoginRegisterWithPATCode
				body, err = vsr.SerializeLoginRegisterWithToken(credentials.personalAccessToken, iggcon.Version, secret)
			} else {
				body, err = vsr.SerializeLoginRegister(credentials.username, credentials.password, iggcon.Version, secret)
			}
			if err != nil {
				return err
			}
			if _, err = c.signIn(ctx, link, uint32(code), body, secret, credentials, true); err == nil {
				break
			}
			var refusal *signInRefusal
			if errors.As(err, &refusal) {
				err = refusal.err
				retry, refused := c.endRefusedSignIn(epoch, credentials, err)
				if retry {
					link.close(err)
					link, err = c.links.connect(ctx)
					if err != nil {
						return err
					}
					continue
				}
				if refused {
					// Connect returns its own refusal, also when it ended the epoch.
					link.close(err)
					return refusal
				}
			}
			link.close(err)
			return err
		}
	}
	if err := c.refreshRoster(ctx, link); err != nil {
		return err
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if err := epoch.err(); err != nil {
		return err
	}
	c.links.startHeartbeat()
	return nil
}

func (c *IggyTcpClient) bindLink(ctx context.Context, link *tcpLink) error {
	select {
	case link.bindGate <- struct{}{}:
	case <-ctx.Done():
		return ierror.ErrTransientNotAccepted
	case <-link.closed:
		return ierror.ErrTransientNotAccepted
	}
	defer func() { <-link.bindGate }()
	c.mtx.Lock()
	if !c.session.Bound() {
		c.mtx.Unlock()
		return ierror.ErrUnauthenticated
	}
	identity := consumerSession{client: c.session.ClientID(), session: c.session.SessionID(), watermark: c.metadataWatermark.Load()}
	secret := c.session.BindSecret()
	fence := c.sessionFence
	c.mtx.Unlock()
	if link.isBound(fence) {
		return nil
	}
	body, err := vsr.SerializeBindSession(identity.bytes(), iggcon.Version, secret)
	if err != nil {
		return err
	}
	for {
		response, err := c.readOnLink(ctx, link, command.BindSessionCode, body, nil)
		if errors.Is(err, ierror.ErrTransientNotAccepted) || errors.Is(err, ierror.ErrNotPrimary) {
			if err = c.pause(ctx, reconnectDelay(replayInterval, 0)); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		bound, err := vsr.DecodeLoginRegister(response)
		if err != nil {
			return err
		}
		c.mtx.Lock()
		current := c.sessionFence == fence
		valid := c.session.ClientID() == identity.client && c.session.SessionID() == identity.session && bound.Session == identity.session
		c.mtx.Unlock()
		// A session change during the bind ends the call like one during any
		// other exchange, so a recovery can restart it.
		if !current {
			return ierror.ErrUnauthenticated
		}
		if !valid {
			return ierror.ErrSessionMismatch
		}
		link.markBound(fence)
		return nil
	}
}

// resetSession ends the session for a logout or disconnect of the caller.
func (c *IggyTcpClient) resetSession(forget bool) *signInEpoch {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.resetSessionLocked(forget)
	return c.epoch.Load()
}

func (c *IggyTcpClient) resetSessionLocked(forget bool) {
	c.links.stopHeartbeat()
	c.endRecoveryLocked()
	c.endSignInEpochLocked(nil)
	c.session.Reset()
	c.invalidateSessionLocked()
	if forget {
		c.rememberedLogin = AutoLogin{}
		c.loggedOut = true
	}
}

func (c *IggyTcpClient) invalidateSessionLocked() {
	c.sessionFence++
	c.operations.reset()
	c.polls.reset()
	c.groups.clear()
	c.topics.clearCounts()
}

// signInEpoch lasts from one sign-in, logout or disconnect of the caller, or
// one refusal of the credentials of an automatic sign-in, to the next. A call keeps its
// starting epoch, so only the SDK's own recovery continues it, and no call goes out
// under a later sign-in. The client replaces its epoch only under mtx, and a
// call reads it without the lock as it starts.
type signInEpoch struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	ended  <-chan struct{}
}

func newSignInEpoch() *signInEpoch {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &signInEpoch{ctx: ctx, cancel: cancel, ended: ctx.Done()}
}

func (e *signInEpoch) over() bool { return e.err() != nil }

func (e *signInEpoch) err() error { return context.Cause(e.ctx) }

// endSignInEpochLocked also cancels registration waits of the old owner.
func (c *IggyTcpClient) endSignInEpochLocked(cause error) {
	epoch := c.epoch.Swap(newSignInEpoch())
	var failure error = ierror.ErrUnauthenticated
	if cause != nil {
		failure = fmt.Errorf("%w: %w", ierror.ErrUnauthenticated, cause)
	}
	epoch.cancel(failure)
}

// endRecoveryLocked stops the automatic sign-in that follows a session loss.
func (c *IggyTcpClient) endRecoveryLocked() {
	if c.recoveryCancel != nil {
		c.recoveryCancel()
		c.recovery, c.recoveryCancel = nil, nil
	}
}

func (c *IggyTcpClient) recoverSession(expected uint64) {
	c.mtx.Lock()
	if c.sessionFence != expected || !c.session.Bound() {
		c.mtx.Unlock()
		return
	}
	if c.recoveryCancel != nil {
		c.recoveryCancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.recovery, c.recoveryCancel = ctx, cancel
	c.session.Reset()
	c.invalidateSessionLocked()
	c.mtx.Unlock()
	c.links.disconnect(false)
	go func() {
		defer cancel()
		c.connectMu.Lock()
		attempt := c.connectionAttempt
		c.connectMu.Unlock()
		if attempt != nil {
			select {
			case <-attempt.done:
			case <-ctx.Done():
				return
			}
		}
		// The session loss stopped every link and the heartbeat, so only
		// this task brings the client back. A sign-in that the server never
		// answered, or refused for a reason other than its credentials, goes
		// again, until a session, a refusal of the credentials, a sign-in by
		// the caller or a disconnect ends the recovery.
		for retry := uint32(0); ; retry++ {
			signInCtx, endSignIn := context.WithTimeout(ctx, responseReadTimeout)
			err := c.connect(signInCtx)
			endSignIn()
			var refusal *signInRefusal
			owed := unanswered(err) || errors.As(err, &refusal) && !credentialRefused(refusal.err)
			if err == nil || !c.config.reconnection.enabled || !owed ||
				c.pause(ctx, reconnectDelay(c.config.reconnection.interval, retry)) != nil {
				return
			}
		}
	}()
}

// signInRefusal wraps the refusal of a sign-in by the server, an eviction or a
// status. It tells the refusal apart from the local errors of signIn, such as
// the 40 of a sign-in that a newer one overtook.
type signInRefusal struct{ err error }

func (r *signInRefusal) Error() string { return r.err.Error() }
func (r *signInRefusal) Unwrap() error { return r.err }

// endRefusedSignIn retries only credentials updated by this same owner.
// A refusal of its current credentials ends the epoch and blocks fallback until
// an explicit login. Any other refusal keeps the epoch, so the session stays
// owed. refused reports whether the refusal answers the current epoch. It is
// false when a newer owner ended the epoch first.
func (c *IggyTcpClient) endRefusedSignIn(epoch *signInEpoch, attempted Credentials, refusal error) (retry, refused bool) {
	c.logger.Error("The server refused the automatic sign-in", slog.Any("error", refusal))
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.epoch.Load() != epoch {
		return false, false
	}
	if !credentialRefused(refusal) {
		return false, true
	}
	current, enabled := c.signInCredentialsLocked()
	if enabled && current != attempted {
		return true, false
	}
	c.rememberedLogin = AutoLogin{}
	c.autoLoginBlocked = true
	c.loggedOut = true
	c.endSignInEpochLocked(refusal)
	return false, true
}

func credentialRefused(refusal error) bool {
	return errors.Is(refusal, ierror.ErrInvalidCredentials) || errors.Is(refusal, ierror.ErrInvalidUsername) ||
		errors.Is(refusal, ierror.ErrInvalidPassword) || errors.Is(refusal, ierror.ErrUnauthenticated) ||
		errors.Is(refusal, ierror.ErrUserInactive) || errors.Is(refusal, ierror.ErrInvalidPersonalAccessToken) ||
		errors.Is(refusal, ierror.ErrPersonalAccessTokenExpired) || errors.Is(refusal, ierror.ErrInvalidFormat)
}

// unanswered reports whether a sign-in failed before the server answered it,
// so trying it again can succeed.
func unanswered(err error) bool {
	return retryBeforeAdmission(err) || errors.Is(err, ierror.ErrCannotEstablishConnection) || errors.Is(err, context.DeadlineExceeded)
}

func (c *IggyTcpClient) LogoutUser(ctx context.Context) error {
	if ctx == nil {
		return ierror.ErrNilContext
	}
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	if _, err := c.do(ctx, &command.LogoutUser{}); err != nil {
		return err
	}
	c.finishLogout(c.callEpoch(ctx))
	return nil
}

func (c *IggyTcpClient) finishLogout(epoch *signInEpoch) {
	c.mtx.Lock()
	if c.epoch.Load() != epoch {
		c.mtx.Unlock()
		return
	}
	c.resetSessionLocked(true)
	stopped := c.links.detach()
	c.mtx.Unlock()
	stopped(true)
}

// signInCredentials are the last sign-in, else the configured AutoLogin. A
// client recovers as whoever it last signed in as, so its calls never
// continue as another user.
func (c *IggyTcpClient) signInCredentials() (Credentials, bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.signInCredentialsLocked()
}

func (c *IggyTcpClient) signInCredentialsLocked() (Credentials, bool) {
	if c.autoLoginBlocked {
		return Credentials{}, false
	}
	if c.rememberedLogin.enabled {
		return c.rememberedLogin.credentials, true
	}
	return c.config.autoLogin.credentials, c.config.autoLogin.enabled
}
func (c *IggyTcpClient) forgetLogin() { c.mtx.Lock(); c.rememberedLogin = AutoLogin{}; c.mtx.Unlock() }
