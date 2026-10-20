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
	secret := vsr.NewBindSecret()
	body, err := vsr.SerializeLoginRegister(username, password, iggcon.Version, secret)
	if err != nil {
		return nil, err
	}
	return c.register(
		ctx,
		uint32(command.LoginRegisterCode),
		body,
		secret,
		NewUsernamePasswordCredentials(username, password),
	)
}

func (c *IggyTcpClient) LoginWithPersonalAccessToken(ctx context.Context, token string) (*iggcon.IdentityInfo, error) {
	secret := vsr.NewBindSecret()
	body, err := vsr.SerializeLoginRegisterWithToken(token, iggcon.Version, secret)
	if err != nil {
		return nil, err
	}
	return c.register(
		ctx,
		uint32(command.LoginRegisterWithPATCode),
		body,
		secret,
		NewPersonalAccessTokenCredentials(token),
	)
}

func (c *IggyTcpClient) acquireSignIn(ctx context.Context) error {
	if ctx == nil {
		return ierror.ErrNilContext
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

func (c *IggyTcpClient) register(ctx context.Context, code uint32, body []byte, secret [vsr.BindSecretBytes]byte, credentials Credentials) (*iggcon.IdentityInfo, error) {
	if ctx == nil {
		return nil, ierror.ErrNilContext
	}
	ctx, cancel := operationContext(ctx)
	defer cancel()
	select {
	case c.registerGate <- struct{}{}:
	default:
		// The gate can belong to an automatic sign-in that waits for its reply
		// for its whole budget, so the caller ends the recovery instead of
		// waiting behind it. A late reply binds nothing, because this Register
		// gets a new client id and fence. A free gate keeps the recovery, whose
		// dial this sign-in can share.
		c.mtx.Lock()
		c.endRecoveryLocked()
		c.mtx.Unlock()
		if err := c.acquireSignIn(ctx); err != nil {
			return nil, err
		}
	}
	defer func() { <-c.registerGate }()
	// The caller leaves the last identity as this sign-in starts, also when
	// it fails before its Register. A recovery that runs meanwhile then finds
	// the client logged out and ends, so it never signs in as that identity.
	c.mtx.Lock()
	c.rememberedLogin = AutoLogin{}
	c.loggedOut = true
	c.mtx.Unlock()
	link := c.links.any()
	if link == nil {
		var err error
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
		c.links.disconnect(true)
		c.resetSession(true)
		var err error
		link, err = c.links.connect(ctx)
		if err != nil {
			return nil, err
		}
	}
	identity, err := c.signIn(ctx, link, code, body, secret)
	if err != nil {
		link.close(err)
		return nil, err
	}
	c.rememberLogin(credentials)
	c.mtx.Lock()
	c.loggedOut = false
	c.mtx.Unlock()
	if err = c.refreshRoster(ctx, link); err != nil {
		return nil, err
	}
	c.links.startHeartbeat()
	return identity, nil
}

func (c *IggyTcpClient) signIn(ctx context.Context, link *tcpLink, code uint32, body []byte, secret [vsr.BindSecretBytes]byte) (*iggcon.IdentityInfo, error) {
	frame := append(reserveHeader(nil), body...)
	c.mtx.Lock()
	c.operations.reset()
	c.sessionFence++
	c.polls.reset()
	c.groups.clear()
	c.topics.clearCounts()
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
			return nil, outcome.Err
		}
		registered, err := vsr.DecodeLoginRegister(outcome.Body)
		if err != nil {
			return nil, err
		}
		c.mtx.Lock()
		if c.sessionFence != fence {
			c.mtx.Unlock()
			return nil, ierror.ErrUnauthenticated
		}
		err = c.session.Bind(registered.Session)
		if err == nil {
			c.session.SetBindSecret(secret)
			c.sessionUserID = registered.UserID
			c.loggedOut = false
			c.observeMetadata(&reply.header)
		}
		c.mtx.Unlock()
		if err != nil {
			return nil, err
		}
		link.markBound(fence, c.metadataWatermark.Load())
		return &iggcon.IdentityInfo{UserId: registered.UserID}, nil
	}
}

func (c *IggyTcpClient) establishSession(ctx context.Context, link *tcpLink) error {
	if err := c.acquireSignIn(ctx); err != nil {
		return err
	}
	defer func() { <-c.registerGate }()
	c.mtx.Lock()
	bound := c.session.Bound()
	fence := c.sessionFence
	loggedOut := c.loggedOut
	c.mtx.Unlock()
	credentialValue, enabled := c.signInCredentials()
	credentials := AutoLogin{enabled: enabled, credentials: credentialValue}
	if loggedOut {
		return nil
	}
	if bound {
		if !link.isBound(fence, c.metadataWatermark.Load()) {
			if err := c.bindLink(ctx, link); err != nil {
				return err
			}
		}
	} else if credentials.enabled {
		secret := vsr.NewBindSecret()
		var body []byte
		var err error
		code := command.LoginRegisterCode
		if credentials.credentials.personalAccessToken != "" {
			code = command.LoginRegisterWithPATCode
			body, err = vsr.SerializeLoginRegisterWithToken(credentials.credentials.personalAccessToken, iggcon.Version, secret)
		} else {
			body, err = vsr.SerializeLoginRegister(credentials.credentials.username, credentials.credentials.password, iggcon.Version, secret)
		}
		if err != nil {
			return err
		}
		if _, err = c.signIn(ctx, link, uint32(code), body, secret); err != nil {
			link.close(err)
			return err
		}
		c.mtx.Lock()
		c.rememberedLogin = credentials
		c.mtx.Unlock()
	} else {
		return nil
	}
	if err := c.refreshRoster(ctx, link); err != nil {
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
	if link.isBound(fence, identity.watermark) {
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
		valid := c.sessionFence == fence && c.session.ClientID() == identity.client && c.session.SessionID() == identity.session && bound.Session == identity.session
		c.mtx.Unlock()
		if !valid {
			return ierror.ErrSessionMismatch
		}
		link.markBound(fence, identity.watermark)
		return nil
	}
}

func (c *IggyTcpClient) resetSession(forget bool) {
	c.links.stopHeartbeat()
	c.mtx.Lock()
	c.endRecoveryLocked()
	c.session.Reset()
	c.sessionFence++
	c.operations.reset()
	c.polls.reset()
	c.groups.clear()
	c.topics.clearCounts()
	if forget {
		c.rememberedLogin = AutoLogin{}
		c.loggedOut = true
	}
	c.mtx.Unlock()
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
	c.sessionFence++
	c.operations.reset()
	c.polls.reset()
	c.groups.clear()
	c.topics.clearCounts()
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
		// answered goes again, until an answer, a sign-in by the caller or a
		// disconnect ends the recovery.
		for retry := uint32(0); ; retry++ {
			signInCtx, endSignIn := context.WithTimeout(ctx, responseReadTimeout)
			err := c.Connect(signInCtx)
			endSignIn()
			if err == nil || !c.config.reconnection.enabled || !unanswered(err) ||
				c.pause(ctx, reconnectDelay(c.config.reconnection.interval, retry)) != nil {
				return
			}
		}
	}()
}

// unanswered reports whether a sign-in failed before the server answered it,
// so trying it again can succeed.
func unanswered(err error) bool {
	return retryBeforeAdmission(err) || errors.Is(err, ierror.ErrCannotEstablishConnection) || errors.Is(err, context.DeadlineExceeded)
}

func (c *IggyTcpClient) LogoutUser(ctx context.Context) error {
	if _, err := c.do(ctx, &command.LogoutUser{}); err != nil {
		return err
	}
	c.resetSession(true)
	c.forgetLogin()
	c.links.disconnect(true)
	return nil
}

func (c *IggyTcpClient) signInCredentials() (Credentials, bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.config.autoLogin.enabled {
		return c.config.autoLogin.credentials, true
	}
	return c.rememberedLogin.credentials, c.rememberedLogin.enabled
}
func (c *IggyTcpClient) rememberLogin(credentials Credentials) {
	c.mtx.Lock()
	c.rememberedLogin = NewAutoLogin(credentials)
	c.mtx.Unlock()
}
func (c *IggyTcpClient) forgetLogin() { c.mtx.Lock(); c.rememberedLogin = AutoLogin{}; c.mtx.Unlock() }
