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
	"bufio"
	"context"
	"io"
	"net"
	"sync"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/vsr"
)

const (
	linkWriterCapacity  = 32
	linkPendingCapacity = 256
	linkReaderCapacity  = 64 * 1024
)

type linkReply struct {
	header [vsr.HeaderSize]byte
	body   []byte
	err    error
}

type linkPending struct{ reply chan linkReply }

type tcpLink struct {
	boundFence     uint64
	boundWatermark uint64
	bindGate       chan struct{}
	conn           net.Conn
	endpoint       string
	mu             sync.Mutex
	pending        map[uint64]*linkPending
	capacity       chan struct{}
	writes         chan *operationFrame
	closed         chan struct{}
	senders        sync.WaitGroup
	closeOnce      sync.Once
	done           sync.WaitGroup
}

func newTCPLink(conn net.Conn, endpoint string) *tcpLink {
	link := &tcpLink{bindGate: make(chan struct{}, 1), conn: conn, endpoint: endpoint, pending: make(map[uint64]*linkPending), capacity: make(chan struct{}, linkPendingCapacity), writes: make(chan *operationFrame, linkWriterCapacity), closed: make(chan struct{})}
	link.done.Add(2)
	go link.writeLoop()
	go link.readLoop()
	return link
}

func (l *tcpLink) exchange(ctx context.Context, frame *operationFrame) (linkReply, bool) {
	select {
	case l.capacity <- struct{}{}:
	case <-ctx.Done():
		return linkReply{err: ierror.ErrTransientNotAccepted}, false
	case <-l.closed:
		return linkReply{err: ierror.ErrTransientNotAccepted}, false
	}
	pending := &linkPending{reply: make(chan linkReply, 1)}
	id := vsr.StampedRequestID(frame.data)
	l.mu.Lock()
	select {
	case <-l.closed:
		l.mu.Unlock()
		<-l.capacity
		return linkReply{err: ierror.ErrTransientNotAccepted}, false
	default:
	}
	if l.pending[id] != nil {
		l.mu.Unlock()
		<-l.capacity
		return linkReply{err: ierror.ErrTransientNotAccepted}, false
	}
	l.pending[id] = pending
	l.senders.Add(1)
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		if l.pending[id] == pending {
			delete(l.pending, id)
		}
		l.mu.Unlock()
		<-l.capacity
	}()
	queued := frame.acquire()
	sent := false
	select {
	case l.writes <- queued:
		sent = true
	case <-ctx.Done():
		queued.release()
	case <-l.closed:
		queued.release()
	}
	l.senders.Done()
	if !sent {
		return linkReply{err: ierror.ErrTransientNotAccepted}, false
	}
	select {
	case reply := <-pending.reply:
		return reply, true
	case <-ctx.Done():
		return linkReply{err: ierror.ErrTransientNotCommitted}, true
	case <-l.closed:
		select {
		case reply := <-pending.reply:
			return reply, true
		default:
		}
		return linkReply{err: ierror.ErrTransientNotCommitted}, true
	}
}

func (l *tcpLink) close(err error) {
	l.closeOnce.Do(func() {
		l.mu.Lock()
		for id, pending := range l.pending {
			pending.reply <- linkReply{err: err}
			delete(l.pending, id)
		}
		close(l.closed)
		l.mu.Unlock()
		_ = l.conn.Close()
	})
}

func (l *tcpLink) writeLoop() {
	defer l.done.Done()
	defer func() {
		l.senders.Wait()
		for {
			select {
			case frame := <-l.writes:
				frame.release()
			default:
				return
			}
		}
	}()
	for {
		select {
		case <-l.closed:
			return
		case frame := <-l.writes:
			remaining := frame.data
			for len(remaining) > 0 {
				count, err := l.conn.Write(remaining)
				if err != nil || count == 0 {
					frame.release()
					l.close(ierror.ErrTransientNotCommitted)
					return
				}
				remaining = remaining[count:]
			}
			frame.release()
		}
	}
}

func (l *tcpLink) readLoop() {
	defer l.done.Done()
	reader := bufio.NewReaderSize(l.conn, linkReaderCapacity)
	for {
		var reply linkReply
		if _, err := io.ReadFull(reader, reply.header[:]); err != nil {
			l.close(ierror.ErrTransientNotCommitted)
			return
		}
		size := vsr.ReadSize(&reply.header)
		if size < vsr.HeaderSize || size > vsr.MaxFrameSize {
			l.close(ierror.ErrInvalidCommand)
			return
		}
		reply.body = make([]byte, int(size)-vsr.HeaderSize)
		if _, err := io.ReadFull(reader, reply.body); err != nil {
			l.close(ierror.ErrTransientNotCommitted)
			return
		}
		if vsr.PeekCommand(&reply.header) == vsr.FrameEviction {
			eviction := vsr.ReadEviction(&reply.header)
			// The server drops a stale connection but keeps its session, so
			// the link is lost and the next one binds the same session.
			if eviction.Reason == vsr.EvictionStaleClient {
				l.close(ierror.ErrTransientNotCommitted)
				return
			}
			l.close(vsr.NewEvictionError(eviction))
			return
		}
		if vsr.PeekCommand(&reply.header) != vsr.FrameReply {
			l.close(ierror.ErrInvalidCommand)
			return
		}
		id := vsr.ReadReplyRequestID(&reply.header)
		l.mu.Lock()
		if pending := l.pending[id]; pending != nil {
			delete(l.pending, id)
			pending.reply <- reply
		}
		l.mu.Unlock()
	}
}

func (l *tcpLink) markBound(fence, watermark uint64) {
	l.mu.Lock()
	l.boundFence = fence
	l.boundWatermark = watermark
	l.mu.Unlock()
}
func (l *tcpLink) isBound(fence, watermark uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.boundFence == fence && l.boundWatermark >= watermark
}
