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
	"sync"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// subscriberBufferSize bounds each subscriber's channel. A subscriber that
// fails to drain within this many events will start dropping the oldest
// events; the publisher is never blocked.
const subscriberBufferSize = 1000

// eventBroadcaster fans out diagnostic events to any number of subscribers.
// Each call to subscribe returns an independent channel; publish delivers
// the event to every live subscriber non-blockingly. close releases all
// subscriber channels and is idempotent.
type eventBroadcaster struct {
	mu          sync.Mutex
	subscribers []chan iggcon.DiagnosticEvent
	closed      bool
}

func newEventBroadcaster() *eventBroadcaster {
	return &eventBroadcaster{}
}

// subscribe returns a new subscriber channel and an unsubscribe function that
// is idempotent and safe to call after the broadcaster is closed.
func (b *eventBroadcaster) subscribe() (<-chan iggcon.DiagnosticEvent, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan iggcon.DiagnosticEvent, subscriberBufferSize)
	if b.closed {
		close(ch)
		return ch, func() {}
	}
	b.subscribers = append(b.subscribers, ch)

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			if b.closed {
				return
			}
			for i, sub := range b.subscribers {
				if sub == ch {
					b.subscribers = append(b.subscribers[:i], b.subscribers[i+1:]...)
					close(ch)
					return
				}
			}
		})
	}
	return ch, unsubscribe
}

func (b *eventBroadcaster) publish(event iggcon.DiagnosticEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}
	for _, ch := range b.subscribers {
		select {
		case ch <- event:
		default:
			// Subscriber is not draining fast enough. Drop the oldest event
			// to make room for the newest, preferring recency. Keeps the
			// publisher non-blocking even with a slow subscriber.
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- event:
			default:
			}
		}
	}
}

func (b *eventBroadcaster) close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}
	b.closed = true
	for _, ch := range b.subscribers {
		close(ch)
	}
	b.subscribers = nil
}
