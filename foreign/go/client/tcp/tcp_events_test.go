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
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func recvWithTimeout(t *testing.T, ch <-chan iggcon.DiagnosticEvent) (iggcon.DiagnosticEvent, bool) {
	t.Helper()
	select {
	case ev, ok := <-ch:
		return ev, ok
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
		return 0, false
	}
}

func TestEventBroadcaster_DeliversToAllSubscribers(t *testing.T) {
	b := newEventBroadcaster()
	a, _ := b.subscribe()
	c, _ := b.subscribe()

	b.publish(iggcon.DiagnosticEventConnected)

	if ev, _ := recvWithTimeout(t, a); ev != iggcon.DiagnosticEventConnected {
		t.Errorf("subscriber A got %v, want connected", ev)
	}
	if ev, _ := recvWithTimeout(t, c); ev != iggcon.DiagnosticEventConnected {
		t.Errorf("subscriber B got %v, want connected", ev)
	}
}

func TestEventBroadcaster_PreservesOrderForOneSubscriber(t *testing.T) {
	b := newEventBroadcaster()
	ch, _ := b.subscribe()

	want := []iggcon.DiagnosticEvent{
		iggcon.DiagnosticEventConnected,
		iggcon.DiagnosticEventSignedIn,
		iggcon.DiagnosticEventDisconnected,
	}
	for _, ev := range want {
		b.publish(ev)
	}
	for i, w := range want {
		got, _ := recvWithTimeout(t, ch)
		if got != w {
			t.Errorf("event %d: got %v, want %v", i, got, w)
		}
	}
}

func TestEventBroadcaster_CloseDeliversNoMoreEvents(t *testing.T) {
	b := newEventBroadcaster()
	ch, _ := b.subscribe()

	b.close()

	// channel should be closed; receive returns zero, ok=false
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for closed channel")
	}

	// subsequent publish is a no-op
	b.publish(iggcon.DiagnosticEventConnected)

	// late subscribe returns an already-closed channel
	late, _ := b.subscribe()
	select {
	case _, ok := <-late:
		if ok {
			t.Fatal("expected late-subscribe channel to be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for closed late-subscribe channel")
	}
}

func TestEventBroadcaster_SlowSubscriberDoesNotBlockPublisher(t *testing.T) {
	b := newEventBroadcaster()
	_, _ = b.subscribe() // never drained

	// Publish more events than the buffer; if the publisher blocked or
	// panicked we'd hang or fail here.
	for range subscriberBufferSize * 2 {
		b.publish(iggcon.DiagnosticEventConnected)
	}
}

func TestEventBroadcaster_CloseIsIdempotent(t *testing.T) {
	b := newEventBroadcaster()
	b.subscribe()
	b.close()
	b.close() // must not panic
}

func TestEventBroadcaster_UnsubscribeStopsDelivery(t *testing.T) {
	b := newEventBroadcaster()
	ch, unsubscribe := b.subscribe()

	unsubscribe()

	if _, ok := <-ch; ok {
		t.Fatal("expected channel to be closed after unsubscribe")
	}
	b.publish(iggcon.DiagnosticEventConnected)

	other, _ := b.subscribe()
	b.publish(iggcon.DiagnosticEventSignedIn)
	if ev, _ := recvWithTimeout(t, other); ev != iggcon.DiagnosticEventSignedIn {
		t.Errorf("remaining subscriber got %v, want signed_in", ev)
	}
}

func TestEventBroadcaster_UnsubscribeIsIdempotentAndSafeAfterClose(t *testing.T) {
	b := newEventBroadcaster()
	_, unsubscribe := b.subscribe()

	unsubscribe()
	unsubscribe() // second call must not panic or double-close

	b.close()
	unsubscribe() // after close must not panic
}
