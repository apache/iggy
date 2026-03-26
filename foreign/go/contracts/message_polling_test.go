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

package iggcon

import "testing"

func TestOffsetPollingStrategy(t *testing.T) {
	s := OffsetPollingStrategy(100)
	if s.Kind != POLLING_OFFSET {
		t.Fatalf("expected kind POLLING_OFFSET (%d), got %d", POLLING_OFFSET, s.Kind)
	}
	if s.Value != 100 {
		t.Fatalf("expected value 100, got %d", s.Value)
	}
}

func TestTimestampPollingStrategy(t *testing.T) {
	s := TimestampPollingStrategy(200)
	if s.Kind != POLLING_TIMESTAMP {
		t.Fatalf("expected kind POLLING_TIMESTAMP (%d), got %d", POLLING_TIMESTAMP, s.Kind)
	}
	if s.Value != 200 {
		t.Fatalf("expected value 200, got %d", s.Value)
	}
}

func TestFirstPollingStrategy(t *testing.T) {
	s := FirstPollingStrategy()
	if s.Kind != POLLING_FIRST {
		t.Fatalf("expected kind POLLING_FIRST (%d), got %d", POLLING_FIRST, s.Kind)
	}
	if s.Value != 0 {
		t.Fatalf("expected value 0, got %d", s.Value)
	}
}

func TestLastPollingStrategy(t *testing.T) {
	s := LastPollingStrategy()
	if s.Kind != POLLING_LAST {
		t.Fatalf("expected kind POLLING_LAST (%d), got %d", POLLING_LAST, s.Kind)
	}
	if s.Value != 0 {
		t.Fatalf("expected value 0, got %d", s.Value)
	}
}

func TestNextPollingStrategy(t *testing.T) {
	s := NextPollingStrategy()
	if s.Kind != POLLING_NEXT {
		t.Fatalf("expected kind POLLING_NEXT (%d), got %d", POLLING_NEXT, s.Kind)
	}
	if s.Value != 0 {
		t.Fatalf("expected value 0, got %d", s.Value)
	}
}

func TestNewPollingStrategy_Custom(t *testing.T) {
	s := NewPollingStrategy(POLLING_OFFSET, 999)
	if s.Kind != POLLING_OFFSET {
		t.Fatalf("expected kind POLLING_OFFSET (%d), got %d", POLLING_OFFSET, s.Kind)
	}
	if s.Value != 999 {
		t.Fatalf("expected value 999, got %d", s.Value)
	}
}
