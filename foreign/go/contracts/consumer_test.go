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

import (
	"bytes"
	"testing"
)

func TestDefaultConsumer_ReturnsSingleWithIdZero(t *testing.T) {
	c := DefaultConsumer()
	if c.Kind != ConsumerKindSingle {
		t.Fatalf("expected kind ConsumerKindSingle (%d), got %d", ConsumerKindSingle, c.Kind)
	}
	val, err := c.Id.Uint32()
	if err != nil {
		t.Fatalf("unexpected error getting id: %v", err)
	}
	if val != 0 {
		t.Fatalf("expected id 0, got %d", val)
	}
}

func TestNewSingleConsumer(t *testing.T) {
	id, err := NewIdentifier(uint32(5))
	if err != nil {
		t.Fatalf("unexpected error creating identifier: %v", err)
	}
	c := NewSingleConsumer(id)
	if c.Kind != ConsumerKindSingle {
		t.Fatalf("expected kind ConsumerKindSingle (%d), got %d", ConsumerKindSingle, c.Kind)
	}
	val, err := c.Id.Uint32()
	if err != nil {
		t.Fatalf("unexpected error getting id: %v", err)
	}
	if val != 5 {
		t.Fatalf("expected id 5, got %d", val)
	}
}

func TestNewGroupConsumer(t *testing.T) {
	id, err := NewIdentifier(uint32(10))
	if err != nil {
		t.Fatalf("unexpected error creating identifier: %v", err)
	}
	c := NewGroupConsumer(id)
	if c.Kind != ConsumerKindGroup {
		t.Fatalf("expected kind ConsumerKindGroup (%d), got %d", ConsumerKindGroup, c.Kind)
	}
	val, err := c.Id.Uint32()
	if err != nil {
		t.Fatalf("unexpected error getting id: %v", err)
	}
	if val != 10 {
		t.Fatalf("expected id 10, got %d", val)
	}
}

func TestConsumer_MarshalBinary_Single(t *testing.T) {
	c := DefaultConsumer()
	data, err := c.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data[0] != byte(ConsumerKindSingle) {
		t.Fatalf("expected first byte %d, got %d", ConsumerKindSingle, data[0])
	}
	idBytes, _ := c.Id.MarshalBinary()
	if !bytes.Equal(data[1:], idBytes) {
		t.Fatalf("expected id bytes %v, got %v", idBytes, data[1:])
	}
}

func TestConsumer_MarshalBinary_Group(t *testing.T) {
	id, err := NewIdentifier(uint32(7))
	if err != nil {
		t.Fatalf("unexpected error creating identifier: %v", err)
	}
	c := NewGroupConsumer(id)
	data, err := c.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data[0] != byte(ConsumerKindGroup) {
		t.Fatalf("expected first byte %d, got %d", ConsumerKindGroup, data[0])
	}
	idBytes, _ := id.MarshalBinary()
	if !bytes.Equal(data[1:], idBytes) {
		t.Fatalf("expected id bytes %v, got %v", idBytes, data[1:])
	}
}
