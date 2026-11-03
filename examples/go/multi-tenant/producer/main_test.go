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

package main

import (
	"context"
	"strings"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

type readableStreams struct {
	iggcon.Client
	names map[string]bool
}

func (c *readableStreams) GetStream(_ context.Context, id iggcon.Identifier) (*iggcon.StreamDetails, error) {
	name, err := id.String()
	if err != nil {
		return nil, err
	}
	if !c.names[name] {
		return nil, ierror.ErrUnauthorized
	}
	return &iggcon.StreamDetails{}, nil
}

func TestEnsureNoAccessFailsWhenANeighbourStreamIsReadable(t *testing.T) {
	tenants := []tenant{
		{id: 1, stream: "tenant_1_stream", user: "tenant_1_producer"},
		{id: 2, stream: "tenant_2_stream", user: "tenant_2_producer"},
	}
	refused := &readableStreams{names: map[string]bool{"tenant_1_stream": true}}
	if err := ensureNoAccess(t.Context(), refused, tenants[0], tenants); err != nil {
		t.Fatalf("a tenant refused its neighbour's stream should pass the check: %v", err)
	}

	leaky := &readableStreams{names: map[string]bool{"tenant_1_stream": true, "tenant_2_stream": true}}
	err := ensureNoAccess(t.Context(), leaky, tenants[0], tenants)
	if err == nil {
		t.Fatal("a tenant that can read its neighbour's stream must fail the check")
	}
	if !strings.Contains(err.Error(), "should have refused") || !strings.Contains(err.Error(), "tenant_2_stream") {
		t.Fatalf("the failure should name the readable stream, got %v", err)
	}
}

func TestProducerPermissionsStayInsideTheTenantStream(t *testing.T) {
	permissions := producerPermissions(7)

	if permissions.Global != (iggcon.GlobalPermissions{}) {
		t.Fatalf("a tenant must not get global permissions, got %+v", permissions.Global)
	}
	if len(permissions.Streams) != 1 {
		t.Fatalf("a tenant must be scoped to exactly one stream, got %d", len(permissions.Streams))
	}
	stream, ok := permissions.Streams[7]
	if !ok {
		t.Fatalf("the tenant's own stream is missing: %+v", permissions.Streams)
	}
	if !stream.ReadStream || !stream.SendMessages {
		t.Fatalf("the tenant needs read and send on its own stream, got %+v", stream)
	}
	if stream.ManageStream || stream.ManageTopics || len(stream.Topics) != 0 {
		t.Fatalf("the tenant is granted more than sending, got %+v", stream)
	}
}
