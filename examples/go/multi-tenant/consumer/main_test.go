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
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// A stream-level read_stream already implies read_topics and poll_messages, so
// this control pins the consumer to the stream grant alone.
func TestConsumerPermissionsStayInsideTheTenantStream(t *testing.T) {
	permissions := consumerPermissions(7)

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
	if !stream.ReadStream {
		t.Fatalf("the tenant needs read on its own stream, got %+v", stream)
	}
	if stream.ManageStream || stream.ManageTopics || stream.SendMessages || len(stream.Topics) != 0 {
		t.Fatalf("a consumer must only read its own stream, got %+v", stream)
	}
}
