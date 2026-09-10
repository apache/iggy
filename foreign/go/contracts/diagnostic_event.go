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

// DiagnosticEvent describes a client-level lifecycle change that subscribers
// can observe via Client.SubscribeEvents.
type DiagnosticEvent int

const (
	DiagnosticEventShutdown DiagnosticEvent = iota
	DiagnosticEventDisconnected
	DiagnosticEventConnected
	DiagnosticEventSignedIn
	DiagnosticEventSignedOut
	DiagnosticEventRedirected
)

func (e DiagnosticEvent) String() string {
	switch e {
	case DiagnosticEventShutdown:
		return "shutdown"
	case DiagnosticEventDisconnected:
		return "disconnected"
	case DiagnosticEventConnected:
		return "connected"
	case DiagnosticEventSignedIn:
		return "signed_in"
	case DiagnosticEventSignedOut:
		return "signed_out"
	case DiagnosticEventRedirected:
		return "redirected"
	default:
		return "unknown"
	}
}
