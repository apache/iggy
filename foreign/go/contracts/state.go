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

type TransportState uint8

const (
	TransportStateDisconnected TransportState = iota
	TransportStateShutdown
	TransportStateConnecting
	TransportStateConnected
)

func (s TransportState) String() string {
	switch s {
	case TransportStateShutdown:
		return "shutdown"
	case TransportStateDisconnected:
		return "disconnected"
	case TransportStateConnecting:
		return "connecting"
	case TransportStateConnected:
		return "connected"
	default:
		return "unknown"
	}
}

type SessionState uint8

const (
	SessionStateUnauthenticated SessionState = iota
	SessionStateAuthenticated
)

func (s SessionState) String() string {
	switch s {
	case SessionStateUnauthenticated:
		return "unauthenticated"
	case SessionStateAuthenticated:
		return "authenticated"
	default:
		return "unknown"
	}
}
