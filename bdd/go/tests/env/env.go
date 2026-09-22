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

// Package env reads the endpoints and credentials the BDD suites run
// against. A default here would turn a dropped compose variable into a run
// against whatever happens to listen on the fallback address, so a missing
// value aborts the suite instead.
package env

import (
	"fmt"
	"os"
)

func required(name string) string {
	value, ok := os.LookupEnv(name)
	if !ok || value == "" {
		panic(fmt.Sprintf("%s must be set; run the suite via scripts/run-bdd-tests.sh", name))
	}
	return value
}

// ServerAddress returns the address of the single-node server.
func ServerAddress() string {
	return required("IGGY_TCP_ADDRESS")
}

// LeaderAddress returns the address of the cluster leader.
func LeaderAddress() string {
	return required("IGGY_TCP_ADDRESS_LEADER")
}

// FollowerAddress returns the address of the cluster follower.
func FollowerAddress() string {
	return required("IGGY_TCP_ADDRESS_FOLLOWER")
}

// RootCredentials returns the root username and password.
func RootCredentials() (string, string) {
	return required("IGGY_ROOT_USERNAME"), required("IGGY_ROOT_PASSWORD")
}
