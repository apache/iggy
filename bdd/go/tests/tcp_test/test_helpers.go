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

package tcp_test

import (
	"context"
	"math/rand"
	"strings"
	"time"

	"github.com/apache/iggy/bdd/go/tests/env"
	"github.com/apache/iggy/foreign/go/client"
	iggcon "github.com/apache/iggy/foreign/go/contracts"

	"github.com/apache/iggy/foreign/go/client/tcp"
)

func createAuthorizedConnection() iggcon.Client {
	cli := createClient()
	username, password := env.RootCredentials()
	_, err := cli.LoginUser(context.Background(), username, password)
	if err != nil {
		panic(err)
	}
	return cli
}

func createClient() iggcon.Client {
	cli, err := client.NewIggyClient(
		client.WithTcp(
			tcp.WithServerAddress(env.ServerAddress()),
		),
	)
	if err != nil {
		panic(err)
	}
	if err := cli.Connect(context.Background()); err != nil {
		panic(err)
	}

	return cli
}

// maxRoutableId is the highest stream or topic id the server's routing
// namespace can address. A larger id cannot be routed at all, so specs that
// want an id the server has never seen must stay inside this range to get the
// server's answer rather than a routing failure.
const maxRoutableId = 4095

func createRandomUInt32() uint32 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var v uint32
	for v == 0 {
		v = r.Uint32() % (maxRoutableId + 1)
	}
	return v
}

func randomU32Identifier() iggcon.Identifier {
	id, _ := iggcon.NewIdentifier(createRandomUInt32())
	return id
}

func createRandomString(length int) string {
	// Define the character set from which to create the random string
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"

	// Initialize the random number generator with a seed based on the current time
	rand.New(rand.NewSource(time.Now().UnixNano()))

	// Create the random string
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}

func createRandomStringWithPrefix(prefix string, length int) string {
	// Define the character set from which to create the random string
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"

	// Initialize the random number generator with a seed based on the current time
	rand.New(rand.NewSource(time.Now().UnixNano()))

	// Create the random string
	result := make([]byte, length-len(prefix))
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return strings.ToLower(prefix) + string(result)
}
