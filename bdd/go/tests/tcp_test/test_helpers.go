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
	"math/rand"
	"os"
	"strings"
	"time"

	. "github.com/apache/iggy/foreign/go"
	. "github.com/apache/iggy/foreign/go/contracts"
)

func createAuthorizedConnection() MessageStream {
	ms := createConnection()
	_, err := ms.LogIn(LogInRequest{
		Username: "iggy",
		Password: "iggy",
	})
	if err != nil {
		panic(err)
	}
	return ms
}

func createConnection() MessageStream {
	addr := os.Getenv("IGGY_TCP_ADDRESS")
	if addr == "" {
		addr = "127.0.0.1:8090"
	}
	factory := &IggyClientFactory{}
	config := IggyConfiguration{
		BaseAddress: addr,
		Protocol:    Tcp,
	}

	ms, err := factory.CreateMessageStream(config)
	if err != nil {
		panic(err)
	}
	return ms
}

func createRandomUInt32() uint32 {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	return rand.Uint32()
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
