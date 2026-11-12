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

package common

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

const (
	// DefaultTenants is the tenant count the multi-tenant examples use when
	// TENANTS_COUNT is unset.
	DefaultTenants = 3
	connectTimeout = 30 * time.Second
)

// ServerOptions parses the --tcp-server-address flag the examples share and
// returns the address together with the TCP options it implies.
func ServerOptions() (string, []tcp.Option) {
	tcpServerAddr := flag.String("tcp-server-address", "127.0.0.1:8090", "TCP server address")
	flag.Parse()

	if _, err := net.ResolveTCPAddr("tcp", *tcpServerAddr); err != nil {
		log.Fatalf("Invalid server address %s! Usage: --tcp-server-address <server-address>", *tcpServerAddr)
	}

	return *tcpServerAddr, []tcp.Option{tcp.WithServerAddress(*tcpServerAddr)}
}

// TenantsFromEnv reads the tenant count from TENANTS_COUNT.
func TenantsFromEnv() int {
	raw, ok := os.LookupEnv("TENANTS_COUNT")
	if !ok || raw == "" {
		return DefaultTenants
	}
	count, err := strconv.Atoi(raw)
	if err != nil || count < 1 {
		log.Fatalf("TENANTS_COUNT must be a positive integer, got %q", raw)
	}
	return count
}

// Dial connects to the server and logs in as the given user.
func Dial(ctx context.Context, address string, options []tcp.Option, username, password string) (iggcon.Client, error) {
	cli, err := client.NewIggyClient(client.WithTcp(options...))
	if err != nil {
		return nil, err
	}
	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	if err := cli.Connect(connectCtx); err != nil {
		return nil, fmt.Errorf("connect to %s: %w", address, err)
	}
	if _, err := cli.LoginUser(connectCtx, username, password); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("log in as %q: %w", username, err)
	}
	return cli, nil
}

// Close closes the client, logging a failure instead of hiding it.
func Close(cli iggcon.Client) {
	if cli == nil {
		return
	}
	if err := cli.Close(); err != nil {
		log.Printf("Error closing client: %v", err)
	}
}
