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
	"errors"
	"fmt"
	"log"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

// Tenant is one tenant of the multi-tenant examples: its ordinal, the stream it
// owns and the user that acts for it.
type Tenant struct {
	ID     int
	Stream string
	User   string
}

// EnsureUser creates the user with the given permissions, or replaces the
// permissions of a user an earlier run left behind, so a second run is safe.
func EnsureUser(ctx context.Context, cli iggcon.Client, username, password string, permissions *iggcon.Permissions) error {
	identifier, err := iggcon.NewIdentifier(username)
	if err != nil {
		return err
	}
	if _, err := cli.GetUser(ctx, identifier); err != nil {
		if _, err := cli.CreateUser(ctx, username, password, iggcon.Active, permissions, nil); err == nil {
			log.Printf("Created user %q", username)
			return nil
		} else if !errors.Is(err, ierror.ErrUserAlreadyExists) {
			return fmt.Errorf("create user %q: %w", username, err)
		}
	}
	if err := cli.UpdatePermissions(ctx, identifier, permissions); err != nil {
		return fmt.Errorf("update permissions of user %q: %w", username, err)
	}
	log.Printf("Reusing user %q with its permissions replaced", username)
	return nil
}

// EnsureNoAccess proves the tenant can read its own stream and cannot read any
// other tenant's stream.
func EnsureNoAccess(ctx context.Context, cli iggcon.Client, current Tenant, tenants []Tenant) error {
	own, err := iggcon.NewIdentifier(current.Stream)
	if err != nil {
		return err
	}
	if _, err := cli.GetStream(ctx, own); err != nil {
		return fmt.Errorf("tenant %s cannot read its own stream %q: %w", current.User, current.Stream, err)
	}
	log.Printf("Tenant %d (%s) can read its own stream %q", current.ID, current.User, current.Stream)

	for _, other := range tenants {
		if other.Stream == current.Stream {
			continue
		}
		identifier, err := iggcon.NewIdentifier(other.Stream)
		if err != nil {
			return err
		}
		if _, err := cli.GetStream(ctx, identifier); err != nil {
			if errors.Is(err, ierror.ErrUnauthorized) || errors.Is(err, ierror.ErrStreamIdNotFound) {
				log.Printf("Tenant %d (%s) was refused stream %q as expected",
					current.ID, current.User, other.Stream)
				continue
			}
			return fmt.Errorf("read stream %q as %s: %w", other.Stream, current.User, err)
		}
		return fmt.Errorf("tenant %s can read stream %q, the server should have refused it",
			current.User, other.Stream)
	}
	return nil
}
