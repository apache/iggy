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

package tcp

import (
	"context"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
)

func (c *IggyTcpClient) GetClusterMetadata(ctx context.Context) (*iggcon.ClusterMetadata, error) {
	if ctx == nil {
		return nil, ierror.ErrNilContext
	}
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	for {
		// The fence refuses a roster that a session change overtook, so it is
		// taken only after a running recovery, and a recovery that overtakes
		// the read starts it again under the new session.
		if err := c.waitForRecovery(ctx); err != nil {
			return nil, err
		}
		c.mtx.Lock()
		fence := c.sessionFence
		c.mtx.Unlock()
		body, err := c.do(ctx, &command.GetClusterMetadata{})
		if err != nil {
			return nil, err
		}
		var metadata iggcon.ClusterMetadata
		if err = metadata.UnmarshalBinary(body); err != nil {
			return nil, err
		}
		err = c.installRoster(fence, &metadata)
		if err == nil {
			return &metadata, nil
		}
		if !c.sessionRecovers(ctx, err) {
			return nil, err
		}
	}
}

func (c *IggyTcpClient) refreshRoster(ctx context.Context, link *tcpLink) error {
	if link == nil {
		link = c.links.any()
	}
	if link == nil {
		return ierror.ErrTransientNotAccepted
	}
	c.mtx.Lock()
	fence := c.sessionFence
	c.mtx.Unlock()
	body, err := c.readOnLink(ctx, link, command.GetClusterMetadataCode, nil, nil)
	if err != nil {
		return err
	}
	var metadata iggcon.ClusterMetadata
	if err = metadata.UnmarshalBinary(body); err != nil {
		return err
	}
	return c.installRoster(fence, &metadata)
}

// installRoster installs metadata read under the session of fence, unless a
// session change overtook that read.
func (c *IggyTcpClient) installRoster(fence uint64, metadata *iggcon.ClusterMetadata) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.sessionFence != fence {
		return ierror.ErrUnauthenticated
	}
	return c.links.install(metadata)
}
