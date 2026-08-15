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

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/internal/command"
)

// DescribeOptions returns the option catalog for one resource scope.
//
// This is how a client learns which option keys the server accepts: a key
// outside the catalog is refused at create, and the binary transports carry
// only the error code back. Scopes with no keys yet return an empty slice.
func (c *IggyTcpClient) DescribeOptions(ctx context.Context, scope iggcon.OptionsScope) ([]iggcon.OptionSpec, error) {
	buffer, err := c.do(ctx, &command.DescribeOptions{Scope: scope})
	if err != nil {
		return nil, err
	}
	return binaryserialization.DeserializeOptionSpecs(buffer)
}

func (c *IggyTcpClient) GetStats(ctx context.Context) (*iggcon.Stats, error) {
	buffer, err := c.do(ctx, &command.GetStats{})
	if err != nil {
		return nil, err
	}

	s := &iggcon.Stats{}
	err = s.UnmarshalBinary(buffer)

	return s, err
}

func (c *IggyTcpClient) Ping(ctx context.Context) error {
	_, err := c.do(ctx, &command.Ping{})
	return err
}
