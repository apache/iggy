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
	"fmt"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

// readWholeList reads a list one page at a time, in ascending id order and in
// pages of iggcon.MaxPageLimit, until a page has no next cursor. The walk is
// one call, so every page keeps the sign-in and the deadline that it started
// with. The result is not one consistent read: an entity that someone creates
// or deletes during the walk can appear or not appear. A page whose next
// cursor is the cursor of its request fails the walk with
// ierror.ErrInvalidFormat, so a faulty server cannot make the walk loop forever.
func readWholeList[T any](
	ctx context.Context,
	c *IggyTcpClient,
	readPage func(context.Context, iggcon.PageQuery) (*iggcon.Page[T], error),
) ([]T, error) {
	if ctx == nil {
		return nil, ierror.ErrNilContext
	}
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	query := iggcon.PageQuery{Order: iggcon.PageOrderID, Limit: iggcon.MaxPageLimit}
	items := []T{}
	for {
		page, err := readPage(ctx, query)
		if err != nil {
			return nil, err
		}
		items = append(items, page.Items...)
		if page.Next == nil {
			return items, nil
		}
		if query.Cursor != nil && *page.Next == *query.Cursor {
			return nil, fmt.Errorf("%w: the server answered the page cursor it was sent", ierror.ErrInvalidFormat)
		}
		query.Cursor = page.Next
	}
}
