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

package client_test

import (
	"context"
	"log"
	"time"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// The README's metadata changes sample, kept here so it compiles.
func Example_metadataChanges() {
	ctx := context.Background()
	cli, err := client.NewIggyClient(client.WithTcp(tcp.WithServerAddress("127.0.0.1:8090")))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = cli.Close() }()
	if err := cli.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	if _, err := cli.LoginUser(ctx, "iggy", "iggy"); err != nil {
		log.Fatal(err)
	}

	err = followMetadataChanges(ctx, cli, iggcon.MetadataChangeGroupStreams, iggcon.MetadataChangeGroupTopics)
	if err != nil {
		log.Fatal(err)
	}
}

func followMetadataChanges(ctx context.Context, cli iggcon.Client, groups ...iggcon.MetadataChangeGroup) error {
	query := iggcon.NewMetadataChangesQuery(iggcon.EarliestMetadataChangeStart(), groups...)
	for {
		page, err := cli.GetMetadataChanges(ctx, query)
		if err != nil {
			return err
		}
		switch page.Status {
		case iggcon.MetadataChangeStatusOk:
			for _, record := range page.Records {
				log.Printf("op %d %s: %+v", record.Op, record.Kind, record.Body)
			}
			if page.Next != nil {
				query.Start = iggcon.AfterMetadataChangeStart(*page.Next)
			}
			if !page.HasMore {
				time.Sleep(time.Second)
			}
		case iggcon.MetadataChangeStatusBehind:
			time.Sleep(100 * time.Millisecond)
		default:
			if query.Start, err = rebuildMetadataMirror(ctx, cli, groups); err != nil {
				return err
			}
		}
	}
}

// rebuildMetadataMirror pages the current state, then returns the position to
// follow from.
func rebuildMetadataMirror(
	ctx context.Context,
	cli iggcon.Client,
	groups []iggcon.MetadataChangeGroup,
) (iggcon.MetadataChangeStart, error) {
	query := iggcon.NewMetadataChangesQuery(iggcon.CurrentMetadataChangeStart(nil), groups...)
	for {
		page, err := cli.GetMetadataChanges(ctx, query)
		if err != nil {
			return iggcon.MetadataChangeStart{}, err
		}
		switch page.Status {
		case iggcon.MetadataChangeStatusBehind:
			time.Sleep(100 * time.Millisecond)
			continue
		case iggcon.MetadataChangeStatusExpired, iggcon.MetadataChangeStatusReset:
			query.Start = iggcon.CurrentMetadataChangeStart(nil)
			continue
		}
		for _, item := range page.Items {
			log.Printf("%s: %+v", item.Group, item.Body)
		}
		switch {
		case page.CurrentPageToken != nil:
			query.Start = iggcon.CurrentMetadataChangeStart(page.CurrentPageToken)
		case page.Next != nil:
			return iggcon.AfterMetadataChangeStart(*page.Next), nil
		default:
			query.Start = iggcon.CurrentMetadataChangeStart(nil)
		}
	}
}
