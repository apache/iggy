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

package command

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

func TestGetClientMarshalBinary(t *testing.T) {
	tests := []struct {
		name     string
		clientID uint32
		want     []byte
	}{
		{"zero", 0, []byte{0, 0, 0, 0}},
		{"little endian", 0x01020304, []byte{4, 3, 2, 1}},
		{"maximum", ^uint32(0), []byte{255, 255, 255, 255}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := (&GetClient{ClientID: test.clientID}).MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, test.want) {
				t.Fatalf("GetClient body = %v, want %v", got, test.want)
			}
		})
	}
}

func TestDescribeOptionsMarshalBinary(t *testing.T) {
	for _, test := range []struct {
		name  string
		scope iggcon.OptionsScope
		want  byte
	}{
		{"topic", iggcon.OptionsScopeTopic, 1},
		{"stream", iggcon.OptionsScopeStream, 2},
		{"user", iggcon.OptionsScopeUser, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := (&DescribeOptions{Scope: test.scope}).MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, []byte{test.want}) {
				t.Fatalf("DescribeOptions body = %v, want [%d]", got, test.want)
			}
		})
	}

	for _, scope := range []iggcon.OptionsScope{0, 4, 255} {
		t.Run(scope.String(), func(t *testing.T) {
			got, err := (&DescribeOptions{Scope: scope}).MarshalBinary()
			if err == nil {
				t.Fatalf("scope %d unexpectedly accepted: %v", scope, got)
			}
			if len(got) != 0 {
				t.Fatalf("scope %d returned a body on error: %v", scope, got)
			}
		})
	}
}

func TestGetMetadataChangesMarshalBinary(t *testing.T) {
	earliest := iggcon.NewMetadataChangesQuery(iggcon.EarliestMetadataChangeStart())
	earliestBody := append([]byte{0}, make([]byte, iggcon.MetadataChangeCursorSize)...)
	earliestBody = append(earliestBody, 0, 0, 100, 0, 0, 0, 0, 0, 0)

	t.Run("earliest every group", func(t *testing.T) {
		got, err := (&GetMetadataChanges{Query: earliest}).MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, earliestBody) {
			t.Fatalf("GetMetadataChanges body = %v, want %v", got, earliestBody)
		}
	})

	t.Run("appends after a prefix", func(t *testing.T) {
		got, err := (&GetMetadataChanges{Query: earliest}).AppendBinary([]byte{0xaa})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, append([]byte{0xaa}, earliestBody...)) {
			t.Fatalf("GetMetadataChanges appended %v", got)
		}
	})

	t.Run("cursor and token only where the start uses them", func(t *testing.T) {
		query := earliest
		query.Start = iggcon.LatestMetadataChangeStart()
		query.Start.Cursor = iggcon.MetadataChangeCursor{Op: 7, Ordinal: 1}
		query.Start.PageToken = &iggcon.MetadataCurrentPageToken{}
		got, err := (&GetMetadataChanges{Query: query}).MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		want := append([]byte{1}, earliestBody[1:]...)
		if !bytes.Equal(got, want) {
			t.Fatalf("GetMetadataChanges body = %v, want %v", got, want)
		}
	})

	t.Run("oversized groups retain every requested group", func(t *testing.T) {
		query := earliest
		query.Groups = []iggcon.MetadataChangeGroup{iggcon.MetadataChangeGroupTopics, iggcon.MetadataChangeGroupStreams, iggcon.MetadataChangeGroupTopics}
		for len(query.Groups) < 300 {
			query.Groups = append(query.Groups, iggcon.MetadataChangeGroupUsers)
		}
		got, err := (&GetMetadataChanges{Query: query}).MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		groupsAt := len(earliestBody) - 3
		if got[groupsAt] != 3 || len(got) != len(earliestBody)+3 {
			t.Fatalf("GetMetadataChanges carries %d groups in %d bytes", got[groupsAt], len(got))
		}
		if !bytes.Equal(got[groupsAt+1:groupsAt+4], []byte{2, 1, 6}) {
			t.Fatalf("GetMetadataChanges groups start %v", got[groupsAt+1:groupsAt+4])
		}
	})

	t.Run("time bounds behind presence bytes", func(t *testing.T) {
		since, until := uint64(0x0102030405060708), uint64(0x1112131415161718)
		query := earliest
		query.Since, query.Until = &since, &until
		got, err := (&GetMetadataChanges{Query: query}).MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		bounds := got[1+iggcon.MetadataChangeCursorSize : 1+iggcon.MetadataChangeCursorSize+18]
		want := []byte{1, 8, 7, 6, 5, 4, 3, 2, 1, 1, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11}
		if !bytes.Equal(bounds, want) {
			t.Fatalf("GetMetadataChanges bounds = %v, want %v", bounds, want)
		}
	})

	for _, limit := range []uint32{0, iggcon.MaxMetadataChangesLimit + 1} {
		t.Run(fmt.Sprintf("limit %d refused before encoding", limit), func(t *testing.T) {
			query := earliest
			query.Limit = limit
			got, err := (&GetMetadataChanges{Query: query}).AppendBinary([]byte{0xaa})
			if !errors.Is(err, ierror.ErrInvalidCommand) {
				t.Fatalf("limit %d error = %v", limit, err)
			}
			if got != nil {
				t.Fatalf("limit %d returned a body on error: %v", limit, got)
			}
		})
	}
}

func TestGetMetadataChanges_GivenManyDuplicateGroupsWhenEncodingShouldPreserveEveryRequestedGroup(t *testing.T) {
	query := iggcon.NewMetadataChangesQuery(iggcon.EarliestMetadataChangeStart())
	for range 255 {
		query.Groups = append(query.Groups, iggcon.MetadataChangeGroupStreams)
	}
	query.Groups = append(query.Groups, iggcon.MetadataChangeGroupUsers)
	actual, err := (&GetMetadataChanges{Query: query}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	query.Groups = []iggcon.MetadataChangeGroup{iggcon.MetadataChangeGroupStreams, iggcon.MetadataChangeGroupUsers}
	expected, err := (&GetMetadataChanges{Query: query}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, expected) {
		t.Fatal("encoding lost a requested group")
	}
}

func TestEmptySystemCommandBodies(t *testing.T) {
	for _, test := range []struct {
		name    string
		command Command
	}{
		{"GetClients", &GetClients{}},
		{"GetClusterMetadata", &GetClusterMetadata{}},
		{"GetStats", &GetStats{}},
		{"GetMe", &GetMe{}},
		{"Ping", &Ping{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.command.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 0 {
				t.Fatalf("%s body = %v, want empty", test.name, got)
			}
		})
	}
}
