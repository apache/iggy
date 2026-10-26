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

package tests

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/apache/iggy/bdd/go/tests/env"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/cucumber/godog"
)

const (
	// A two phase delete commits its record after the call returns.
	metadataChangesFollowAttempts = 100
	metadataChangesFollowInterval = 100 * time.Millisecond
	rootUserID                    = 0
)

type metadataChangesCtxKey struct{}

type metadataChangesCtx struct {
	groups        []iggcon.MetadataChangeGroup
	cursor        *iggcon.MetadataChangeCursor
	followed      []iggcon.MetadataChangeRecord
	currentItems  []iggcon.MetadataCurrentItem
	signedInAsNew bool
}

func getMetadataChangesCtx(ctx context.Context) *metadataChangesCtx {
	return ctx.Value(metadataChangesCtxKey{}).(*metadataChangesCtx)
}

type metadataChangesSteps struct{}

func (s metadataChangesSteps) givenLatestCursor(ctx context.Context, groups string) error {
	state := getMetadataChangesCtx(ctx)
	parsed, err := parseMetadataChangeGroups(groups)
	if err != nil {
		return err
	}
	state.groups = parsed
	changes, err := s.read(ctx, iggcon.LatestMetadataChangeStart())
	if err != nil {
		return fmt.Errorf("read the latest cursor: %w", err)
	}
	if changes.Status != iggcon.MetadataChangeStatusOk || changes.Next == nil {
		return fmt.Errorf("latest answered %s with cursor %v", changes.Status, changes.Next)
	}
	state.cursor = changes.Next
	return nil
}

func (metadataChangesSteps) whenDeleteStream(ctx context.Context) error {
	options := getResourceOptionsCtx(ctx)
	if err := options.client.DeleteStream(ctx, streamIdentifier(options)); err != nil {
		return fmt.Errorf("delete stream: %w", err)
	}
	options.createdStreams = slices.DeleteFunc(options.createdStreams, func(id uint32) bool { return id == *options.streamId })
	return nil
}

func (metadataChangesSteps) whenChurnStreams(ctx context.Context, count int, prefix string) error {
	client := getResourceOptionsCtx(ctx).client
	for index := range count {
		stream, err := client.CreateStream(ctx, fmt.Sprintf("%s-%d", prefix, index), nil)
		if err != nil {
			return fmt.Errorf("create stream %d: %w", index, err)
		}
		streamId, _ := iggcon.NewIdentifier(stream.Id)
		if err := client.DeleteStream(ctx, streamId); err != nil {
			return fmt.Errorf("delete stream %d: %w", index, err)
		}
	}
	return nil
}

func (s metadataChangesSteps) thenFollowedKinds(ctx context.Context, kinds string) error {
	state := getMetadataChangesCtx(ctx)
	streamKey, err := s.createdStreamKey(ctx)
	if err != nil {
		return err
	}
	var want []iggcon.MetadataChangeKind
	for _, name := range strings.Split(kinds, ",") {
		kind, err := iggcon.ParseMetadataChangeKind(strings.TrimSpace(name))
		if err != nil {
			return err
		}
		want = append(want, kind)
	}
	cursor := *state.cursor
	var records []iggcon.MetadataChangeRecord
	var mine []iggcon.MetadataChangeRecord
	for range metadataChangesFollowAttempts {
		changes, err := s.read(ctx, iggcon.AfterMetadataChangeStart(cursor))
		if err != nil {
			return fmt.Errorf("follow changes: %w", err)
		}
		if changes.Status != iggcon.MetadataChangeStatusOk || changes.Next == nil {
			return fmt.Errorf("follow answered %s with cursor %v", changes.Status, changes.Next)
		}
		cursor = *changes.Next
		records = append(records, changes.Records...)
		if changes.HasMore {
			continue
		}
		mine = slices.DeleteFunc(slices.Clone(records), func(record iggcon.MetadataChangeRecord) bool {
			key, ok := changeStreamKey(record)
			return !ok || key != streamKey
		})
		if len(mine) >= len(want) {
			break
		}
		time.Sleep(metadataChangesFollowInterval)
	}
	for _, record := range records {
		if len(state.groups) > 0 && !slices.Contains(state.groups, record.Group) {
			return fmt.Errorf("record of group %s outside the requested %v", record.Group, state.groups)
		}
	}
	got := make([]iggcon.MetadataChangeKind, 0, len(mine))
	for _, record := range mine {
		got = append(got, record.Kind)
	}
	if !slices.Equal(got, want) {
		return fmt.Errorf("followed kinds %v, want %v", got, want)
	}
	state.followed = mine
	return nil
}

func (metadataChangesSteps) thenRootActor(ctx context.Context) error {
	for _, record := range getMetadataChangesCtx(ctx).followed {
		if record.ActorUserID == nil || *record.ActorUserID != rootUserID {
			return fmt.Errorf("op %d names actor %v, want the root user", record.Op, record.ActorUserID)
		}
	}
	return nil
}

func (s metadataChangesSteps) thenRequestedOpBeforeOwnOp(ctx context.Context, kind string) error {
	record, err := s.followedRecord(ctx, kind)
	if err != nil {
		return err
	}
	deleted, ok := record.Body.(*iggcon.MetadataStreamDeleted)
	if !ok {
		return fmt.Errorf("expected a stream deletion, got %T", record.Body)
	}
	if deleted.RequestedOp == nil || *deleted.RequestedOp >= record.Op {
		return fmt.Errorf("requested op %v is not before op %d", deleted.RequestedOp, record.Op)
	}
	return nil
}

func (s metadataChangesSteps) thenChangeKeySource(ctx context.Context, kind, source string) error {
	stream, err := s.followedStreamOptions(ctx, kind)
	if err != nil {
		return err
	}
	if got, ok := stream.EncryptionKeySource(); !ok || got != iggcon.StreamEncryptionSource(source) {
		return fmt.Errorf("encryption key source is %q (reported %t), want %q", got, ok, source)
	}
	return nil
}

func (s metadataChangesSteps) thenChangeFingerprint(ctx context.Context, kind string) error {
	stream, err := s.followedStreamOptions(ctx, kind)
	if err != nil {
		return err
	}
	want, err := iggcon.EncryptionKeyFingerprint(getResourceOptionsCtx(ctx).streamKey)
	if err != nil {
		return fmt.Errorf("fingerprint the stream key: %w", err)
	}
	if got, ok := stream.EncryptionKeyFingerprint(); !ok || got != want {
		return fmt.Errorf("encryption key fingerprint is %q (reported %t), want %q", got, ok, want)
	}
	return nil
}

func (metadataChangesSteps) thenChangesHideKeys(ctx context.Context) error {
	key := getResourceOptionsCtx(ctx).streamKey
	for _, record := range getMetadataChangesCtx(ctx).followed {
		stream, ok := record.Body.(*iggcon.MetadataStreamChange)
		if !ok {
			continue
		}
		if stream.Options.Unreadable {
			return fmt.Errorf("op %d options must stay readable", record.Op)
		}
		for _, options := range []map[string]iggcon.HeaderValue{stream.Options.Options, stream.Options.DerivedOptions} {
			for _, hidden := range []string{string(iggcon.StreamOptionEncryptionKey), "encryption_key_wrapped", "critical_keys"} {
				if _, found := options[hidden]; found {
					return fmt.Errorf("op %d returns %s", record.Op, hidden)
				}
			}
			for name, value := range options {
				if len(key) > 0 && (bytes.Contains(value.Value, key) ||
					bytes.Contains(value.Value, []byte(base64.StdEncoding.EncodeToString(key))) ||
					bytes.Contains(value.Value, []byte(hex.EncodeToString(key)))) {
					return fmt.Errorf("op %d option %s carries the stream key", record.Op, name)
				}
			}
		}
	}
	return nil
}

func (s metadataChangesSteps) thenFollowStatus(ctx context.Context, status string) error {
	state := getMetadataChangesCtx(ctx)
	changes, err := s.read(ctx, iggcon.AfterMetadataChangeStart(*state.cursor))
	if err != nil {
		return fmt.Errorf("follow changes: %w", err)
	}
	if changes.Status.String() != status {
		return fmt.Errorf("follow answered %s, want %s", changes.Status, status)
	}
	if changes.Status != iggcon.MetadataChangeStatusOk && (len(changes.Records) != 0 || changes.Next != nil) {
		return fmt.Errorf("a %s answer carries %d records and cursor %v", changes.Status, len(changes.Records), changes.Next)
	}
	return nil
}

func (s metadataChangesSteps) whenReadCurrentState(ctx context.Context, groups string) error {
	state := getMetadataChangesCtx(ctx)
	parsed, err := parseMetadataChangeGroups(groups)
	if err != nil {
		return err
	}
	state.groups = parsed
	state.currentItems = nil
	var token *iggcon.MetadataCurrentPageToken
	for {
		changes, err := s.read(ctx, iggcon.CurrentMetadataChangeStart(token))
		if err != nil {
			return fmt.Errorf("read current state: %w", err)
		}
		if changes.Status != iggcon.MetadataChangeStatusOk {
			return fmt.Errorf("current state answered %s", changes.Status)
		}
		state.currentItems = append(state.currentItems, changes.Items...)
		if changes.CurrentPageToken == nil {
			if changes.Next == nil {
				return errors.New("the last current state page returned no anchor")
			}
			state.cursor = changes.Next
			return nil
		}
		token = changes.CurrentPageToken
	}
}

func (metadataChangesSteps) thenCurrentMatchesList(ctx context.Context) error {
	listed, err := getResourceOptionsCtx(ctx).client.GetStreams(ctx)
	if err != nil {
		return fmt.Errorf("list streams: %w", err)
	}
	want := make(map[uint32]string, len(listed))
	for _, stream := range listed {
		want[stream.Id] = stream.Name
	}
	got := make(map[uint32]string)
	for _, item := range getMetadataChangesCtx(ctx).currentItems {
		if stream, ok := item.Body.(*iggcon.MetadataStreamChange); ok {
			got[stream.StreamID] = stream.Name
		}
	}
	if !maps.Equal(got, want) {
		return fmt.Errorf("current state lists %v, the stream list %v", got, want)
	}
	return nil
}

func (metadataChangesSteps) givenUserWithoutServerPermissions(ctx context.Context, username, password string) error {
	options := getResourceOptionsCtx(ctx)
	permissions := &iggcon.Permissions{Global: iggcon.GlobalPermissions{ReadServers: true}}
	user, err := options.client.CreateUser(ctx, username, password, iggcon.Active, permissions, nil)
	if err != nil {
		return fmt.Errorf("create user %s: %w", username, err)
	}
	options.userId = &user.Id
	options.createdUsers = append(options.createdUsers, user.Id)
	return nil
}

func (metadataChangesSteps) whenAuthenticateAs(ctx context.Context, username, password string) error {
	client := getResourceOptionsCtx(ctx).client
	if err := client.LogoutUser(ctx); err != nil {
		return fmt.Errorf("logout: %w", err)
	}
	getMetadataChangesCtx(ctx).signedInAsNew = true
	if _, err := client.LoginUser(ctx, username, password); err != nil {
		return fmt.Errorf("login as %s: %w", username, err)
	}
	return nil
}

func (s metadataChangesSteps) thenChangesRefused(ctx context.Context) error {
	getMetadataChangesCtx(ctx).groups = nil
	_, err := s.read(ctx, iggcon.EarliestMetadataChangeStart())
	if !errors.Is(err, ierror.ErrUnauthorized) {
		return fmt.Errorf("reading changes returned %v, want unauthorized", err)
	}
	return nil
}

func (metadataChangesSteps) read(ctx context.Context, start iggcon.MetadataChangeStart) (*iggcon.MetadataChanges, error) {
	query := iggcon.NewMetadataChangesQuery(start, getMetadataChangesCtx(ctx).groups...)
	query.Limit = iggcon.MaxMetadataChangesLimit
	return getResourceOptionsCtx(ctx).client.GetMetadataChanges(ctx, query)
}

// createdStreamKey reads the key of the scenario's stream from its creation
// record. Ids are reused after a delete, so a stream id alone can match a
// stream another test created and deleted meanwhile.
func (metadataChangesSteps) createdStreamKey(ctx context.Context) (iggcon.MetadataStreamKey, error) {
	options := getResourceOptionsCtx(ctx)
	query := iggcon.NewMetadataChangesQuery(
		iggcon.AfterMetadataChangeStart(*getMetadataChangesCtx(ctx).cursor),
		iggcon.MetadataChangeGroupStreams,
	)
	query.Limit = iggcon.MaxMetadataChangesLimit
	for {
		changes, err := options.client.GetMetadataChanges(ctx, query)
		if err != nil {
			return iggcon.MetadataStreamKey{}, fmt.Errorf("read stream changes: %w", err)
		}
		if changes.Status != iggcon.MetadataChangeStatusOk || changes.Next == nil {
			return iggcon.MetadataStreamKey{}, fmt.Errorf("stream changes answered %s with cursor %v", changes.Status, changes.Next)
		}
		for _, record := range changes.Records {
			created, ok := record.Body.(*iggcon.MetadataStreamChange)
			if ok && record.Kind == iggcon.MetadataChangeKindStreamCreated &&
				created.StreamID == *options.streamId && created.Name == options.streamName {
				return created.MetadataStreamKey, nil
			}
		}
		if !changes.HasMore {
			return iggcon.MetadataStreamKey{}, fmt.Errorf("no stream_created change for stream %d %q", *options.streamId, options.streamName)
		}
		query.Start = iggcon.AfterMetadataChangeStart(*changes.Next)
	}
}

func (metadataChangesSteps) followedRecord(ctx context.Context, kind string) (iggcon.MetadataChangeRecord, error) {
	want, err := iggcon.ParseMetadataChangeKind(kind)
	if err != nil {
		return iggcon.MetadataChangeRecord{}, err
	}
	for _, record := range getMetadataChangesCtx(ctx).followed {
		if record.Kind == want {
			return record, nil
		}
	}
	return iggcon.MetadataChangeRecord{}, fmt.Errorf("no followed %s change", kind)
}

// followedStreamOptions views a followed stream change's options as a stream,
// so the stream's projection helpers read them.
func (s metadataChangesSteps) followedStreamOptions(ctx context.Context, kind string) (iggcon.Stream, error) {
	record, err := s.followedRecord(ctx, kind)
	if err != nil {
		return iggcon.Stream{}, err
	}
	stream, ok := record.Body.(*iggcon.MetadataStreamChange)
	if !ok {
		return iggcon.Stream{}, fmt.Errorf("expected a stream change, got %T", record.Body)
	}
	if stream.Options.Unreadable {
		return iggcon.Stream{}, errors.New("stream options must stay readable")
	}
	return iggcon.Stream{Options: stream.Options.Options, DerivedOptions: stream.Options.DerivedOptions}, nil
}

func initMetadataChangesScenario(sc *godog.ScenarioContext) {
	sc.Before(func(context.Context, *godog.Scenario) (context.Context, error) {
		ctx := context.WithValue(context.Background(), resourceOptionsCtxKey{}, &resourceOptionsCtx{})
		return context.WithValue(ctx, metadataChangesCtxKey{}, &metadataChangesCtx{}), nil
	})
	options := resourceOptionsSteps{}
	s := metadataChangesSteps{}
	sc.Step(`^I have a running Iggy server$`, options.givenRunningServer)
	sc.Step(`^I am authenticated as the root user$`, options.givenAuthenticationAsRoot)
	sc.Step(`^I create a stream named "([^"]+)" without an encryption key$`, options.whenCreateKeylessStream)
	sc.Step(`^I create a stream named "([^"]+)" with an encryption key of 32 bytes valued (\d+)$`, options.whenCreateKeyedStream)
	sc.Step(`^I rename the stream to "([^"]+)"$`, options.whenRenameStream)
	sc.Step(`^I create a topic named "([^"]+)" with (\d+) partitions? in the stream$`, options.whenCreateTopic)
	sc.Step(`^I hold the latest metadata change cursor for the groups "([^"]+)"$`, s.givenLatestCursor)
	sc.Step(`^I delete the stream$`, s.whenDeleteStream)
	sc.Step(`^I create and delete (\d+) streams named "([^"]+)"$`, s.whenChurnStreams)
	sc.Step(`^following the metadata changes should return the kinds "([^"]+)" for the stream$`, s.thenFollowedKinds)
	sc.Step(`^each followed change should name the root user as its actor$`, s.thenRootActor)
	sc.Step(`^the followed "([^"]+)" change should carry a requested op before its own op$`, s.thenRequestedOpBeforeOwnOp)
	sc.Step(`^the followed "([^"]+)" change should report encryption key source "([^"]+)"$`, s.thenChangeKeySource)
	sc.Step(`^the followed "([^"]+)" change should carry the stream key fingerprint$`, s.thenChangeFingerprint)
	sc.Step(`^the followed changes should expose no encryption key material$`, s.thenChangesHideKeys)
	sc.Step(`^following the metadata changes should answer "([^"]+)"$`, s.thenFollowStatus)
	sc.Step(`^I read the current state for the groups "([^"]+)"$`, s.whenReadCurrentState)
	sc.Step(`^the current state should list the same streams as the stream list$`, s.thenCurrentMatchesList)
	sc.Step(`^a user named "([^"]+)" with password "([^"]+)" and no server permissions exists$`, s.givenUserWithoutServerPermissions)
	sc.Step(`^I authenticate as "([^"]+)" with password "([^"]+)"$`, s.whenAuthenticateAs)
	sc.Step(`^reading the metadata changes should be refused as unauthorized$`, s.thenChangesRefused)
	sc.After(func(ctx context.Context, scenario *godog.Scenario, scenarioError error) (context.Context, error) {
		options := getResourceOptionsCtx(ctx)
		if options.client != nil && getMetadataChangesCtx(ctx).signedInAsNew {
			username, password := env.RootCredentials()
			logoutError := options.client.LogoutUser(ctx)
			_, loginError := options.client.LoginUser(ctx, username, password)
			scenarioError = errors.Join(scenarioError, logoutError, loginError)
		}
		return resourceOptionsCleanup(ctx, scenario, scenarioError)
	})
}

func parseMetadataChangeGroups(names string) ([]iggcon.MetadataChangeGroup, error) {
	var groups []iggcon.MetadataChangeGroup
	for _, name := range strings.Split(names, ",") {
		group, err := iggcon.ParseMetadataChangeGroup(strings.TrimSpace(name))
		if err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	return groups, nil
}

// changeStreamKey names the stream a stream, topic or partitions record
// belongs to.
func changeStreamKey(record iggcon.MetadataChangeRecord) (iggcon.MetadataStreamKey, bool) {
	switch body := record.Body.(type) {
	case *iggcon.MetadataStreamChange:
		return body.MetadataStreamKey, true
	case *iggcon.MetadataStreamDeleted:
		return body.MetadataStreamKey, true
	case *iggcon.MetadataTopicChange:
		return body.MetadataStreamKey, true
	case *iggcon.MetadataTopicDeleted:
		return body.MetadataStreamKey, true
	case *iggcon.MetadataPartitionsChanged:
		return body.MetadataStreamKey, true
	default:
		return iggcon.MetadataStreamKey{}, false
	}
}
