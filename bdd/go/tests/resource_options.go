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
	"slices"

	"github.com/apache/iggy/bdd/go/tests/env"
	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/cucumber/godog"
)

// resourceOptionsPartition is the partition every message of these scenarios
// goes to and is polled from.
const resourceOptionsPartition = 0

type resourceOptionsCtxKey struct{}

type resourceOptionsCtx struct {
	client         iggcon.Client
	streamId       *uint32
	streamKey      []byte
	topicId        *uint32
	userId         *uint32
	createdStreams []uint32
	createdUsers   []uint32
	updateError    error
	optionSpecs    []iggcon.OptionSpec
}

func getResourceOptionsCtx(ctx context.Context) *resourceOptionsCtx {
	return ctx.Value(resourceOptionsCtxKey{}).(*resourceOptionsCtx)
}

type resourceOptionsSteps struct{}

func (resourceOptionsSteps) givenRunningServer(context.Context) error {
	return nil
}

func (resourceOptionsSteps) givenAuthenticationAsRoot(ctx context.Context) error {
	state := getResourceOptionsCtx(ctx)
	iggyClient, err := client.NewIggyClient(client.WithTcp(tcp.WithServerAddress(env.ServerAddress())))
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	if err = iggyClient.Connect(ctx); err != nil {
		return fmt.Errorf("connect client: %w", err)
	}
	username, password := env.RootCredentials()
	if _, err = iggyClient.LoginUser(ctx, username, password); err != nil {
		return fmt.Errorf("authenticate client: %w", err)
	}
	state.client = iggyClient
	return nil
}

func (s resourceOptionsSteps) whenCreateKeyedStream(ctx context.Context, name string, value int) error {
	state := getResourceOptionsCtx(ctx)
	key := bytes.Repeat([]byte{byte(value)}, iggcon.StreamEncryptionKeyLength)
	if err := s.createStream(ctx, name, &iggcon.StreamOptions{EncryptionKey: key}); err != nil {
		return err
	}
	state.streamKey = key
	return nil
}

func (s resourceOptionsSteps) whenCreateKeylessStream(ctx context.Context, name string) error {
	getResourceOptionsCtx(ctx).streamKey = nil
	return s.createStream(ctx, name, nil)
}

func (resourceOptionsSteps) createStream(ctx context.Context, name string, options *iggcon.StreamOptions) error {
	state := getResourceOptionsCtx(ctx)
	stream, err := state.client.CreateStream(ctx, name, options)
	if err != nil {
		return fmt.Errorf("create stream %s: %w", name, err)
	}
	state.streamId = &stream.Id
	state.createdStreams = append(state.createdStreams, stream.Id)
	return nil
}

func (s resourceOptionsSteps) thenStreamSource(ctx context.Context, want string) error {
	stream, err := s.fetchStream(ctx)
	if err != nil {
		return err
	}
	source, ok := stream.EncryptionKeySource()
	if !ok || source != iggcon.StreamEncryptionSource(want) {
		return fmt.Errorf("encryption key source is %q (reported %t), want %q", source, ok, want)
	}
	return nil
}

func (s resourceOptionsSteps) thenFingerprintMatchesKey(ctx context.Context) error {
	stream, err := s.fetchStream(ctx)
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

func (s resourceOptionsSteps) thenStreamReportsFingerprint(ctx context.Context) error {
	stream, err := s.fetchStream(ctx)
	if err != nil {
		return err
	}
	if fingerprint, ok := stream.EncryptionKeyFingerprint(); !ok || len(fingerprint) != 16 {
		return fmt.Errorf("encryption key fingerprint is %q (reported %t), want 16 hex characters", fingerprint, ok)
	}
	return nil
}

func (s resourceOptionsSteps) thenStreamReportsKek(ctx context.Context) error {
	stream, err := s.fetchStream(ctx)
	if err != nil {
		return err
	}
	if len(stream.EncryptionKekFingerprints()) == 0 {
		return errors.New("the stream reports no KEK fingerprint")
	}
	return nil
}

func (s resourceOptionsSteps) thenNoKeyMaterial(ctx context.Context) error {
	stream, err := s.fetchStream(ctx)
	if err != nil {
		return err
	}
	key := getResourceOptionsCtx(ctx).streamKey
	for _, options := range []map[string]iggcon.HeaderValue{stream.Options, stream.DerivedOptions} {
		for _, secret := range []string{string(iggcon.StreamOptionEncryptionKey), "encryption_key_wrapped", "critical_keys"} {
			if _, found := options[secret]; found {
				return fmt.Errorf("%s must never be returned", secret)
			}
		}
		if key == nil {
			continue
		}
		for name, value := range options {
			if bytes.Contains(value.Value, key) ||
				bytes.Contains(value.Value, []byte(base64.StdEncoding.EncodeToString(key))) ||
				bytes.Contains(value.Value, []byte(hex.EncodeToString(key))) {
				return fmt.Errorf("option %s carries the stream key", name)
			}
		}
	}
	return nil
}

func (resourceOptionsSteps) whenCreateTopic(ctx context.Context, name string, partitions uint32) error {
	state := getResourceOptionsCtx(ctx)
	expiry := iggcon.IggyExpiryNeverExpire
	topic, err := state.client.CreateTopic(ctx, streamIdentifier(state), name,
		&iggcon.TopicOptions{PartitionsCount: &partitions, MessageExpiry: &expiry})
	if err != nil {
		return fmt.Errorf("create topic %s: %w", name, err)
	}
	state.topicId = &topic.Id
	return nil
}

func (s resourceOptionsSteps) whenSendPayload(ctx context.Context, payload string) error {
	return s.sendMessage(ctx, payload, nil)
}

func (s resourceOptionsSteps) whenSendPayloadWithHeader(ctx context.Context, payload, header, value string) error {
	return s.sendMessage(ctx, payload, []iggcon.HeaderEntry{{
		Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte(header)},
		Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte(value)},
	}})
}

func (resourceOptionsSteps) sendMessage(ctx context.Context, payload string, headers []iggcon.HeaderEntry) error {
	state := getResourceOptionsCtx(ctx)
	var options []iggcon.IggyMessageOpt
	if len(headers) > 0 {
		options = append(options, iggcon.WithUserHeaders(headers))
	}
	message, err := iggcon.NewIggyMessage([]byte(payload), options...)
	if err != nil {
		return fmt.Errorf("build message: %w", err)
	}
	_, err = state.client.SendMessages(ctx, streamIdentifier(state), topicIdentifier(state),
		iggcon.PartitionId(resourceOptionsPartition), []iggcon.IggyMessage{message})
	if err != nil {
		return fmt.Errorf("send message: %w", err)
	}
	return nil
}

func (s resourceOptionsSteps) thenPollReturnsPayload(ctx context.Context, payload string) error {
	message, err := s.pollFirst(ctx)
	if err != nil {
		return err
	}
	if string(message.Payload) != payload {
		return fmt.Errorf("polled payload %q, want %q", message.Payload, payload)
	}
	return nil
}

func (s resourceOptionsSteps) thenPollReturnsPayloadAndHeader(ctx context.Context, payload, header, value string) error {
	message, err := s.pollFirst(ctx)
	if err != nil {
		return err
	}
	if string(message.Payload) != payload {
		return fmt.Errorf("polled payload %q, want %q", message.Payload, payload)
	}
	headers, err := iggcon.DeserializeHeaders(message.UserHeaders)
	if err != nil {
		return fmt.Errorf("read user headers: %w", err)
	}
	want := iggcon.HeaderEntry{
		Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte(header)},
		Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte(value)},
	}
	if !slices.ContainsFunc(headers, func(entry iggcon.HeaderEntry) bool {
		return entry.Key.Kind == want.Key.Kind && bytes.Equal(entry.Key.Value, want.Key.Value) &&
			entry.Value.Kind == want.Value.Kind && bytes.Equal(entry.Value.Value, want.Value.Value)
	}) {
		return fmt.Errorf("polled headers %v do not carry %s=%s", headers, header, value)
	}
	return nil
}

func (resourceOptionsSteps) pollFirst(ctx context.Context) (iggcon.IggyMessage, error) {
	state := getResourceOptionsCtx(ctx)
	partition := uint32(resourceOptionsPartition)
	polled, err := state.client.PollMessages(ctx, streamIdentifier(state), topicIdentifier(state),
		iggcon.DefaultConsumer(), iggcon.OffsetPollingStrategy(0), 1, false, &partition)
	if err != nil {
		return iggcon.IggyMessage{}, fmt.Errorf("poll messages: %w", err)
	}
	if len(polled.Messages) == 0 {
		return iggcon.IggyMessage{}, errors.New("the poll returned no message")
	}
	return polled.Messages[0], nil
}

func (resourceOptionsSteps) whenRenameStream(ctx context.Context, name string) error {
	state := getResourceOptionsCtx(ctx)
	if err := state.client.UpdateStream(ctx, streamIdentifier(state), iggcon.StreamUpdate{Name: &name}); err != nil {
		return fmt.Errorf("rename stream: %w", err)
	}
	return nil
}

func (s resourceOptionsSteps) thenStreamName(ctx context.Context, name string) error {
	stream, err := s.fetchStream(ctx)
	if err != nil {
		return err
	}
	if stream.Name != name {
		return fmt.Errorf("stream name is %q, want %q", stream.Name, name)
	}
	return nil
}

func (resourceOptionsSteps) whenResetStreamOption(ctx context.Context, key string) error {
	state := getResourceOptionsCtx(ctx)
	state.updateError = state.client.UpdateStream(ctx, streamIdentifier(state),
		iggcon.StreamUpdate{Reset: []iggcon.StreamOptionKey{iggcon.StreamOptionKey(key)}})
	return nil
}

func (resourceOptionsSteps) thenStreamUpdateRejected(ctx context.Context) error {
	if getResourceOptionsCtx(ctx).updateError == nil {
		return errors.New("the update must fail")
	}
	return nil
}

func (resourceOptionsSteps) whenDescribeStreamOptions(ctx context.Context) error {
	state := getResourceOptionsCtx(ctx)
	specs, err := state.client.DescribeOptions(ctx, iggcon.OptionsScopeStream)
	if err != nil {
		return fmt.Errorf("describe options: %w", err)
	}
	state.optionSpecs = specs
	return nil
}

func (s resourceOptionsSteps) thenOptionUpdatable(ctx context.Context, key string) error {
	spec, err := s.optionSpec(ctx, key)
	if err != nil {
		return err
	}
	if !spec.Updatable || spec.Secret || spec.ReadOnly {
		return fmt.Errorf("%s is %+v, want updatable and not secret", key, spec)
	}
	return nil
}

func (s resourceOptionsSteps) thenOptionCreateOnlySecret(ctx context.Context, key string) error {
	spec, err := s.optionSpec(ctx, key)
	if err != nil {
		return err
	}
	if spec.Updatable || spec.Resettable || !spec.Secret || spec.ReadOnly {
		return fmt.Errorf("%s is %+v, want create only and secret", key, spec)
	}
	return nil
}

func (s resourceOptionsSteps) thenOptionReadOnly(ctx context.Context, key string) error {
	spec, err := s.optionSpec(ctx, key)
	if err != nil {
		return err
	}
	if !spec.ReadOnly || spec.Updatable || spec.Secret {
		return fmt.Errorf("%s is %+v, want read only", key, spec)
	}
	return nil
}

func (resourceOptionsSteps) optionSpec(ctx context.Context, key string) (iggcon.OptionSpec, error) {
	for _, spec := range getResourceOptionsCtx(ctx).optionSpecs {
		if spec.Key == key {
			return spec, nil
		}
	}
	return iggcon.OptionSpec{}, fmt.Errorf("the catalog does not describe %s", key)
}

func (resourceOptionsSteps) givenTopicWithSettings(ctx context.Context, name string, expiry, maxTopicSize uint64) error {
	state := getResourceOptionsCtx(ctx)
	partitions := uint32(1)
	messageExpiry := iggcon.Duration(expiry)
	topic, err := state.client.CreateTopic(ctx, streamIdentifier(state), name, &iggcon.TopicOptions{
		PartitionsCount: &partitions,
		MessageExpiry:   &messageExpiry,
		MaxTopicSize:    &maxTopicSize,
	})
	if err != nil {
		return fmt.Errorf("create topic %s: %w", name, err)
	}
	state.topicId = &topic.Id
	return nil
}

func (s resourceOptionsSteps) whenUpdateTopicExpiry(ctx context.Context, expiry uint64) error {
	messageExpiry := iggcon.Duration(expiry)
	return s.updateTopic(ctx, iggcon.TopicUpdate{MessageExpiry: &messageExpiry})
}

func (s resourceOptionsSteps) whenResetTopicOption(ctx context.Context, key string) error {
	return s.updateTopic(ctx, iggcon.TopicUpdate{Reset: []iggcon.TopicOptionKey{iggcon.TopicOptionKey(key)}})
}

func (s resourceOptionsSteps) whenRenameTopic(ctx context.Context, name string) error {
	return s.updateTopic(ctx, iggcon.TopicUpdate{Name: &name})
}

func (resourceOptionsSteps) updateTopic(ctx context.Context, update iggcon.TopicUpdate) error {
	state := getResourceOptionsCtx(ctx)
	if err := state.client.UpdateTopic(ctx, streamIdentifier(state), topicIdentifier(state), update); err != nil {
		return fmt.Errorf("update topic: %w", err)
	}
	return nil
}

func (s resourceOptionsSteps) thenTopicName(ctx context.Context, name string) error {
	topic, err := s.fetchTopic(ctx)
	if err != nil {
		return err
	}
	if topic.Name != name {
		return fmt.Errorf("topic name is %q, want %q", topic.Name, name)
	}
	return nil
}

func (s resourceOptionsSteps) thenTopicExpiryExplicit(ctx context.Context, expiry uint64) error {
	topic, err := s.fetchTopic(ctx)
	if err != nil {
		return err
	}
	if uint64(topic.MessageExpiry) != expiry {
		return fmt.Errorf("message expiry is %d, want %d", topic.MessageExpiry, expiry)
	}
	if _, explicit := topic.Options[string(iggcon.TopicOptionMessageExpiry)]; !explicit {
		return errors.New("message expiry must be set explicitly")
	}
	return nil
}

func (s resourceOptionsSteps) thenTopicMaxSizeExplicit(ctx context.Context, maxTopicSize uint64) error {
	topic, err := s.fetchTopic(ctx)
	if err != nil {
		return err
	}
	if topic.MaxTopicSize != maxTopicSize {
		return fmt.Errorf("max topic size is %d, want %d", topic.MaxTopicSize, maxTopicSize)
	}
	if _, explicit := topic.Options[string(iggcon.TopicOptionMaxTopicSize)]; !explicit {
		return errors.New("max topic size must be set explicitly")
	}
	return nil
}

func (s resourceOptionsSteps) thenTopicMaxSizeDerived(ctx context.Context) error {
	topic, err := s.fetchTopic(ctx)
	if err != nil {
		return err
	}
	if _, explicit := topic.Options[string(iggcon.TopicOptionMaxTopicSize)]; explicit {
		return errors.New("max topic size must no longer be explicit")
	}
	if _, derived := topic.DerivedOptions[string(iggcon.TopicOptionMaxTopicSize)]; !derived {
		return errors.New("max topic size must be derived from the server default")
	}
	return nil
}

func (resourceOptionsSteps) givenUser(ctx context.Context, username, password string) error {
	state := getResourceOptionsCtx(ctx)
	user, err := state.client.CreateUser(ctx, username, password, iggcon.Active, nil, &iggcon.UserOptions{})
	if err != nil {
		return fmt.Errorf("create user %s: %w", username, err)
	}
	state.userId = &user.Id
	state.createdUsers = append(state.createdUsers, user.Id)
	return nil
}

func (resourceOptionsSteps) whenUpdateUser(ctx context.Context, username, status string) error {
	state := getResourceOptionsCtx(ctx)
	userStatus, err := parseUserStatus(status)
	if err != nil {
		return err
	}
	update := iggcon.UserUpdate{Username: &username, Status: &userStatus}
	if err = state.client.UpdateUser(ctx, userIdentifier(state), update); err != nil {
		return fmt.Errorf("update user: %w", err)
	}
	return nil
}

func (s resourceOptionsSteps) thenUserUsername(ctx context.Context, username string) error {
	user, err := s.fetchUser(ctx)
	if err != nil {
		return err
	}
	if user.Username != username {
		return fmt.Errorf("username is %q, want %q", user.Username, username)
	}
	return nil
}

func (s resourceOptionsSteps) thenUserStatus(ctx context.Context, status string) error {
	user, err := s.fetchUser(ctx)
	if err != nil {
		return err
	}
	want, err := parseUserStatus(status)
	if err != nil {
		return err
	}
	if user.Status != want {
		return fmt.Errorf("user status is %d, want %s", user.Status, status)
	}
	return nil
}

func (resourceOptionsSteps) fetchStream(ctx context.Context) (*iggcon.StreamDetails, error) {
	state := getResourceOptionsCtx(ctx)
	stream, err := state.client.GetStream(ctx, streamIdentifier(state))
	if err != nil {
		return nil, fmt.Errorf("get stream: %w", err)
	}
	return stream, nil
}

func (resourceOptionsSteps) fetchTopic(ctx context.Context) (*iggcon.TopicDetails, error) {
	state := getResourceOptionsCtx(ctx)
	topic, err := state.client.GetTopic(ctx, streamIdentifier(state), topicIdentifier(state))
	if err != nil {
		return nil, fmt.Errorf("get topic: %w", err)
	}
	return topic, nil
}

func (resourceOptionsSteps) fetchUser(ctx context.Context) (*iggcon.UserInfoDetails, error) {
	state := getResourceOptionsCtx(ctx)
	user, err := state.client.GetUser(ctx, userIdentifier(state))
	if err != nil {
		return nil, fmt.Errorf("get user: %w", err)
	}
	return user, nil
}

func initResourceOptionsScenario(sc *godog.ScenarioContext) {
	sc.Before(func(context.Context, *godog.Scenario) (context.Context, error) {
		return context.WithValue(context.Background(), resourceOptionsCtxKey{}, &resourceOptionsCtx{}), nil
	})
	s := resourceOptionsSteps{}
	sc.Step(`^I have a running Iggy server$`, s.givenRunningServer)
	sc.Step(`^I am authenticated as the root user$`, s.givenAuthenticationAsRoot)
	sc.Step(`^I create a stream named "([^"]+)" with an encryption key of 32 bytes valued (\d+)$`, s.whenCreateKeyedStream)
	sc.Step(`^a stream named "([^"]+)" with an encryption key of 32 bytes valued (\d+) exists$`, s.whenCreateKeyedStream)
	sc.Step(`^I create a stream named "([^"]+)" without an encryption key$`, s.whenCreateKeylessStream)
	sc.Step(`^a stream named "([^"]+)" exists$`, s.whenCreateKeylessStream)
	sc.Step(`^the stream should report encryption key source "([^"]+)"$`, s.thenStreamSource)
	sc.Step(`^the stream encryption key fingerprint should match the key$`, s.thenFingerprintMatchesKey)
	sc.Step(`^the stream should report an encryption key fingerprint$`, s.thenStreamReportsFingerprint)
	sc.Step(`^the stream should report at least one KEK fingerprint$`, s.thenStreamReportsKek)
	sc.Step(`^the stream should expose no encryption key material$`, s.thenNoKeyMaterial)
	sc.Step(`^I create a topic named "([^"]+)" with (\d+) partitions? in the stream$`, s.whenCreateTopic)
	sc.Step(`^I send a message with payload "([^"]+)" to the topic$`, s.whenSendPayload)
	sc.Step(`^I send a message with payload "([^"]+)" and header "([^"]+)" set to "([^"]+)" to the topic$`, s.whenSendPayloadWithHeader)
	sc.Step(`^polling the topic should return the payload "([^"]+)"$`, s.thenPollReturnsPayload)
	sc.Step(`^polling the topic should return the payload "([^"]+)" and header "([^"]+)" set to "([^"]+)"$`, s.thenPollReturnsPayloadAndHeader)
	sc.Step(`^I rename the stream to "([^"]+)"$`, s.whenRenameStream)
	sc.Step(`^the stream name should be "([^"]+)"$`, s.thenStreamName)
	sc.Step(`^I request a reset of the stream option "([^"]+)"$`, s.whenResetStreamOption)
	sc.Step(`^the stream update should be rejected$`, s.thenStreamUpdateRejected)
	sc.Step(`^I describe the stream options$`, s.whenDescribeStreamOptions)
	sc.Step(`^the stream option "([^"]+)" should be updatable and not secret$`, s.thenOptionUpdatable)
	sc.Step(`^the stream option "([^"]+)" should be create only and secret$`, s.thenOptionCreateOnlySecret)
	sc.Step(`^the stream option "([^"]+)" should be read only$`, s.thenOptionReadOnly)
	sc.Step(`^a topic named "([^"]+)" exists in the stream with message expiry (\d+) microseconds and max topic size (\d+) bytes$`, s.givenTopicWithSettings)
	sc.Step(`^I update the topic message expiry to (\d+) microseconds without a name$`, s.whenUpdateTopicExpiry)
	sc.Step(`^I reset the topic option "([^"]+)"$`, s.whenResetTopicOption)
	sc.Step(`^I rename the topic to "([^"]+)"$`, s.whenRenameTopic)
	sc.Step(`^the topic name should be "([^"]+)"$`, s.thenTopicName)
	sc.Step(`^the topic message expiry should be (\d+) microseconds set explicitly$`, s.thenTopicExpiryExplicit)
	sc.Step(`^the topic max topic size should be (\d+) bytes set explicitly$`, s.thenTopicMaxSizeExplicit)
	sc.Step(`^the topic max topic size should be derived from the server default$`, s.thenTopicMaxSizeDerived)
	sc.Step(`^a user named "([^"]+)" with password "([^"]+)" exists$`, s.givenUser)
	sc.Step(`^I update the user username to "([^"]+)" and status to "([^"]+)"$`, s.whenUpdateUser)
	sc.Step(`^the user should have username "([^"]+)"$`, s.thenUserUsername)
	sc.Step(`^the user should have status "([^"]+)"$`, s.thenUserStatus)
	sc.After(func(ctx context.Context, _ *godog.Scenario, scenarioError error) (context.Context, error) {
		state := getResourceOptionsCtx(ctx)
		if state.client == nil {
			return ctx, scenarioError
		}
		for _, id := range state.createdStreams {
			streamId, err := iggcon.NewIdentifier(id)
			scenarioError = errors.Join(scenarioError, err, state.client.DeleteStream(ctx, streamId))
		}
		for _, id := range state.createdUsers {
			userId, err := iggcon.NewIdentifier(id)
			scenarioError = errors.Join(scenarioError, err, state.client.DeleteUser(ctx, userId))
		}
		return ctx, errors.Join(scenarioError, state.client.Close())
	})
}

func streamIdentifier(state *resourceOptionsCtx) iggcon.Identifier {
	identifier, _ := iggcon.NewIdentifier(*state.streamId)
	return identifier
}

func topicIdentifier(state *resourceOptionsCtx) iggcon.Identifier {
	identifier, _ := iggcon.NewIdentifier(*state.topicId)
	return identifier
}

func userIdentifier(state *resourceOptionsCtx) iggcon.Identifier {
	identifier, _ := iggcon.NewIdentifier(*state.userId)
	return identifier
}

func parseUserStatus(status string) (iggcon.UserStatus, error) {
	switch status {
	case "active":
		return iggcon.Active, nil
	case "inactive":
		return iggcon.Inactive, nil
	default:
		return 0, fmt.Errorf("unknown user status %q", status)
	}
}
