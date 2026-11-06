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

package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/apache/iggy/foreign/go/client/reader"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// defaultResolveDeadline is the budget of a resolution that names none.
const defaultResolveDeadline = 30 * time.Second

type noArgs struct{}

type loginArgs struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type streamArgs struct {
	StreamID id `json:"stream_id"`
}

type createStreamArgs struct {
	Name             string  `json:"name"`
	EncryptionKeyHex *string `json:"encryption_key_hex"`
}

type updateStreamArgs struct {
	StreamID id       `json:"stream_id"`
	Name     *string  `json:"name"`
	Reset    []string `json:"reset"`
}

type topicArgs struct {
	StreamID id `json:"stream_id"`
	TopicID  id `json:"topic_id"`
}

type createTopicArgs struct {
	StreamID        id         `json:"stream_id"`
	Name            string     `json:"name"`
	PartitionsCount *uint32    `json:"partitions_count"`
	MessageExpiry   *expiry    `json:"message_expiry"`
	MaxTopicSize    *topicSize `json:"max_topic_size"`
}

type updateTopicArgs struct {
	StreamID      id         `json:"stream_id"`
	TopicID       id         `json:"topic_id"`
	Name          *string    `json:"name"`
	MessageExpiry *expiry    `json:"message_expiry"`
	MaxTopicSize  *topicSize `json:"max_topic_size"`
	Reset         []string   `json:"reset"`
}

type sendArgs struct {
	StreamID     id           `json:"stream_id"`
	TopicID      id           `json:"topic_id"`
	Partitioning partitioning `json:"partitioning"`
	Messages     []messageArg `json:"messages"`
}

type pollArgs struct {
	StreamID    id        `json:"stream_id"`
	TopicID     id        `json:"topic_id"`
	PartitionID uint32    `json:"partition_id"`
	Consumer    *consumer `json:"consumer"`
	Strategy    strategy  `json:"strategy"`
	Count       uint32    `json:"count"`
	Commit      string    `json:"commit"`
	MaxWaitMs   *uint64   `json:"max_wait_ms"`
	MinCount    *uint32   `json:"min_count"`
}

type createGroupArgs struct {
	StreamID id     `json:"stream_id"`
	TopicID  id     `json:"topic_id"`
	Name     string `json:"name"`
}

type groupArgs struct {
	StreamID id `json:"stream_id"`
	TopicID  id `json:"topic_id"`
	GroupID  id `json:"group_id"`
}

type buildReaderArgs struct {
	StreamID   id        `json:"stream_id"`
	TopicID    id        `json:"topic_id"`
	Consumer   *consumer `json:"consumer"`
	Partitions []uint32  `json:"partitions"`
	Start      *strategy `json:"start"`
	MaxWaitMs  *uint64   `json:"max_wait_ms"`
	Commit     string    `json:"commit"`
}

type buildConsumerArgs struct {
	StreamID  id      `json:"stream_id"`
	TopicID   id      `json:"topic_id"`
	Group     string  `json:"group"`
	MaxWaitMs *uint64 `json:"max_wait_ms"`
	Count     *uint32 `json:"count"`
	Commit    string  `json:"commit"`
}

type binaryRequestArgs struct {
	Code       uint32 `json:"code"`
	PayloadHex string `json:"payload_hex"`
}

type binaryRequestToReplicaArgs struct {
	Code       uint32 `json:"code"`
	PayloadHex string `json:"payload_hex"`
	Replica    uint8  `json:"replica"`
}

type describeOptionsArgs struct {
	Scope string `json:"scope"`
}

type globalPermissionsArg struct {
	ManageServers bool `json:"manage_servers"`
	ReadServers   bool `json:"read_servers"`
	ManageUsers   bool `json:"manage_users"`
	ReadUsers     bool `json:"read_users"`
	ManageStreams bool `json:"manage_streams"`
	ReadStreams   bool `json:"read_streams"`
	ManageTopics  bool `json:"manage_topics"`
	ReadTopics    bool `json:"read_topics"`
	PollMessages  bool `json:"poll_messages"`
	SendMessages  bool `json:"send_messages"`
}

type createUserArgs struct {
	Username    string  `json:"username"`
	Password    string  `json:"password"`
	Status      *string `json:"status"`
	Permissions *struct {
		Global globalPermissionsArg `json:"global"`
	} `json:"permissions"`
}

type userArgs struct {
	UserID id `json:"user_id"`
}

type updateUserArgs struct {
	UserID   id      `json:"user_id"`
	Username *string `json:"username"`
	Status   *string `json:"status"`
}

type metadataChangesArgs struct {
	Start  metadataStart `json:"start"`
	Groups []string      `json:"groups"`
	Limit  *uint32       `json:"limit"`
}

type resolveArgs struct {
	OperationID uint64  `json:"operation_id"`
	DeadlineMs  *uint64 `json:"deadline_ms"`
}

type abandonArgs struct {
	OperationID uint64 `json:"operation_id"`
}

// disconnecter is the SDK client that can drop its links and connect again.
type disconnecter interface{ Disconnect() error }

// disconnectable is the client, or the client that it wraps, as a
// disconnecter, or nil.
func disconnectable(client iggcon.Client) disconnecter {
	for {
		if dropping, ok := client.(disconnecter); ok {
			return dropping
		}
		wrapper, ok := client.(interface{ Unwrap() iggcon.Client })
		if !ok {
			return nil
		}
		client = wrapper.Unwrap()
	}
}

func (a *agent) call(ctx context.Context, entity, method string, raw json.RawMessage, saveAs string) reply {
	if method == "next" {
		if err := decode(raw, &noArgs{}); err != nil {
			return failure("arguments: %v", err)
		}
		if target := a.reader(entity); target != nil {
			target.mu.Lock()
			defer target.mu.Unlock()
			polled, err := target.reader.Next(ctx)
			if err != nil {
				return sdkError(err)
			}
			return converted(canonicalPolled(polled))
		}
		if target := a.consumer(entity); target != nil {
			target.mu.Lock()
			defer target.mu.Unlock()
			received, err := target.consumer.Next(ctx)
			if err != nil {
				return sdkError(err)
			}
			message, err := canonicalMessage(received.Message)
			if err != nil {
				return failure("result: %v", err)
			}
			return result(receivedMessageResult{PartitionID: received.PartitionID, Message: message})
		}
		return failure("no reader or consumer %q", entity)
	}
	cli := a.client(entity)
	if cli == nil {
		return failure("no client %q", entity)
	}
	switch method {
	case "connect":
		return noArgsCall(raw, func() error { return cli.Connect(ctx) })
	case "disconnect":
		dropping := disconnectable(cli)
		if dropping == nil {
			return unsupported("a client without Disconnect")
		}
		return noArgsCall(raw, dropping.Disconnect)
	case "ping":
		return noArgsCall(raw, func() error { return cli.Ping(ctx) })
	case "logout_user":
		return noArgsCall(raw, func() error { return cli.LogoutUser(ctx) })
	case "login_user":
		var args loginArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		identity, err := cli.LoginUser(ctx, args.Username, args.Password)
		if err != nil {
			return sdkError(err)
		}
		return result(identityResult{UserID: identity.UserId})
	case "get_stream":
		var args streamArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		stream, err := cli.GetStream(ctx, args.StreamID.identifier)
		if err != nil {
			return sdkError(err)
		}
		if stream == nil {
			return result(nil)
		}
		return converted(canonicalStreamDetails(stream))
	case "get_streams":
		if err := decode(raw, &noArgs{}); err != nil {
			return failure("arguments: %v", err)
		}
		streams, err := cli.GetStreams(ctx)
		if err != nil {
			return sdkError(err)
		}
		results := make([]streamResult, 0, len(streams))
		for _, stream := range streams {
			converted, err := canonicalStream(stream)
			if err != nil {
				return failure("result: %v", err)
			}
			results = append(results, converted)
		}
		return result(results)
	case "create_stream":
		var args createStreamArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		var options *iggcon.StreamOptions
		if args.EncryptionKeyHex != nil {
			key, err := hex.DecodeString(*args.EncryptionKeyHex)
			if err != nil {
				return failure("encryption_key_hex: %v", err)
			}
			options = &iggcon.StreamOptions{EncryptionKey: key}
		}
		stream, err := cli.CreateStream(ctx, args.Name, options)
		if err != nil {
			return sdkError(err)
		}
		return converted(canonicalStreamDetails(stream))
	case "update_stream":
		var args updateStreamArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		update := iggcon.StreamUpdate{Name: args.Name, RawReset: args.Reset}
		return nullOrError(cli.UpdateStream(ctx, args.StreamID.identifier, update))
	case "delete_stream":
		var args streamArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		return nullOrError(cli.DeleteStream(ctx, args.StreamID.identifier))
	case "get_topic":
		var args topicArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		topic, err := cli.GetTopic(ctx, args.StreamID.identifier, args.TopicID.identifier)
		if err != nil {
			return sdkError(err)
		}
		if topic == nil {
			return result(nil)
		}
		return converted(canonicalTopicDetails(topic))
	case "get_topics":
		var args streamArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		topics, err := cli.GetTopics(ctx, args.StreamID.identifier)
		if err != nil {
			return sdkError(err)
		}
		results := make([]topicResult, 0, len(topics))
		for _, topic := range topics {
			converted, err := canonicalTopic(topic)
			if err != nil {
				return failure("result: %v", err)
			}
			results = append(results, converted)
		}
		return result(results)
	case "create_topic":
		var args createTopicArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		options := &iggcon.TopicOptions{PartitionsCount: args.PartitionsCount}
		if args.MessageExpiry != nil {
			options.MessageExpiry = &args.MessageExpiry.value
		}
		if args.MaxTopicSize != nil {
			options.MaxTopicSize = &args.MaxTopicSize.value
		}
		topic, err := cli.CreateTopic(ctx, args.StreamID.identifier, args.Name, options)
		if err != nil {
			return sdkError(err)
		}
		return converted(canonicalTopicDetails(topic))
	case "update_topic":
		var args updateTopicArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		update := iggcon.TopicUpdate{Name: args.Name, RawReset: args.Reset}
		if args.MessageExpiry != nil {
			update.MessageExpiry = &args.MessageExpiry.value
		}
		if args.MaxTopicSize != nil {
			update.MaxTopicSize = &args.MaxTopicSize.value
		}
		return nullOrError(cli.UpdateTopic(ctx, args.StreamID.identifier, args.TopicID.identifier, update))
	case "delete_topic":
		var args topicArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		return nullOrError(cli.DeleteTopic(ctx, args.StreamID.identifier, args.TopicID.identifier))
	case "send_messages":
		var args sendArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		messages := make([]iggcon.IggyMessage, 0, len(args.Messages))
		for _, message := range args.Messages {
			built, err := message.message()
			if err != nil {
				return sdkError(err)
			}
			messages = append(messages, built)
		}
		_, err := cli.SendMessages(ctx, args.StreamID.identifier, args.TopicID.identifier, args.Partitioning.value, messages)
		return nullOrError(err)
	case "poll_messages":
		var args pollArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		options := iggcon.DefaultPollOptions()
		options.Count = args.Count
		options.MaxWait = milliseconds(args.MaxWaitMs)
		if args.MinCount != nil {
			options.MinCount = *args.MinCount
		}
		commit, err := commitPolicy(args.Commit)
		if err != nil {
			return failure("arguments: %v", err)
		}
		options.Commit = commit
		polled, err := cli.PollMessages(ctx, args.StreamID.identifier, args.TopicID.identifier,
			args.PartitionID, consumerOrDefault(args.Consumer), args.Strategy.value, options)
		if err != nil {
			return sdkError(err)
		}
		return converted(canonicalPolled(polled))
	case "create_consumer_group":
		var args createGroupArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		group, err := cli.CreateConsumerGroup(ctx, args.StreamID.identifier, args.TopicID.identifier, args.Name)
		if err != nil {
			return sdkError(err)
		}
		return result(canonicalGroup(group))
	case "get_consumer_group":
		var args groupArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		group, err := cli.GetConsumerGroup(ctx, args.StreamID.identifier, args.TopicID.identifier, args.GroupID.identifier)
		if err != nil {
			return sdkError(err)
		}
		if group == nil {
			return result(nil)
		}
		return result(canonicalGroup(group))
	case "delete_consumer_group":
		var args groupArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		return nullOrError(cli.DeleteConsumerGroup(ctx, args.StreamID.identifier, args.TopicID.identifier, args.GroupID.identifier))
	case "build_reader":
		var args buildReaderArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		if saveAs == "" {
			return failure("build_reader needs save_as")
		}
		commit, err := commitPolicy(args.Commit)
		if err != nil {
			return failure("arguments: %v", err)
		}
		options := iggcon.DefaultPollOptions()
		options.Commit = commit
		options.MaxWait = milliseconds(args.MaxWaitMs)
		readerOptions := []reader.Option{reader.WithPollOptions(options)}
		if args.Partitions != nil {
			readerOptions = append(readerOptions, reader.WithPartitions(args.Partitions...))
		}
		if args.Start != nil {
			readerOptions = append(readerOptions, reader.WithStart(args.Start.value))
		}
		built, err := reader.NewPartitionReader(ctx, cli, args.StreamID.identifier, args.TopicID.identifier,
			consumerOrDefault(args.Consumer), readerOptions...)
		if err != nil {
			return sdkError(err)
		}
		a.keepReader(saveAs, built)
		return result(nil)
	case "build_consumer":
		var args buildConsumerArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		if saveAs == "" {
			return failure("build_consumer needs save_as")
		}
		commit, err := commitPolicy(args.Commit)
		if err != nil {
			return failure("arguments: %v", err)
		}
		options := iggcon.DefaultPollOptions()
		options.Commit = commit
		options.MaxWait = milliseconds(args.MaxWaitMs)
		if args.Count != nil {
			options.Count = *args.Count
		}
		group, err := iggcon.NewIdentifier(args.Group)
		if err != nil {
			return sdkError(err)
		}
		built, err := reader.NewIggyConsumer(ctx, cli, args.StreamID.identifier, args.TopicID.identifier,
			iggcon.NewGroupConsumer(group), reader.WithPollOptions(options))
		if err != nil {
			return sdkError(err)
		}
		a.keepConsumer(saveAs, built)
		return result(nil)
	case "send_binary_request":
		var args binaryRequestArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		payload, err := hex.DecodeString(args.PayloadHex)
		if err != nil {
			return failure("payload_hex: %v", err)
		}
		answer, err := cli.SendBinaryRequest(ctx, args.Code, payload)
		if err != nil {
			return sdkError(err)
		}
		return result(rawResult{PayloadHex: hex.EncodeToString(answer)})
	case "send_binary_request_to_replica":
		var args binaryRequestToReplicaArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		payload, err := hex.DecodeString(args.PayloadHex)
		if err != nil {
			return failure("payload_hex: %v", err)
		}
		answer, err := cli.SendBinaryRequestToReplica(ctx, args.Code, payload, args.Replica)
		if err != nil {
			return sdkError(err)
		}
		return result(rawResult{PayloadHex: hex.EncodeToString(answer)})
	case "describe_options":
		var args describeOptionsArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		scopes := map[string]iggcon.OptionsScope{
			"stream": iggcon.OptionsScopeStream, "topic": iggcon.OptionsScopeTopic, "user": iggcon.OptionsScopeUser,
		}
		scope, known := scopes[args.Scope]
		if !known {
			return failure("unknown scope %q", args.Scope)
		}
		specs, err := cli.DescribeOptions(ctx, scope)
		if err != nil {
			return sdkError(err)
		}
		results := make([]optionSpecResult, 0, len(specs))
		for _, spec := range specs {
			results = append(results, canonicalOptionSpec(spec))
		}
		return result(results)
	case "create_user":
		var args createUserArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		status, err := userStatus(args.Status)
		if err != nil {
			return failure("arguments: %v", err)
		}
		var permissions *iggcon.Permissions
		if args.Permissions != nil {
			global := args.Permissions.Global
			permissions = &iggcon.Permissions{Global: iggcon.GlobalPermissions{
				ManageServers: global.ManageServers, ReadServers: global.ReadServers,
				ManageUsers: global.ManageUsers, ReadUsers: global.ReadUsers,
				ManageStreams: global.ManageStreams, ReadStreams: global.ReadStreams,
				ManageTopics: global.ManageTopics, ReadTopics: global.ReadTopics,
				PollMessages: global.PollMessages, SendMessages: global.SendMessages,
			}}
		}
		user, err := cli.CreateUser(ctx, args.Username, args.Password, status, permissions, nil)
		if err != nil {
			return sdkError(err)
		}
		return converted(canonicalUser(user))
	case "get_user":
		var args userArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		user, err := cli.GetUser(ctx, args.UserID.identifier)
		if err != nil {
			return sdkError(err)
		}
		if user == nil {
			return result(nil)
		}
		return converted(canonicalUser(user))
	case "update_user":
		var args updateUserArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		update := iggcon.UserUpdate{Username: args.Username}
		if args.Status != nil {
			status, err := userStatus(args.Status)
			if err != nil {
				return failure("arguments: %v", err)
			}
			update.Status = &status
		}
		return nullOrError(cli.UpdateUser(ctx, args.UserID.identifier, update))
	case "delete_user":
		var args userArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		return nullOrError(cli.DeleteUser(ctx, args.UserID.identifier))
	case "get_metadata_changes":
		var args metadataChangesArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		groups := make([]iggcon.MetadataChangeGroup, 0, len(args.Groups))
		for _, name := range args.Groups {
			group, err := iggcon.ParseMetadataChangeGroup(name)
			if err != nil {
				return failure("group %q: %v", name, err)
			}
			groups = append(groups, group)
		}
		query := iggcon.NewMetadataChangesQuery(args.Start.value, groups...)
		if args.Limit != nil {
			query.Limit = *args.Limit
		}
		changes, err := cli.GetMetadataChanges(ctx, query)
		if err != nil {
			return sdkError(err)
		}
		return converted(changesView(changes))
	case "get_connection_info":
		if err := decode(raw, &noArgs{}); err != nil {
			return failure("arguments: %v", err)
		}
		info := cli.GetConnectionInfo()
		return result(connectionInfoResult{ServerAddress: info.ServerAddress})
	case "resolve_operation":
		var args resolveArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		budget := defaultResolveDeadline
		if args.DeadlineMs != nil {
			budget = time.Duration(*args.DeadlineMs) * time.Millisecond
		}
		resolveCtx, cancel := context.WithTimeout(ctx, budget)
		defer cancel()
		return nullOrError(cli.ResolveOperation(resolveCtx, args.OperationID))
	case "abandon_operation":
		var args abandonArgs
		if err := decode(raw, &args); err != nil {
			return failure("arguments: %v", err)
		}
		return nullOrError(cli.AbandonOperation(ctx, args.OperationID))
	default:
		return unsupported("the method %s", method)
	}
}

func noArgsCall(raw json.RawMessage, call func() error) reply {
	if err := decode(raw, &noArgs{}); err != nil {
		return failure("arguments: %v", err)
	}
	return nullOrError(call())
}

func nullOrError(err error) reply {
	if err != nil {
		return sdkError(err)
	}
	return result(nil)
}

func converted[T any](value T, err error) reply {
	if err != nil {
		return failure("result: %v", err)
	}
	return result(value)
}
