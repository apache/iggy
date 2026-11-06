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

type resolveArgs struct {
	OperationID uint64  `json:"operation_id"`
	DeadlineMs  *uint64 `json:"deadline_ms"`
}

type abandonArgs struct {
	OperationID uint64 `json:"operation_id"`
}

// disconnecter is the SDK client that can drop its links and connect again.
type disconnecter interface{ Disconnect() error }

func (a *agent) call(ctx context.Context, entity, method string, raw json.RawMessage, saveAs string) reply {
	if method == "next" {
		target := a.reader(entity)
		if target == nil {
			return failure("no reader %q", entity)
		}
		if err := decode(raw, &noArgs{}); err != nil {
			return failure("arguments: %v", err)
		}
		target.mu.Lock()
		defer target.mu.Unlock()
		polled, err := target.reader.Next(ctx)
		if err != nil {
			return sdkError(err)
		}
		return converted(canonicalPolled(polled))
	}
	cli := a.client(entity)
	if cli == nil {
		return failure("no client %q", entity)
	}
	switch method {
	case "connect":
		return noArgsCall(raw, func() error { return cli.Connect(ctx) })
	case "disconnect":
		dropping, ok := cli.(disconnecter)
		if !ok {
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
