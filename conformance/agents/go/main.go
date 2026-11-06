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

// The conformance agent of the Go SDK. It reads JSON requests from stdin,
// calls the SDK and writes one JSON answer per request to stdout. It never
// judges a result. See conformance/README.md for the protocol.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/reader"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

const protocolVersion = 1

// sdkVersion names the SDK build. The agent builds against the SDK tree of
// the same commit, so the version is the commit's.
const sdkVersion = "workspace"

var capabilities = []string{
	"reader", "consumer", "resolve_operation", "abandon_operation", "raw_command",
	"raw_command_replica", "describe_options", "metadata_changes", "connection_info",
}

type request struct {
	ID     uint64          `json:"id"`
	Op     string          `json:"op"`
	Kind   string          `json:"kind"`
	Entity string          `json:"entity"`
	Method string          `json:"method"`
	Args   json.RawMessage `json:"args"`
	SaveAs string          `json:"save_as"`
	Task   string          `json:"task"`
}

type agentError struct {
	Code        uint32  `json:"code"`
	OperationID *uint64 `json:"operation_id,omitempty"`
	PartitionID *uint32 `json:"partition_id,omitempty"`
	Message     string  `json:"message,omitempty"`
}

// reply is exactly one of a result, an SDK error, an unsupported call and a
// failure of the agent itself.
type reply struct {
	result      any
	err         *agentError
	unsupported string
	failure     string
}

func result(value any) reply { return reply{result: value} }

func unsupported(format string, args ...any) reply {
	return reply{unsupported: fmt.Sprintf(format, args...)}
}

func failure(format string, args ...any) reply {
	return reply{failure: fmt.Sprintf(format, args...)}
}

// sdkError is the answer for an error of the SDK. An error that carries no
// Iggy code is code 0, which no IggyError has, so the test fails and the
// skip list can name the divergence.
func sdkError(err error) reply {
	var iggyError ierror.IggyError
	if !errors.As(err, &iggyError) {
		return reply{err: &agentError{Code: 0, Message: err.Error()}}
	}
	answer := &agentError{Code: uint32(iggyError.Code()), Message: err.Error()}
	var uncertain ierror.OperationUncertain
	var blocked ierror.LaneBlocked
	var notRetained ierror.OperationNotRetained
	var partitionBlocked ierror.PartitionLaneBlocked
	var fetchUncertain ierror.FetchProgressUncertain
	switch {
	case errors.As(err, &partitionBlocked):
		answer.OperationID = &partitionBlocked.OperationID
		answer.PartitionID = &partitionBlocked.PartitionID
	case errors.As(err, &uncertain):
		answer.OperationID = &uncertain.OperationID
	case errors.As(err, &blocked):
		answer.OperationID = &blocked.OperationID
	case errors.As(err, &notRetained):
		answer.OperationID = &notRetained.OperationID
	case errors.As(err, &fetchUncertain):
		answer.OperationID = &fetchUncertain.OperationID
	}
	var readError *reader.ReadError
	if answer.PartitionID == nil && errors.As(err, &readError) && readError.PartitionID != nil {
		answer.PartitionID = readError.PartitionID
	}
	return reply{err: answer}
}

func (r reply) line(id uint64) map[string]any {
	line := map[string]any{"id": id}
	switch {
	case r.err != nil:
		line["error"] = r.err
	case r.unsupported != "":
		line["unsupported"] = r.unsupported
	case r.failure != "":
		line["failure"] = r.failure
	default:
		line["result"] = r.result
	}
	return line
}

type output struct {
	mu      sync.Mutex
	encoder *json.Encoder
}

func (o *output) write(line any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.encoder.Encode(line); err != nil {
		slog.Error("cannot write an answer", "error", err)
	}
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	out := &output{encoder: json.NewEncoder(os.Stdout)}
	out.write(map[string]any{"hello": map[string]any{
		"sdk":          "go",
		"sdk_version":  sdkVersion,
		"protocol":     protocolVersion,
		"transports":   []string{"tcp"},
		"capabilities": capabilities,
	}})

	agent := newAgent()
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 1024*1024), 256*1024*1024)
	var running sync.WaitGroup
	for scanner.Scan() {
		var req request
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			slog.Error("cannot read a request", "error", err)
			continue
		}
		// Each request runs on its own goroutine, so a long call never delays
		// a reset or a cancellation behind it.
		running.Add(1)
		go func() {
			defer running.Done()
			out.write(agent.handle(req).line(req.ID))
		}()
	}
	if err := scanner.Err(); err != nil {
		slog.Error("cannot read stdin", "error", err)
	}
	agent.reset()
	running.Wait()
}

type task struct {
	cancel context.CancelFunc
	done   chan reply
}

type readerEntity struct {
	mu     sync.Mutex
	reader *reader.PartitionReader
}

type consumerEntity struct {
	mu       sync.Mutex
	consumer *reader.IggyConsumer
}

type agent struct {
	mu        sync.Mutex
	clients   map[string]iggcon.Client
	readers   map[string]*readerEntity
	consumers map[string]*consumerEntity
	tasks     map[string]*task
}

func newAgent() *agent {
	return &agent{
		clients:   map[string]iggcon.Client{},
		readers:   map[string]*readerEntity{},
		consumers: map[string]*consumerEntity{},
		tasks:     map[string]*task{},
	}
}

func (a *agent) handle(req request) reply {
	switch req.Op {
	case "create":
		if req.Kind != "client" {
			return unsupported("creating a %s with create", req.Kind)
		}
		return a.createClient(req.Entity, req.Args)
	case "call":
		return a.call(context.Background(), req.Entity, req.Method, req.Args, req.SaveAs)
	case "start":
		ctx, cancel := context.WithCancel(context.Background())
		started := &task{cancel: cancel, done: make(chan reply, 1)}
		a.mu.Lock()
		if previous, ok := a.tasks[req.Task]; ok {
			previous.cancel()
		}
		a.tasks[req.Task] = started
		a.mu.Unlock()
		go func() {
			started.done <- a.call(ctx, req.Entity, req.Method, req.Args, "")
		}()
		return result(nil)
	case "await":
		running := a.takeTask(req.Task)
		if running == nil {
			return failure("no task %q", req.Task)
		}
		answer := <-running.done
		running.cancel()
		return answer
	case "cancel":
		running := a.takeTask(req.Task)
		if running == nil {
			return failure("no task %q", req.Task)
		}
		running.cancel()
		return result(nil)
	case "close":
		a.close(req.Entity)
		return result(nil)
	case "reset":
		a.reset()
		return result(nil)
	default:
		return failure("unknown op %q", req.Op)
	}
}

type createClientArgs struct {
	Transport string `json:"transport"`
	Address   string `json:"address"`
	Login     *struct {
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"login"`
}

func (a *agent) createClient(entity string, raw json.RawMessage) reply {
	var args createClientArgs
	if err := decode(raw, &args); err != nil {
		return failure("create arguments: %v", err)
	}
	if args.Transport != "tcp" {
		return unsupported("the %s transport", args.Transport)
	}
	created, err := client.NewIggyClient(client.WithTcp(tcp.WithServerAddress(args.Address)))
	if err != nil {
		return sdkError(err)
	}
	ctx := context.Background()
	if err = created.Connect(ctx); err != nil {
		_ = created.Close()
		return sdkError(err)
	}
	if args.Login != nil {
		if _, err = created.LoginUser(ctx, args.Login.Username, args.Login.Password); err != nil {
			_ = created.Close()
			return sdkError(err)
		}
	}
	a.mu.Lock()
	a.clients[entity] = created
	a.mu.Unlock()
	return result(nil)
}

func (a *agent) client(entity string) iggcon.Client {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.clients[entity]
}

func (a *agent) reader(entity string) *readerEntity {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.readers[entity]
}

func (a *agent) keepReader(entity string, partitionReader *reader.PartitionReader) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.readers[entity] = &readerEntity{reader: partitionReader}
}

func (a *agent) consumer(entity string) *consumerEntity {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.consumers[entity]
}

func (a *agent) keepConsumer(entity string, groupConsumer *reader.IggyConsumer) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.consumers[entity] = &consumerEntity{consumer: groupConsumer}
}

func (a *agent) takeTask(name string) *task {
	a.mu.Lock()
	defer a.mu.Unlock()
	running := a.tasks[name]
	delete(a.tasks, name)
	return running
}

func (a *agent) close(entity string) {
	a.mu.Lock()
	partitionReader := a.readers[entity]
	delete(a.readers, entity)
	groupConsumer := a.consumers[entity]
	delete(a.consumers, entity)
	closing := a.clients[entity]
	delete(a.clients, entity)
	a.mu.Unlock()
	if groupConsumer != nil {
		if err := groupConsumer.consumer.Shutdown(context.Background()); err != nil {
			slog.Warn("cannot shut down a consumer", "entity", entity, "error", err)
		}
	}
	if partitionReader != nil {
		if err := partitionReader.reader.Close(context.Background()); err != nil {
			slog.Warn("cannot close a reader", "entity", entity, "error", err)
		}
	}
	if closing != nil {
		if err := closing.Close(); err != nil {
			slog.Warn("cannot close a client", "entity", entity, "error", err)
		}
	}
}

func (a *agent) reset() {
	a.mu.Lock()
	tasks := a.tasks
	a.tasks = map[string]*task{}
	entities := make([]string, 0, len(a.readers)+len(a.consumers)+len(a.clients))
	for entity := range a.readers {
		entities = append(entities, entity)
	}
	for entity := range a.consumers {
		entities = append(entities, entity)
	}
	for entity := range a.clients {
		entities = append(entities, entity)
	}
	a.mu.Unlock()
	for _, running := range tasks {
		running.cancel()
	}
	for _, entity := range entities {
		a.close(entity)
	}
}
