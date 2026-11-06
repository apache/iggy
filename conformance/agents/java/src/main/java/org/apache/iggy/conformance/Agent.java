/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iggy.conformance;

import org.apache.iggy.IggyVersion;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.io.BufferedReader;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The conformance agent of the Java SDK. It reads JSON requests from stdin, calls the SDK and writes one JSON answer
 * per request to stdout. It never judges a result. See conformance/README.md for the protocol.
 */
public final class Agent {

    private static final int PROTOCOL_VERSION = 1;

    private static final List<String> CAPABILITIES = List.of(
            "reader",
            "consumer",
            "resolve_operation",
            "abandon_operation",
            "raw_command",
            "raw_command_replica",
            "describe_options",
            "metadata_changes",
            "connection_info");

    private static final Logger log = LoggerFactory.getLogger(Agent.class);
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final PrintStream answers;
    private final Entities entities = new Entities();
    private final Calls calls = new Calls(entities);
    private final Map<String, Task> tasks = new ConcurrentHashMap<>();

    // Each request runs on its own thread, so a long call never delays a reset or a cancellation behind it.
    private final ExecutorService workers = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "conformance-request");
        thread.setDaemon(true);
        return thread;
    });

    private Agent(PrintStream answers) {
        this.answers = answers;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        PrintStream answers = new PrintStream(new FileOutputStream(FileDescriptor.out), false, StandardCharsets.UTF_8);
        // stdout carries only answers, so whatever else prints there goes to stderr.
        System.setOut(System.err);
        Agent agent = new Agent(answers);
        agent.write(hello());
        BufferedReader requests = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        for (String line = requests.readLine(); line != null; line = requests.readLine()) {
            if (!line.isBlank()) {
                agent.accept(line);
            }
        }
        agent.reset();
        agent.workers.shutdown();
        agent.workers.awaitTermination(5, TimeUnit.SECONDS);
        System.exit(0);
    }

    private static ObjectNode hello() {
        ObjectNode line = Canonical.object();
        ObjectNode hello = line.putObject("hello");
        hello.put("sdk", "java");
        hello.put("sdk_version", IggyVersion.getInstance().getVersion());
        hello.put("protocol", PROTOCOL_VERSION);
        hello.putArray("transports").add("tcp");
        ArrayNode capabilities = hello.putArray("capabilities");
        CAPABILITIES.forEach(capabilities::add);
        return line;
    }

    private void accept(String line) {
        JsonNode request;
        try {
            request = JSON.readTree(line);
        } catch (JacksonException error) {
            log.error("Cannot read the request {}: {}", line, error.getMessage());
            return;
        }
        workers.execute(() -> write(handle(request).line(request.get("id"))));
    }

    private synchronized void write(JsonNode line) {
        answers.print(JSON.writeValueAsString(line) + "\n");
        answers.flush();
    }

    private Reply handle(JsonNode request) {
        try {
            String op = text(request, "op");
            return switch (op) {
                case "create" -> create(text(request, "kind"), text(request, "entity"), request.get("args"));
                case "call" ->
                    invoke(
                            new Call(),
                            text(request, "entity"),
                            text(request, "method"),
                            request.get("args"),
                            optionalText(request, "save_as"));
                case "start" ->
                    start(text(request, "task"), text(request, "entity"), text(request, "method"), request.get("args"));
                case "await" -> await(text(request, "task"));
                case "cancel" -> cancel(text(request, "task"));
                case "close" -> {
                    entities.close(text(request, "entity"));
                    yield Reply.result(null);
                }
                case "reset" -> {
                    reset();
                    yield Reply.result(null);
                }
                default -> Reply.failure("unknown op " + op);
            };
        } catch (RuntimeException error) {
            return Reply.of(error);
        }
    }

    private Reply invoke(Call call, String entity, String method, JsonNode args, String saveAs) {
        try {
            return Reply.result(calls.call(call, entity, method, args, saveAs));
        } catch (RuntimeException error) {
            return Reply.of(error);
        }
    }

    private Reply create(String kind, String entity, JsonNode raw) {
        if (!kind.equals("client")) {
            throw new Reply.NotSupported("creating a " + kind + " with create");
        }
        Args args = Args.of(raw, "transport", "address", "login");
        String transport = args.text("transport");
        if (!transport.equals("tcp")) {
            throw new Reply.NotSupported("the " + transport + " transport");
        }
        String address = args.text("address");
        int colon = address.lastIndexOf(':');
        AsyncIggyTcpClient client = AsyncIggyTcpClient.builder()
                .host(address.substring(0, colon).replace("[", "").replace("]", ""))
                .port(Integer.parseInt(address.substring(colon + 1)))
                // Netty's default quiet period would hold every close of a client for 2 seconds.
                .shutdownQuietPeriod(Duration.ZERO)
                .build();
        try {
            Call call = new Call();
            call.await(client.connect());
            Args login = args.object("login", "username", "password").orElse(null);
            if (login != null) {
                call.await(client.users().login(login.text("username"), login.text("password")));
            }
        } catch (RuntimeException error) {
            client.close();
            throw error;
        }
        entities.keepClient(entity, client);
        return Reply.result(null);
    }

    private Reply start(String name, String entity, String method, JsonNode args) {
        Call call = new Call();
        Future<Reply> reply = workers.submit(() -> invoke(call, entity, method, args, null));
        Task replaced = tasks.put(name, new Task(call, reply));
        if (replaced != null) {
            replaced.call().cancel();
        }
        return Reply.result(null);
    }

    private Reply await(String name) {
        Task task = tasks.remove(name);
        if (task == null) {
            return Reply.failure("no task " + name);
        }
        try {
            return task.reply().get();
        } catch (ExecutionException error) {
            return Reply.of(error);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return Reply.failure("interrupted while awaiting " + name);
        }
    }

    private Reply cancel(String name) {
        Task task = tasks.remove(name);
        if (task == null) {
            return Reply.failure("no task " + name);
        }
        task.call().cancel();
        return Reply.result(null);
    }

    private void reset() {
        for (String name : List.copyOf(tasks.keySet())) {
            Task task = tasks.remove(name);
            if (task != null) {
                task.call().cancel();
            }
        }
        entities.closeAll();
    }

    private static String optionalText(JsonNode request, String field) {
        JsonNode value = request.get(field);
        return value == null || value.isNull() ? null : text(request, field);
    }

    private static String text(JsonNode request, String field) {
        JsonNode value = request.get(field);
        if (value == null || !value.isString()) {
            throw new IllegalArgumentException("the request has no " + field + ": " + request);
        }
        return value.stringValue();
    }

    /** A call started as a task, and its answer once it ends. */
    private record Task(Call call, Future<Reply> reply) {}
}
