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

package org.apache.iggy.client.async.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientEndpointFailoverTest.MockNode;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Request;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.client.async.tcp.vsr.VsrOperation;
import org.apache.iggy.consumergroup.ConsumerGroup;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.exception.IggyTimeoutException;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.identifier.Identifier;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.system.NamedPageQuery;
import org.apache.iggy.system.Page;
import org.apache.iggy.system.PageCursor;
import org.apache.iggy.system.PageQuery;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.user.UserInfo;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iggy.ListPagesFixture.section;
import static org.apache.iggy.MetadataChangesFixture.buffer;
import static org.apache.iggy.MetadataChangesFixture.bytes;
import static org.apache.iggy.MetadataChangesFixture.find;
import static org.apache.iggy.ResourceOptionsFixture.hex;
import static org.apache.iggy.client.async.AsyncClientIntegrationTest.field;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientFetchMessagesTest.signedIn;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientFetchMessagesTest.singleNode;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientFetchMessagesTest.unexpected;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_CLUSTER_METADATA_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_REGISTER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.assertServerError;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.singleNodeMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The page walk of the whole-list methods and the refusal of a broken query, against one fake node. */
class ListPagesWalkTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(2);
    // One page fits in the request timeout and two do not.
    private static final Duration PAGE_DELAY = REQUEST_TIMEOUT.multipliedBy(3).dividedBy(5);
    // A sign-in needs another loop than the one a paused walk holds.
    private static final int IO_THREADS = 4;
    private static final HexFormat HEX = HexFormat.of();

    /** {@code [next_len = 0][count = 0]}: an empty last page. */
    private static final String EMPTY_LAST_PAGE_HEX = "0000000000";

    private static final StreamId STREAM = StreamId.of(7L);
    private static final TopicId TOPIC = TopicId.of(3L);

    /**
     * The first page of each list, from the shared vectors. Each has two items and a next cursor. The second page holds
     * the same two items without the cursor, so a walk that drops a page fails.
     */
    private static final Map<Integer, String> FIRST_PAGES = Map.of(
            CommandCode.Stream.GET_ALL.getValue(), "streams_with_next",
            CommandCode.Topic.GET_ALL.getValue(), "topics_created_descending_with_next",
            CommandCode.ConsumerGroup.GET_ALL.getValue(), "consumer_groups_with_next",
            CommandCode.User.GET_ALL.getValue(), "users_with_next");

    private static final NamedPageQuery WHOLE_LIST =
            new NamedPageQuery(PageQuery.builder().limit(PageQuery.MAX_LIMIT).build());

    private static final NamedPageQuery FIRST_PAGE =
            new NamedPageQuery(PageQuery.builder().build());

    @Test
    void givenTwoPagesWhenReadingWholeListsShouldSendTheNextCursorAndJoinThePages() throws Exception {
        assertThat(hex(BytesSerializer.toPageRequest(WHOLE_LIST)))
                .isEqualTo(find(section("requests"), "streams_whole_list_walk")
                        .get("request_hex")
                        .asString());
        List<Request> lists = new CopyOnWriteArrayList<>();
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> served = singleNode(socket, request -> {
                if (request.operation() != OPERATION_NON_REPLICATED
                        || !FIRST_PAGES.containsKey(request.commandCode())) {
                    return unexpected(request);
                }
                boolean first = lists.stream().noneMatch(seen -> seen.commandCode() == request.commandCode());
                lists.add(request);
                return Response.success(
                        OPERATION_NON_REPLICATED,
                        first
                                ? reply(FIRST_PAGES.get(request.commandCode()))
                                : lastPage(FIRST_PAGES.get(request.commandCode())));
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                assertThat(await(client.streams().getStreams()))
                        .extracting(StreamBase::id)
                        .containsExactly(1L, 4L, 1L, 4L);
                assertThat(await(client.topics().getTopics(STREAM)))
                        .extracting(Topic::id)
                        .containsExactly(3L, 1L, 3L, 1L);
                assertThat(await(client.consumerGroups().getConsumerGroups(STREAM, TOPIC)))
                        .extracting(ConsumerGroup::id)
                        .containsExactly(1L, 3L, 1L, 3L);
                assertThat(await(client.users().getUsers()))
                        .extracting(UserInfo::id)
                        .containsExactly(1L, 2L, 1L, 2L);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        assertThat(lists.stream().map(request -> HEX.formatHex(request.body())))
                .containsExactly(
                        firstPage(),
                        secondPage(CommandCode.Stream.GET_ALL.getValue()),
                        firstPage(STREAM),
                        secondPage(CommandCode.Topic.GET_ALL.getValue(), STREAM),
                        firstPage(STREAM, TOPIC),
                        secondPage(CommandCode.ConsumerGroup.GET_ALL.getValue(), STREAM, TOPIC),
                        firstPage(),
                        secondPage(CommandCode.User.GET_ALL.getValue()));
    }

    @Test
    void givenAPageThatRepeatsItsCursorWhenReadingAWholeListShouldFailAsMalformed() throws Exception {
        List<Request> lists = new CopyOnWriteArrayList<>();
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> served = singleNode(socket, request -> {
                if (!request.is(CommandCode.Stream.GET_ALL.getValue(), OPERATION_NON_REPLICATED)) {
                    return unexpected(request);
                }
                lists.add(request);
                // Bounded, so a walk without the guard ends on the third page and does not loop.
                return Response.success(
                        OPERATION_NON_REPLICATED,
                        lists.size() <= 2 ? reply("streams_with_next") : lastPage("streams_with_next"));
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                assertThatThrownBy(() -> await(client.streams().getStreams()))
                        .cause()
                        .isInstanceOf(IggyMalformedResponseException.class);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        assertThat(lists.stream().map(request -> HEX.formatHex(request.body())))
                .containsExactly(firstPage(), secondPage(CommandCode.Stream.GET_ALL.getValue()));
    }

    @Test
    void givenABrokenQueryWhenReadingAPageShouldFailBeforeSendingIt() throws Exception {
        PageQuery broken = PageQuery.builder().limit(0).build();
        NamedPageQuery brokenNamed = new NamedPageQuery(broken);
        List<Request> lists = new CopyOnWriteArrayList<>();
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            CompletableFuture<Void> served = singleNode(socket, request -> {
                if (!request.is(CommandCode.Stream.GET_ALL.getValue(), OPERATION_NON_REPLICATED)) {
                    return unexpected(request);
                }
                lists.add(request);
                return Response.success(OPERATION_NON_REPLICATED, buffer(EMPTY_LAST_PAGE_HEX));
            });
            AsyncIggyTcpClient client = signedIn(socket);
            try {
                List<CompletableFuture<?>> refused = List.of(
                        client.streams().getStreamsPage(brokenNamed),
                        client.topics().getTopicsPage(STREAM, brokenNamed),
                        client.partitions().getPartitionsPage(STREAM, TOPIC, broken),
                        client.consumerGroups().getConsumerGroupsPage(STREAM, TOPIC, brokenNamed),
                        client.users().getUsersPage(brokenNamed));
                for (CompletableFuture<?> page : refused) {
                    assertThat(page).isCompletedExceptionally();
                    assertThatThrownBy(() -> await(page))
                            .cause()
                            .isInstanceOfSatisfying(
                                    IggyValidationException.class,
                                    refusal -> assertThat(refusal.getErrorCode())
                                            .isEqualTo(IggyErrorCode.INVALID_COMMAND));
                }

                assertThat(await(client.streams().getStreamsPage(FIRST_PAGE)).next())
                        .isEmpty();
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
            served.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        assertThat(lists.stream().map(request -> HEX.formatHex(request.body())))
                .containsExactly(hex(BytesSerializer.toPageRequest(FIRST_PAGE)));
    }

    /**
     * The walk is one call, so a caller sign-in after page 1 ends it with 40 instead of reading page 2 as the new user.
     * The walk holds the event loop of its connection while the caller signs in, so the caller reconnects first, as
     * in {@code AsyncIggyTcpClientSessionRulesTest}.
     */
    @Test
    void givenACallerSignInBetweenTwoPagesWhenReadingAWholeListShouldFailAsUnauthenticated() throws Exception {
        List<Request> lists = new CopyOnWriteArrayList<>();
        AtomicInteger registrations = new AtomicInteger();
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            MockNode node = MockNode.serve(socket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    return Response.success(OPERATION_REGISTER, registerBody(registrations.incrementAndGet()));
                }
                if (request.operation() == VsrOperation.LOGOUT) {
                    return Response.success(VsrOperation.LOGOUT, Unpooled.EMPTY_BUFFER);
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, singleNodeMetadata(socket.getLocalPort()));
                }
                if (!request.is(CommandCode.Stream.GET_ALL.getValue(), OPERATION_NON_REPLICATED)) {
                    return unexpected(request);
                }
                lists.add(request);
                return Response.success(
                        OPERATION_NON_REPLICATED,
                        lists.size() == 1 ? reply("streams_with_next") : lastPage("streams_with_next"));
            });
            AsyncIggyTcpClient client = client(socket);
            CompletableFuture<Void> paused = new CompletableFuture<>();
            CompletableFuture<Void> resume = new CompletableFuture<>();
            try {
                client.connect().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                client.users().login("alice", "alice-password").get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                @SuppressWarnings("unchecked")
                var connection = (AtomicReference<AsyncTcpConnection>) field(client, "connection");
                AtomicBoolean held = new AtomicBoolean();
                var streams = new StreamsTcpClient(() -> {
                    if (!lists.isEmpty() && held.compareAndSet(false, true)) {
                        paused.complete(null);
                        resume.join();
                    }
                    return connection.get();
                });

                var walk = streams.getStreams();
                paused.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                client.connect().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                client.users().login("bob", "bob-password").get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                resume.complete(null);

                assertServerError(walk, IggyErrorCode.UNAUTHENTICATED.getCode());
                assertThat(lists).hasSize(1);
            } finally {
                resume.complete(null);
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                node.close();
            }
        }
    }

    @Test
    void givenPagesThatTogetherOutlastTheRequestTimeoutWhenReadingAWholeListShouldTimeOut() throws Exception {
        List<Request> lists = new CopyOnWriteArrayList<>();
        try (ServerSocket socket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            singleNode(socket, request -> {
                if (!request.is(CommandCode.Stream.GET_ALL.getValue(), OPERATION_NON_REPLICATED)) {
                    return unexpected(request);
                }
                lists.add(request);
                try {
                    Thread.sleep(PAGE_DELAY.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
                return Response.success(
                        OPERATION_NON_REPLICATED,
                        lists.size() == 1 ? reply("streams_with_next") : lastPage("streams_with_next"));
            });
            AsyncIggyTcpClient client = client(socket);
            try {
                client.connect().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                client.users().login("alice", "alice-password").get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                assertThatThrownBy(() -> await(client.streams().getStreams()))
                        .cause()
                        .isInstanceOf(IggyTimeoutException.class);
                assertThat(lists).hasSize(2);
            } finally {
                client.close().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    @Test
    void givenAFailedPageWhenReadingAWholeListShouldPassItsOwnErrorToTheCaller() {
        PageCursor next = PageCursor.parse(
                find(section("responses"), "streams_with_next").get("next").asString());
        IllegalStateException refused = new IllegalStateException("page refused");

        CompletableFuture<List<Long>> firstFails =
                PageReads.readWholeList(query -> CompletableFuture.failedFuture(refused));
        CompletableFuture<List<Long>> secondFails =
                PageReads.readWholeList(query -> query.cursor().isEmpty()
                        ? CompletableFuture.completedFuture(new Page<>(List.of(1L), Optional.of(next)))
                        : CompletableFuture.failedFuture(refused));

        for (CompletableFuture<List<Long>> walk : List.of(firstFails, secondFails)) {
            assertThat(walk.handle((items, error) -> error).join()).isSameAs(refused);
        }
    }

    private static AsyncIggyTcpClient client(ServerSocket socket) {
        return AsyncIggyTcpClient.builder()
                .host(socket.getInetAddress().getHostAddress())
                .port(socket.getLocalPort())
                .ioThreads(IO_THREADS)
                .requestTimeout(REQUEST_TIMEOUT)
                .heartbeatInterval(Duration.ofMinutes(1))
                .build();
    }

    private static ByteBuf reply(String vectorCase) {
        return buffer(find(section("responses"), vectorCase).get("response_hex").asString());
    }

    /** The reply of {@code vectorCase} with its next cursor cut out: the same items on the last page. */
    private static ByteBuf lastPage(String vectorCase) {
        byte[] page =
                bytes(find(section("responses"), vectorCase).get("response_hex").asString());
        int nextSize = Byte.toUnsignedInt(page[0]);
        byte[] last = new byte[page.length - nextSize];
        System.arraycopy(page, 1 + nextSize, last, 1, last.length - 1);
        return Unpooled.wrappedBuffer(last);
    }

    private static String firstPage(Identifier... parents) {
        return hex(BytesSerializer.toPageRequest(WHOLE_LIST, parents));
    }

    /** The second request of a walk carries the next cursor of the first page of {@code command}. */
    private static String secondPage(int command, Identifier... parents) {
        JsonNode first = find(section("responses"), FIRST_PAGES.get(command));
        PageCursor next = PageCursor.parse(first.get("next").asString());
        return hex(BytesSerializer.toPageRequest(WHOLE_LIST.withCursor(next), parents));
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        return future.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }
}
