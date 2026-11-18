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
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.identifier.Identifier;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;
import org.apache.iggy.system.NamedPageQuery;
import org.apache.iggy.system.Page;
import org.apache.iggy.system.PageCursor;
import org.apache.iggy.system.PageQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/** Reads of the list commands: one page, or a walk over a whole list. */
final class PageReads {

    private static final PageQuery WHOLE_LIST =
            PageQuery.builder().limit(PageQuery.MAX_LIMIT).build();

    private PageReads() {}

    /**
     * Sends one list request. A query that {@link NamedPageQuery#validate()} refuses fails the call and sends nothing.
     */
    static <T> CompletableFuture<Page<T>> read(
            AsyncTcpConnection connection,
            CommandCode command,
            NamedPageQuery query,
            Function<ByteBuf, T> readItem,
            Identifier... parents) {
        return read(connection, command, query, readItem, 0, new AsyncTcpConnection.TransientFailoverState(), parents);
    }

    private static <T> CompletableFuture<Page<T>> read(
            AsyncTcpConnection connection,
            CommandCode command,
            NamedPageQuery query,
            Function<ByteBuf, T> readItem,
            long deadline,
            AsyncTcpConnection.TransientFailoverState state,
            Identifier... parents) {
        try {
            query.validate();
        } catch (IggyValidationException refused) {
            return CompletableFuture.failedFuture(refused);
        }
        return AsyncTcpConnection.mapResponse(
                connection.send(command.getValue(), BytesSerializer.toPageRequest(query, parents), deadline, state),
                response -> {
                    try {
                        return BytesDeserializer.readPage(response, readItem);
                    } finally {
                        response.release();
                    }
                });
    }

    /**
     * Reads a whole list as one call: every page keeps the caller sign-in and the budget that the walk started with, so
     * a caller sign-in between two pages ends the walk.
     */
    static <T> CompletableFuture<List<T>> readWholeList(
            Supplier<AsyncTcpConnection> connection,
            CommandCode command,
            Function<ByteBuf, T> readItem,
            Identifier... parents) {
        AsyncTcpConnection first = connection.get();
        var state = new AsyncTcpConnection.TransientFailoverState();
        state.startIn(first.sessionOwner());
        long deadline = first.operationDeadline();
        return readWholeList(query -> read(
                connection.get(),
                command,
                new NamedPageQuery(query),
                readItem,
                deadline,
                state.nextHistory(),
                parents));
    }

    /**
     * Reads a whole list in ascending id order and in pages of {@link PageQuery#MAX_LIMIT}, until a page has no next
     * cursor. The result is not one consistent read: an entity that someone creates or deletes during the walk can be
     * in it or not. A page whose next cursor is the cursor it was read with fails the walk with
     * {@link IggyMalformedResponseException}, because the walk would not end.
     */
    static <T> CompletableFuture<List<T>> readWholeList(Function<PageQuery, CompletableFuture<Page<T>>> readPage) {
        CompletableFuture<List<T>> result = new CompletableFuture<>();
        readFrom(WHOLE_LIST, readPage, new ArrayList<>(), result);
        return result;
    }

    /**
     * Completes {@code result} by hand, as {@link AsyncTcpConnection#mapResponse} does, so a failed page reaches the
     * caller as its own error and not inside a {@link java.util.concurrent.CompletionException}. A cancelled result
     * stops the walk after the page in flight.
     */
    private static <T> void readFrom(
            PageQuery query,
            Function<PageQuery, CompletableFuture<Page<T>>> readPage,
            List<T> items,
            CompletableFuture<List<T>> result) {
        CompletableFuture<Page<T>> pending;
        try {
            pending = readPage.apply(query);
        } catch (RuntimeException failure) {
            result.completeExceptionally(failure);
            return;
        }
        pending.whenComplete((page, error) -> {
            if (error != null) {
                result.completeExceptionally(error);
            } else if (!result.isDone()) {
                try {
                    items.addAll(page.items());
                    Optional<PageCursor> next = page.next();
                    if (next.isEmpty()) {
                        result.complete(items);
                    } else if (next.equals(query.cursor())) {
                        throw new IggyMalformedResponseException(
                                "Malformed page: the server answered the page cursor " + next.get() + " it was sent");
                    } else {
                        readFrom(query.withCursor(next.get()), readPage, items, result);
                    }
                } catch (RuntimeException failure) {
                    result.completeExceptionally(failure);
                }
            }
        });
    }
}
