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

import org.apache.iggy.client.async.reader.ReadException;
import org.apache.iggy.exception.IggyException;
import org.apache.iggy.exception.IggyOperationException;
import org.apache.iggy.exception.IggyServerException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.NullNode;
import tools.jackson.databind.node.ObjectNode;

import java.math.BigInteger;
import java.util.OptionalLong;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/** One answer: exactly one of a result, an SDK error, an unsupported call and a failure of the agent itself. */
sealed interface Reply {

    ObjectNode line(JsonNode id);

    static Reply result(JsonNode value) {
        return new Result(value == null ? NullNode.getInstance() : value);
    }

    static Reply failure(String reason) {
        return new Failure(reason);
    }

    /**
     * The answer for a call that threw. An SDK error carries its Iggy code. An error without one is a failure, as is
     * any fault of the agent.
     */
    static Reply of(Throwable thrown) {
        Throwable error = unwrap(thrown);
        if (error instanceof NotSupported notSupported) {
            return new Unsupported(notSupported.getMessage());
        }
        OptionalLong readPartition = OptionalLong.empty();
        if (error instanceof ReadException read && read.getCause() != null) {
            readPartition = read.partitionId();
            error = unwrap(read.getCause());
        }
        if (error instanceof IggyException && !(error instanceof IggyServerException)) {
            // Code 0 belongs to no IggyError, so the test fails and the skip list can name it.
            return new SdkError(0, OptionalLong.empty(), readPartition, String.valueOf(error.getMessage()));
        }
        if (!(error instanceof IggyServerException server)) {
            return new Failure(error.toString());
        }
        OptionalLong operationId = OptionalLong.empty();
        OptionalLong partitionId = readPartition;
        if (server instanceof IggyOperationException operation) {
            operationId = OptionalLong.of(operation.getOperationId());
            if (operation.getPartitionId().isPresent()) {
                partitionId = operation.getPartitionId();
            }
        }
        return new SdkError(server.getRawErrorCode(), operationId, partitionId, server.getMessage());
    }

    private static Throwable unwrap(Throwable error) {
        while ((error instanceof CompletionException || error instanceof ExecutionException)
                && error.getCause() != null) {
            error = error.getCause();
        }
        return error;
    }

    private static ObjectNode answer(JsonNode id, String field, JsonNode value) {
        ObjectNode line = JsonNodeFactory.instance.objectNode();
        line.set("id", id);
        line.set(field, value);
        return line;
    }

    record Result(JsonNode value) implements Reply {
        @Override
        public ObjectNode line(JsonNode id) {
            return answer(id, "result", value);
        }
    }

    record SdkError(long code, OptionalLong operationId, OptionalLong partitionId, String message) implements Reply {
        @Override
        public ObjectNode line(JsonNode id) {
            ObjectNode error = JsonNodeFactory.instance.objectNode();
            error.put("code", code);
            // Operation ids are unsigned 64-bit values.
            operationId.ifPresent(
                    operation -> error.put("operation_id", new BigInteger(Long.toUnsignedString(operation))));
            partitionId.ifPresent(partition -> error.put("partition_id", partition));
            error.put("message", message);
            return answer(id, "error", error);
        }
    }

    record Unsupported(String reason) implements Reply {
        @Override
        public ObjectNode line(JsonNode id) {
            return answer(id, "unsupported", JsonNodeFactory.instance.stringNode(reason));
        }
    }

    record Failure(String reason) implements Reply {
        @Override
        public ObjectNode line(JsonNode id) {
            return answer(id, "failure", JsonNodeFactory.instance.stringNode(reason));
        }
    }

    /** Ends a call as unsupported: the SDK has no such method, argument value, transport or entity kind. */
    final class NotSupported extends RuntimeException {
        NotSupported(String reason) {
            super(reason);
        }
    }
}
