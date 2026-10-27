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

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientEndpointFailoverTest.MockNode;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientEndpointFailoverTest.RequestHandler;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.Response;
import org.apache.iggy.client.async.tcp.vsr.VsrOperation;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.identifier.UserId;
import org.apache.iggy.serde.BytesSerializer;
import org.apache.iggy.serde.CommandCode;
import org.apache.iggy.user.UserUpdate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientEndpointFailoverTest.FIRST_PASSWORD;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientEndpointFailoverTest.RECOVERY;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientEndpointFailoverTest.SECOND_PASSWORD;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientEndpointFailoverTest.SIGNED_IN_USER_ID;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientEndpointFailoverTest.USERNAME;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientEndpointFailoverTest.limitedClient;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientEndpointFailoverTest.uncertainOperation;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.CHANGE_PASSWORD_OPERATION;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.GET_CLUSTER_METADATA_CODE;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_LOGOUT;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_NON_REPLICATED;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.OPERATION_REGISTER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.RENAME_USER;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.UPDATE_RESOURCE_OPERATION;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.client;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.committedOperation;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.registerBody;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.serve;
import static org.apache.iggy.client.async.tcp.AsyncIggyTcpClientTransientFailoverTest.singleNodeMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A committed rename or password change of the signed-in user reaches the credentials of its next sign-in. */
class AsyncIggyTcpClientCredentialChangeTest {
    private static final int CHANGE_PASSWORD_CODE = CommandCode.User.CHANGE_PASSWORD.getValue();
    private static final int UPDATE_RESOURCE_CODE = CommandCode.System.UPDATE_RESOURCE.getValue();
    private static final long OTHER_USER_ID = 2;
    private static final String RENAMED = "carol";

    @Test
    void shouldRetainSuccessfulSelfRenameAndPasswordChangeForConfiguredLogin() throws Exception {
        InetAddress loopback = InetAddress.getLoopbackAddress();
        try (ServerSocket serverSocket = new ServerSocket(0, 4, loopback)) {
            List<String> logins = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> server = serve(serverSocket, request -> {
                if (request.operation() == OPERATION_REGISTER) {
                    logins.add(request.bodyAsText());
                    return Response.success(OPERATION_REGISTER, registerBody(logins.size()));
                }
                if (request.is(GET_CLUSTER_METADATA_CODE, OPERATION_NON_REPLICATED)) {
                    return Response.success(OPERATION_NON_REPLICATED, singleNodeMetadata(serverSocket.getLocalPort()));
                }
                if (request.operation() == UPDATE_RESOURCE_OPERATION
                        || request.operation() == CHANGE_PASSWORD_OPERATION) {
                    return Response.committed(
                            committedOperation(request.operation()),
                            11,
                            Unpooled.buffer().writeIntLE(0));
                }
                if (request.operation() == OPERATION_LOGOUT) {
                    return Response.success(OPERATION_LOGOUT, Unpooled.EMPTY_BUFFER);
                }
                throw new IllegalStateException("Unexpected request: " + request);
            });
            AsyncIggyTcpClient client = client(serverSocket);
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                client.users().updateUser(UserId.of(1L), RENAME_USER).get(5, TimeUnit.SECONDS);
                client.users()
                        .changePassword(UserId.of("renamed-user"), "configured-password", "new-password")
                        .get(5, TimeUnit.SECONDS);
                client.users().logout().get(5, TimeUnit.SECONDS);
                client.login().get(5, TimeUnit.SECONDS);
                assertThat(logins).hasSize(2);
                assertThat(logins.get(0)).contains("configured-user").contains("configured-password");
                assertThat(logins.get(1))
                        .contains("renamed-user")
                        .contains("new-password")
                        .doesNotContain("configured-user")
                        .doesNotContain("configured-password");
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
            }
            server.get(5, TimeUnit.SECONDS);
        }
    }

    /**
     * A committed password change or rename of the signed-in user reaches every later sign-in, also when the
     * change was sent raw, or ended uncertain until a resolve proved it. Only the request names the new
     * credentials.
     */
    @ParameterizedTest(name = "raw={0}, rename={1}")
    @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    @SuppressWarnings("checkstyle:NPathComplexity")
    void shouldSignInWithTheCommittedChangeOfTheSignedInUser(boolean raw, boolean rename) throws Exception {
        try (ServerSocket nodeSocket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            AtomicBoolean committing = new AtomicBoolean(raw);
            AtomicBoolean hangUpOnRegister = new AtomicBoolean();
            List<String> signIns = new CopyOnWriteArrayList<>();
            MockNode node = credentialsNode(
                    nodeSocket,
                    hangUpOnRegister,
                    signIns,
                    request -> committing.get()
                            ? Response.committed(
                                    committedOperation(request.operation()),
                                    2,
                                    Unpooled.buffer().writeIntLE(0))
                            : Response.error(request.operation(), AsyncTcpConnection.TRANSIENT_NOT_COMMITTED));
            AsyncIggyTcpClient client = limitedClient(nodeSocket.getLocalPort());
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login(USERNAME, FIRST_PASSWORD).get(5, TimeUnit.SECONDS);
                // A rename names the user by name and a password change by id, so both ways must match.
                UserId self = rename ? UserId.of(USERNAME) : UserId.of(SIGNED_IN_USER_ID);
                if (raw) {
                    client.sendBinaryRequest(
                                    rename ? UPDATE_RESOURCE_CODE : CHANGE_PASSWORD_CODE,
                                    rename
                                            ? renameBody(self, RENAMED)
                                            : changePasswordBody(self, FIRST_PASSWORD, SECOND_PASSWORD))
                            .get(5, TimeUnit.SECONDS);
                } else {
                    CompletableFuture<Void> change = rename
                            ? client.users()
                                    .updateUser(
                                            self,
                                            UserUpdate.builder()
                                                    .username(RENAMED)
                                                    .build())
                            : client.users().changePassword(self, FIRST_PASSWORD, SECOND_PASSWORD);
                    long operation = uncertainOperation(change);
                    committing.set(true);
                    client.resolveOperation(operation, Duration.ofSeconds(1)).get(5, TimeUnit.SECONDS);
                }

                assertThat(signInsAfterTheSessionIsLost(client, node, hangUpOnRegister, signIns))
                        .allSatisfy(signIn -> assertThat(signIn)
                                .contains(rename ? RENAMED : USERNAME, rename ? FIRST_PASSWORD : SECOND_PASSWORD)
                                .doesNotContain(rename ? USERNAME : FIRST_PASSWORD));
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                node.close();
            }
        }
    }

    /**
     * A change that the server committed as rejected, such as a wrong current password, and a change of another
     * user leave the sign-in as it was.
     */
    @ParameterizedTest(name = "rejected={0}")
    @ValueSource(booleans = {false, true})
    void shouldKeepTheSignInAfterARejectedChangeOrAChangeOfAnotherUser(boolean rejected) throws Exception {
        try (ServerSocket nodeSocket = new ServerSocket(0, 4, InetAddress.getLoopbackAddress())) {
            AtomicBoolean hangUpOnRegister = new AtomicBoolean();
            List<String> signIns = new CopyOnWriteArrayList<>();
            MockNode node = credentialsNode(
                    nodeSocket,
                    hangUpOnRegister,
                    signIns,
                    request -> Response.committed(
                            request.operation(),
                            2,
                            rejected
                                    ? AsyncIggyTcpClientTransientFailoverTest.transientResult(
                                            IggyErrorCode.INVALID_CREDENTIALS.getCode())
                                    : Unpooled.buffer().writeIntLE(0)));
            AsyncIggyTcpClient client = limitedClient(nodeSocket.getLocalPort());
            try {
                client.connect().get(5, TimeUnit.SECONDS);
                client.users().login(USERNAME, FIRST_PASSWORD).get(5, TimeUnit.SECONDS);
                var change = client.sendBinaryRequest(
                        CHANGE_PASSWORD_CODE,
                        changePasswordBody(
                                UserId.of(rejected ? SIGNED_IN_USER_ID : OTHER_USER_ID),
                                FIRST_PASSWORD,
                                SECOND_PASSWORD));
                if (rejected) {
                    assertThatThrownBy(() -> change.get(5, TimeUnit.SECONDS))
                            .cause()
                            .isInstanceOfSatisfying(
                                    IggyServerException.class,
                                    error -> assertThat(error.getRawErrorCode())
                                            .isEqualTo(IggyErrorCode.INVALID_CREDENTIALS.getCode()));
                } else {
                    change.get(5, TimeUnit.SECONDS);
                }

                assertThat(signInsAfterTheSessionIsLost(client, node, hangUpOnRegister, signIns))
                        .allSatisfy(signIn -> assertThat(signIn)
                                .contains(USERNAME, FIRST_PASSWORD)
                                .doesNotContain(SECOND_PASSWORD));
            } finally {
                client.close().get(5, TimeUnit.SECONDS);
                node.close();
            }
        }
    }

    /**
     * Records every Register and hangs up on the first one after {@code hangUpOnRegister} is set. A password
     * change or a rename gets the reply of {@code change}.
     */
    private static MockNode credentialsNode(
            ServerSocket socket, AtomicBoolean hangUpOnRegister, List<String> signIns, RequestHandler change) {
        return MockNode.serve(socket, request -> {
            if (request.operation() == OPERATION_REGISTER) {
                signIns.add(request.bodyAsText());
                return hangUpOnRegister.getAndSet(false)
                        ? Response.disconnect()
                        : Response.success(OPERATION_REGISTER, registerBody(signIns.size()));
            }
            if (request.operation() == VsrOperation.CHANGE_PASSWORD
                    || request.operation() == VsrOperation.UPDATE_RESOURCE) {
                return change.handle(request);
            }
            return Response.success(request.operation(), Unpooled.EMPTY_BUFFER);
        });
    }

    /**
     * Ends the session and returns the sign-ins that restore it: the one on the next link of the connection,
     * which hangs up, and the one that the redial replays. A roster read never waits for a session, so it ends
     * when that sign-in hangs up.
     */
    private static List<String> signInsAfterTheSessionIsLost(
            AsyncIggyTcpClient client, MockNode node, AtomicBoolean hangUpOnRegister, List<String> signIns)
            throws Exception {
        int before = signIns.size();
        node.rejectBindings = true;
        hangUpOnRegister.set(true);
        node.dropConnections();
        assertThatThrownBy(() -> client.system().getClusterMetadata().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class);
        AsyncIggyTcpClientEventLoopGroupTest.await(() -> signIns.size() >= before + 2, "the redial sign-in", RECOVERY);
        List<String> all = List.copyOf(signIns);
        return all.subList(before, all.size());
    }

    /** The body that {@code UsersClient.changePassword} writes. */
    private static byte[] changePasswordBody(UserId user, String currentPassword, String newPassword) {
        return ByteBufUtil.getBytes(BytesSerializer.toBytes(user)
                .writeBytes(BytesSerializer.toBytes(currentPassword, "current password"))
                .writeBytes(BytesSerializer.toBytes(newPassword, "new password")));
    }

    /** The body that {@code UsersClient.updateUser} writes for a rename: a username and no status. */
    private static byte[] renameBody(UserId user, String username) {
        return ByteBufUtil.getBytes(BytesSerializer.toUpdateResource(
                user, UserUpdate.builder().username(username).build()));
    }
}
