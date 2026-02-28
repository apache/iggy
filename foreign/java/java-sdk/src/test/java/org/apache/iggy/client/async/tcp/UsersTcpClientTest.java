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

import org.apache.iggy.client.BaseIntegrationTest;
import org.apache.iggy.identifier.UserId;
import org.apache.iggy.user.GlobalPermissions;
import org.apache.iggy.user.IdentityInfo;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.UserInfo;
import org.apache.iggy.user.UserStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class UsersTcpClientTest extends BaseIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(UsersTcpClientTest.class);
    private static final String USERNAME = "iggy";
    private static final String PASSWORD = "iggy";

    private static AsyncIggyTcpClient client;
    private static IdentityInfo loggedInUser;

    @BeforeAll
    public static void setup() throws Exception {
        log.info("Setting up async client for integration tests");
        client = new AsyncIggyTcpClient(serverHost(), serverTcpPort());

        // Connect and login
        loggedInUser = client.connect()
                .thenCompose(v -> {
                    log.info("Connected to Iggy server");
                    return client.users().login(USERNAME, PASSWORD);
                })
                .get(5, TimeUnit.SECONDS);

        log.info("Successfully logged in as: {}", USERNAME);
    }

    @Test
    void shouldLogIn() {
        assertThat(loggedInUser).isNotNull();
        assertThat(loggedInUser.userId()).isEqualTo(0L);
    }

    @Test
    void shouldGetUserWhenUserExists() throws Exception {
        var userDetails = client.users().getUser(0L).get(5, TimeUnit.SECONDS);

        assertThat(userDetails).isPresent();
        assertThat(userDetails.get().id()).isEqualTo(0L);
        assertThat(userDetails.get().username()).isEqualTo(USERNAME);
    }

    @Test
    void shouldGetEmptyOptionalWhenUserDoesNotExist() throws Exception {
        var userDetails = client.users().getUser(123456L).get(5, TimeUnit.SECONDS);

        assertThat(userDetails).isNotPresent();
    }

    @Test
    void shouldGetUsers() throws Exception {
        var users = client.users().getUsers().get(5, TimeUnit.SECONDS);

        assertThat(users).isNotEmpty();
        assertThat(users).hasSize(1);
        assertThat(users.get(0).username()).isEqualTo(USERNAME);
    }

    @Test
    void shouldCreateAndDeleteUsers() throws Exception {
        var users = client.users().getUsers().get(5, TimeUnit.SECONDS);
        assertThat(users).hasSize(1);

        var globalPermissions =
                new GlobalPermissions(true, false, false, false, false, false, false, false, false, false);
        var permissions = Optional.of(new Permissions(globalPermissions, Map.of()));

        var foo = client.users()
                .createUser("foo", "foo", UserStatus.Active, permissions)
                .get(5, TimeUnit.SECONDS);
        assertThat(foo).isNotNull();
        assertThat(foo.permissions()).isPresent();
        assertThat(foo.permissions().get().global()).isEqualTo(globalPermissions);

        var bar = client.users()
                .createUser("bar", "bar", UserStatus.Active, permissions)
                .get(5, TimeUnit.SECONDS);
        assertThat(bar).isNotNull();
        assertThat(bar.permissions()).isPresent();
        assertThat(bar.permissions().get().global()).isEqualTo(globalPermissions);

        users = client.users().getUsers().get(5, TimeUnit.SECONDS);

        assertThat(users).hasSize(3);
        assertThat(users).map(UserInfo::username).containsExactlyInAnyOrder(USERNAME, "foo", "bar");

        client.users().deleteUser(foo.id()).get(5, TimeUnit.SECONDS);
        users = client.users().getUsers().get(5, TimeUnit.SECONDS);
        assertThat(users).hasSize(2);

        client.users().deleteUser(UserId.of(bar.id())).get(5, TimeUnit.SECONDS);
        users = client.users().getUsers().get(5, TimeUnit.SECONDS);
        assertThat(users).hasSize(1);
    }

    @Test
    void shouldUpdateUser() throws Exception {
        var created = client.users()
                .createUser("test", "test", UserStatus.Active, Optional.empty())
                .get(5, TimeUnit.SECONDS);

        client.users()
                .updateUser(created.id(), Optional.of("foo"), Optional.of(UserStatus.Inactive))
                .get(5, TimeUnit.SECONDS);

        var user = client.users().getUser(created.id()).get(5, TimeUnit.SECONDS);
        assertThat(user).isPresent();
        assertThat(user.get().username()).isEqualTo("foo");
        assertThat(user.get().status()).isEqualTo(UserStatus.Inactive);

        client.users()
                .updateUser(created.id(), Optional.empty(), Optional.of(UserStatus.Active))
                .get(5, TimeUnit.SECONDS);

        user = client.users().getUser(created.id()).get(5, TimeUnit.SECONDS);
        assertThat(user).isPresent();
        assertThat(user.get().username()).isEqualTo("foo");
        assertThat(user.get().status()).isEqualTo(UserStatus.Active);

        client.users()
                .updateUser(UserId.of(created.id()), Optional.of("test"), Optional.empty())
                .get(5, TimeUnit.SECONDS);

        user = client.users().getUser(created.id()).get(5, TimeUnit.SECONDS);
        assertThat(user).isPresent();
        assertThat(user.get().username()).isEqualTo("test");
        assertThat(user.get().status()).isEqualTo(UserStatus.Active);

        client.users().deleteUser(created.id()).get(5, TimeUnit.SECONDS);

        var users = client.users().getUsers().get(5, TimeUnit.SECONDS);
        assertThat(users).hasSize(1);
    }

    @Test
    void shouldUpdatePermissions() throws Exception {
        var created = client.users()
                .createUser("test", "test", UserStatus.Active, Optional.empty())
                .get(5, TimeUnit.SECONDS);

        var allPermissions = new Permissions(
                new GlobalPermissions(true, true, true, true, true, true, true, true, true, true), Map.of());
        var noPermissions = new Permissions(
                new GlobalPermissions(false, false, false, false, false, false, false, false, false, false), Map.of());

        client.users()
                .updatePermissions(created.id(), Optional.of(allPermissions))
                .get(5, TimeUnit.SECONDS);

        var user = client.users().getUser(created.id()).get(5, TimeUnit.SECONDS);
        assertThat(user).isPresent();
        assertThat(user.get().permissions()).isPresent();
        assertThat(user.get().permissions().get()).isEqualTo(allPermissions);

        client.users()
                .updatePermissions(UserId.of(created.id()), Optional.of(noPermissions))
                .get(5, TimeUnit.SECONDS);

        user = client.users().getUser(created.id()).get(5, TimeUnit.SECONDS);
        assertThat(user).isPresent();
        assertThat(user.get().permissions()).isPresent();
        assertThat(user.get().permissions().get()).isEqualTo(noPermissions);

        client.users().deleteUser(created.id()).get(5, TimeUnit.SECONDS);

        var users = client.users().getUsers().get(5, TimeUnit.SECONDS);
        assertThat(users).hasSize(1);
    }

    @Test
    void shouldChangePassword() throws Exception {
        var newUser = client.users()
                .createUser("test", "test", UserStatus.Active, Optional.empty())
                .get(5, TimeUnit.SECONDS);
        client.users().logout().get(5, TimeUnit.SECONDS);

        var identity = client.users().login("test", "test").get(5, TimeUnit.SECONDS);
        assertThat(identity).isNotNull();
        assertThat(identity.userId()).isEqualTo(newUser.id());

        client.users().changePassword(identity.userId(), "test", "foobar").get(5, TimeUnit.SECONDS);
        client.users().logout().get(5, TimeUnit.SECONDS);
        identity = client.users().login("test", "foobar").get(5, TimeUnit.SECONDS);
        assertThat(identity).isNotNull();
        assertThat(identity.userId()).isEqualTo(newUser.id());

        client.users()
                .changePassword(UserId.of(identity.userId()), "foobar", "barfoo")
                .get(5, TimeUnit.SECONDS);
        client.users().logout().get(5, TimeUnit.SECONDS);
        identity = client.users().login("test", "barfoo").get(5, TimeUnit.SECONDS);
        assertThat(identity).isNotNull();
        assertThat(identity.userId()).isEqualTo(newUser.id());

        client.users().logout().get(5, TimeUnit.SECONDS);
        client.users().login(USERNAME, PASSWORD).get(5, TimeUnit.SECONDS);
        client.users().deleteUser(newUser.id()).get(5, TimeUnit.SECONDS);

        var users = client.users().getUsers().get(5, TimeUnit.SECONDS);
        assertThat(users).hasSize(1);
    }
}
