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

package org.apache.iggy.client.blocking;

import org.apache.iggy.user.GlobalPermissions;
import org.apache.iggy.user.IdentityInfo;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.UserInfo;
import org.apache.iggy.user.UserInfoDetails;
import org.apache.iggy.user.UserStatus;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class UsersClientBaseTest extends IntegrationTest {

    protected UsersClient usersClient;

    @BeforeEach
    void beforeEachBase() {
        usersClient = client.users();
    }

    @Test
    void shouldLogin() {
        // when
        var identityInfo = usersClient.login("iggy", "iggy");

        // then
        assertThat(identityInfo).isNotNull();
        assertThat(identityInfo.userId()).isEqualTo(0L);
    }

    @Test
    void shouldGetUser() {
        // given
        login();

        // when
        var user = usersClient.getUser(0L);

        // then
        assertThat(user).isPresent();
    }

    @Test
    void shouldCreateAndDeleteUser() {
        // given
        login();

        // when
        var createdUser = usersClient.createUser(
                "test",
                "test",
                UserStatus.Active,
                Optional.of(new Permissions(createGlobalPermissions(true), Collections.emptyMap())));
        trackUser(createdUser.id());

        // then
        assertThat(createdUser).isNotNull();
        assertThat(createdUser
                        .permissions()
                        .map(Permissions::global)
                        .map(GlobalPermissions::manageServers)
                        .orElse(false))
                .isTrue();

        // when
        List<UserInfo> users = usersClient.getUsers();

        // then
        assertThat(users).hasSize(2);
        assertThat(users).map(UserInfo::username).containsExactlyInAnyOrder("iggy", "test");

        // when
        usersClient.deleteUser(createdUser.id());

        // then
        users = usersClient.getUsers();
        assertThat(users).hasSize(1);
    }

    @Test
    void shouldUpdateUserStatus() {
        // given
        login();
        UserInfoDetails user = usersClient.createUser("test", "test", UserStatus.Active, Optional.empty());
        trackUser(user.id());

        // when
        usersClient.updateUser(user.id(), Optional.empty(), Optional.of(UserStatus.Inactive));

        // then
        List<UserInfo> users = usersClient.getUsers();
        assertThat(users).map(UserInfo::status).contains(UserStatus.Inactive);
    }

    @Test
    void shouldUpdateUserPermissions() {
        // given
        var permissions = new Permissions(createGlobalPermissions(true), Collections.emptyMap());
        login();
        UserInfoDetails user = usersClient.createUser("test", "test", UserStatus.Active, Optional.of(permissions));
        trackUser(user.id());

        // when
        usersClient.updatePermissions(
                user.id(), Optional.of(new Permissions(createGlobalPermissions(false), Collections.emptyMap())));

        // then
        var updatedUser = usersClient.getUser(user.id());
        assertThat(updatedUser).isPresent();
        assertThat(updatedUser
                        .get()
                        .permissions()
                        .map(Permissions::global)
                        .map(GlobalPermissions::manageServers)
                        .orElse(true))
                .isFalse();
    }

    @Test
    void shouldChangePassword() {
        // given
        IdentityInfo identity = usersClient.login("iggy", "iggy");

        // when
        usersClient.changePassword(identity.userId(), "iggy", "new-pass");
        usersClient.logout();
        IdentityInfo newLogin = usersClient.login("iggy", "new-pass");

        // then
        assertThat(newLogin).isNotNull();

        // restore original password for other tests
        usersClient.changePassword(identity.userId(), "new-pass", "iggy");
    }

    @Test
    void shouldReturnEmptyForNonExistingUser() {
        // given
        login();

        // when
        var user = usersClient.getUser(404L);

        // then
        assertThat(user).isEmpty();
    }

    private static @NotNull GlobalPermissions createGlobalPermissions(boolean manageServers) {
        return new GlobalPermissions(manageServers, false, false, false, false, false, false, false, false, false);
    }
}
