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

import org.apache.iggy.user.IdentityInfo;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class LoginRoutingHookTest {

    private static final IdentityInfo IDENTITY = new IdentityInfo(1L, Optional.empty());

    @Test
    void shouldRouteUsernameLoginBeforeCreatingRegisterAttempt() {
        List<String> events = new ArrayList<>();
        var client = new UsersTcpClient(failingConnectionSupplier(events), routingHook(events));

        var identity = client.login("iggy", "iggy").join();

        assertThat(identity).isEqualTo(IDENTITY);
        assertThat(events).containsExactly("route", "login-attempt");
    }

    @Test
    void shouldRoutePersonalAccessTokenLoginBeforeCreatingRegisterAttempt() {
        List<String> events = new ArrayList<>();
        var client = new PersonalAccessTokensTcpClient(failingConnectionSupplier(events), routingHook(events));

        var identity = client.loginWithPersonalAccessToken("token").join();

        assertThat(identity).isEqualTo(IDENTITY);
        assertThat(events).containsExactly("route", "login-attempt");
    }

    @Test
    void shouldExecuteOneAttemptWithNoOpHook() {
        var attempts = new AtomicInteger();

        var identity = LoginRoutingHook.NONE
                .loginOnLeader(() -> {
                    attempts.incrementAndGet();
                    return CompletableFuture.completedFuture(IDENTITY);
                })
                .join();

        assertThat(identity).isEqualTo(IDENTITY);
        assertThat(attempts).hasValue(1);
    }

    private static Supplier<AsyncTcpConnection> failingConnectionSupplier(List<String> events) {
        return () -> {
            events.add("login-attempt");
            throw new LoginAttemptReached();
        };
    }

    private static LoginRoutingHook routingHook(List<String> events) {
        return loginAttempt -> {
            events.add("route");
            try {
                loginAttempt.get();
            } catch (LoginAttemptReached expected) {
                return CompletableFuture.completedFuture(IDENTITY);
            }
            throw new AssertionError("Expected the login attempt to acquire its connection");
        };
    }

    private static final class LoginAttemptReached extends RuntimeException {}
}
