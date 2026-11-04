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

package org.apache.iggy.client.blocking.http;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class InternalHttpClientLoggingTest {

    private static final String ENCRYPTION_KEY = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
    private static final String PASSWORD = "secret-password";

    @Test
    void givenDebugLoggingWhenPreparingBodiesShouldNotLogSecrets() throws Exception {
        var logger = (Logger) LoggerFactory.getLogger(InternalHttpClient.class);
        var previousLevel = logger.getLevel();
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.DEBUG);
        try (var client =
                new InternalHttpClient("http://127.0.0.1:1", Optional.empty(), Optional.empty(), Optional.empty())) {
            client.preparePostRequest(
                    "/streams", Map.of("name", "keyed", "options", Map.of("encryption_key", ENCRYPTION_KEY)));
            client.preparePutRequest("/users/1/password", Map.of("new_password", PASSWORD));
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(previousLevel);
        }

        assertThat(appender.list)
                .extracting(ILoggingEvent::getFormattedMessage)
                .noneMatch(message -> message.contains(ENCRYPTION_KEY) || message.contains(PASSWORD));
    }
}
