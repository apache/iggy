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

package org.apache.iggy.user;

import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.system.OptionKey;

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.iggy.serde.BytesSerializer.MAX_USERNAME_LENGTH;
import static org.apache.iggy.serde.BytesSerializer.MIN_USERNAME_LENGTH;

/**
 * What an update changes on a user. Every key left out keeps its stored value.
 *
 * <p>No user key is resettable in this release, so a non-empty {@code reset} is refused before anything is
 * sent, with the error the server would answer with. An update with a raw key or a raw reset name gets only
 * the structural checks, the server decides the rest. Renaming the signed-in user also moves the
 * credentials a TCP client signs back in with after a reconnect.
 *
 * @param username the new username, or empty to keep it
 * @param status the new status, or empty to keep it. Deactivating a user ends its sessions.
 * @param reset keys to return to their defaults
 * @param raw keys this build has no field for, a field wins on collision
 * @param rawReset reset names this build has no key constant for
 */
public record UserUpdate(
        Optional<String> username,
        Optional<UserStatus> status,
        Set<UserOptionKey> reset,
        Map<String, HeaderValue> raw,
        Set<String> rawReset) {

    public UserUpdate {
        reset = copyOf(reset);
        raw = Collections.unmodifiableMap(new LinkedHashMap<>(raw));
        rawReset = Set.copyOf(rawReset);
        boolean typedOnly = raw.isEmpty() && rawReset.isEmpty();
        OptionKey.requireValidUpdate(
                entries(username, status, raw).keySet(),
                reset,
                rawReset,
                typedOnly,
                EnumSet.allOf(UserOptionKey.class));
        if (typedOnly) {
            username.ifPresent(value -> OptionKey.requireTextLength(
                    UserOptionKey.USERNAME.value(),
                    value,
                    MIN_USERNAME_LENGTH,
                    MAX_USERNAME_LENGTH,
                    IggyErrorCode.INVALID_USERNAME));
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The keys this update sets, keyed by option name. */
    public Map<String, HeaderValue> entries() {
        return entries(username, status, raw);
    }

    private static Map<String, HeaderValue> entries(
            Optional<String> username, Optional<UserStatus> status, Map<String, HeaderValue> raw) {
        Map<String, HeaderValue> entries = new LinkedHashMap<>(raw);
        username.ifPresent(value -> entries.put(
                UserOptionKey.USERNAME.value(), OptionKey.textValue(value, MIN_USERNAME_LENGTH, MAX_USERNAME_LENGTH)));
        status.ifPresent(value -> entries.put(
                UserOptionKey.STATUS.value(),
                HeaderValue.fromString(value.name().toLowerCase(Locale.ROOT))));
        return Collections.unmodifiableMap(entries);
    }

    /** The names this update resets, typed and raw together in ascending order. */
    public List<String> resetKeys() {
        return OptionKey.resetNames(reset, rawReset);
    }

    /** Raw entries print by name only, because this build cannot tell which newer keys are secret. */
    @Override
    public String toString() {
        return "UserUpdate[username=" + username + ", status=" + status + ", reset=" + reset + ", raw=" + raw.keySet()
                + ", rawReset=" + rawReset + "]";
    }

    private static Set<UserOptionKey> copyOf(Set<UserOptionKey> keys) {
        Set<UserOptionKey> copy = EnumSet.noneOf(UserOptionKey.class);
        copy.addAll(keys);
        return Collections.unmodifiableSet(copy);
    }

    public static final class Builder {

        private final Set<UserOptionKey> reset = EnumSet.noneOf(UserOptionKey.class);
        private final Map<String, HeaderValue> raw = new LinkedHashMap<>();
        private final Set<String> rawReset = new LinkedHashSet<>();
        private Optional<String> username = Optional.empty();
        private Optional<UserStatus> status = Optional.empty();

        private Builder() {}

        public Builder username(String username) {
            this.username = Optional.of(username);
            return this;
        }

        public Builder status(UserStatus status) {
            this.status = Optional.of(status);
            return this;
        }

        public Builder reset(UserOptionKey key) {
            reset.add(key);
            return this;
        }

        public Builder raw(String key, HeaderValue value) {
            raw.put(key, value);
            return this;
        }

        /** Resets a key this build has no constant for. The server decides whether the reset is allowed. */
        public Builder rawReset(String key) {
            rawReset.add(key);
            return this;
        }

        /**
         * @throws org.apache.iggy.exception.IggyValidationException when the server would refuse the update
         *     for its shape
         */
        public UserUpdate build() {
            return new UserUpdate(username, status, reset, raw, rawReset);
        }
    }
}
