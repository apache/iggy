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

package org.apache.iggy;

import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.serde.CommandCode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the hand-kept command and error codes against the Rust sources of this checkout, so a code that
 * changes on either side fails here instead of on the wire.
 */
class ProtocolParityTest {

    private static final String CODES_PATH = "core/binary_protocol/src/codes.rs";
    private static final String ERRORS_PATH = "core/common/src/error/iggy_error.rs";
    private static final Pattern RUST_COMMAND_CODE =
            Pattern.compile("(?m)^pub const ([A-Z0-9_]+_CODE): u32 = ([0-9_]+);$");
    private static final Pattern RUST_RETIRED_CODES =
            Pattern.compile("const RETIRED_CODES: &\\[u32\\] = &\\[([0-9_,\\s]+)\\];");
    private static final Pattern RUST_ERROR_ENUM = Pattern.compile("(?ms)^pub enum IggyError \\{$(.*?)^\\}$");
    // A unit, tuple or struct variant at the indent of the enum body, up to its explicit discriminant.
    private static final Pattern RUST_ERROR_VARIANT =
            Pattern.compile("(?ms)^ {4}([A-Z][A-Za-z0-9]*)(?:\\([^\\n]*\\)| \\{.*?^ {4}\\})? = ([0-9_]+),$");
    // The SDK has no snapshot or segment API, so it never sends these.
    private static final Set<String> UNSENT_COMMAND_CODES = Set.of("GET_SNAPSHOT_FILE_CODE", "DELETE_SEGMENTS_CODE");
    // The SDK names its command codes otherwise than Rust, and only a comparison by name finds two swapped codes.
    private static final Map<CommandCode, String> RUST_COMMAND_NAMES = Map.ofEntries(
            Map.entry(CommandCode.System.PING, "PING_CODE"),
            Map.entry(CommandCode.System.GET_STATS, "GET_STATS_CODE"),
            Map.entry(CommandCode.System.GET_CLUSTER_METADATA, "GET_CLUSTER_METADATA_CODE"),
            Map.entry(CommandCode.System.DESCRIBE_OPTIONS, "DESCRIBE_OPTIONS_CODE"),
            Map.entry(CommandCode.System.BIND_SESSION, "BIND_SESSION_CODE"),
            Map.entry(CommandCode.System.UPDATE_RESOURCE, "UPDATE_RESOURCE_CODE"),
            Map.entry(CommandCode.System.GET_ME, "GET_ME_CODE"),
            Map.entry(CommandCode.System.GET_CLIENT, "GET_CLIENT_CODE"),
            Map.entry(CommandCode.System.GET_ALL_CLIENTS, "GET_CLIENTS_CODE"),
            Map.entry(CommandCode.User.GET, "GET_USER_CODE"),
            Map.entry(CommandCode.User.GET_ALL, "GET_USERS_CODE"),
            Map.entry(CommandCode.User.CREATE, "CREATE_USER_CODE"),
            Map.entry(CommandCode.User.DELETE, "DELETE_USER_CODE"),
            Map.entry(CommandCode.User.UPDATE_PERMISSIONS, "UPDATE_PERMISSIONS_CODE"),
            Map.entry(CommandCode.User.CHANGE_PASSWORD, "CHANGE_PASSWORD_CODE"),
            Map.entry(CommandCode.User.LOGIN, "LOGIN_USER_CODE"),
            Map.entry(CommandCode.User.LOGOUT, "LOGOUT_USER_CODE"),
            Map.entry(CommandCode.User.LOGIN_REGISTER, "LOGIN_REGISTER_CODE"),
            Map.entry(CommandCode.PersonalAccessToken.GET_ALL, "GET_PERSONAL_ACCESS_TOKENS_CODE"),
            Map.entry(CommandCode.PersonalAccessToken.CREATE, "CREATE_PERSONAL_ACCESS_TOKEN_CODE"),
            Map.entry(CommandCode.PersonalAccessToken.DELETE, "DELETE_PERSONAL_ACCESS_TOKEN_CODE"),
            Map.entry(CommandCode.PersonalAccessToken.LOGIN, "LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE"),
            Map.entry(CommandCode.PersonalAccessToken.LOGIN_REGISTER, "LOGIN_REGISTER_WITH_PAT_CODE"),
            Map.entry(CommandCode.Messages.POLL, "POLL_MESSAGES_CODE"),
            Map.entry(CommandCode.Messages.SEND, "SEND_MESSAGES_CODE"),
            Map.entry(CommandCode.Messages.GET_PARTITION_ROUTE, "GET_PARTITION_ROUTE_CODE"),
            Map.entry(CommandCode.ConsumerOffset.GET, "GET_CONSUMER_OFFSET_CODE"),
            Map.entry(CommandCode.ConsumerOffset.STORE, "STORE_CONSUMER_OFFSET_CODE"),
            Map.entry(CommandCode.ConsumerOffset.DELETE, "DELETE_CONSUMER_OFFSET_CODE"),
            Map.entry(CommandCode.Stream.GET, "GET_STREAM_CODE"),
            Map.entry(CommandCode.Stream.GET_ALL, "GET_STREAMS_CODE"),
            Map.entry(CommandCode.Stream.CREATE, "CREATE_STREAM_CODE"),
            Map.entry(CommandCode.Stream.DELETE, "DELETE_STREAM_CODE"),
            Map.entry(CommandCode.Topic.GET, "GET_TOPIC_CODE"),
            Map.entry(CommandCode.Topic.GET_ALL, "GET_TOPICS_CODE"),
            Map.entry(CommandCode.Topic.CREATE, "CREATE_TOPIC_CODE"),
            Map.entry(CommandCode.Topic.DELETE, "DELETE_TOPIC_CODE"),
            Map.entry(CommandCode.Partition.CREATE, "CREATE_PARTITIONS_CODE"),
            Map.entry(CommandCode.Partition.DELETE, "DELETE_PARTITIONS_CODE"),
            Map.entry(CommandCode.ConsumerGroup.GET, "GET_CONSUMER_GROUP_CODE"),
            Map.entry(CommandCode.ConsumerGroup.GET_ALL, "GET_CONSUMER_GROUPS_CODE"),
            Map.entry(CommandCode.ConsumerGroup.CREATE, "CREATE_CONSUMER_GROUP_CODE"),
            Map.entry(CommandCode.ConsumerGroup.DELETE, "DELETE_CONSUMER_GROUP_CODE"),
            Map.entry(CommandCode.ConsumerGroup.JOIN, "JOIN_CONSUMER_GROUP_CODE"),
            Map.entry(CommandCode.ConsumerGroup.LEAVE, "LEAVE_CONSUMER_GROUP_CODE"),
            Map.entry(CommandCode.ConsumerGroup.SYNC, "SYNC_CONSUMER_GROUP_CODE"));

    @Test
    void commandCodesMatchTheRustCodes() throws IOException {
        String source = RustSources.read(CODES_PATH);
        Map<String, Integer> rust = namedNumbers(RUST_COMMAND_CODE, source);
        Set<Integer> retired = retiredCodes(source);

        List<String> mismatches = new ArrayList<>();
        sdkCommandCodes().forEach(code -> {
            String sdk = sdkName(code) + " = " + code.getValue();
            String rustName = RUST_COMMAND_NAMES.get(code);
            if (rustName == null) {
                mismatches.add(sdk + " has no entry in RUST_COMMAND_NAMES");
            } else if (retired.contains(code.getValue())) {
                mismatches.add(sdk + " reuses a retired code");
            } else if (!Integer.valueOf(code.getValue()).equals(rust.get(rustName))) {
                mismatches.add(sdk + ", " + rustName + " = " + rust.get(rustName));
            }
        });
        rust.forEach((name, code) -> {
            boolean declared = RUST_COMMAND_NAMES.containsValue(name);
            boolean unsent = UNSENT_COMMAND_CODES.contains(name);
            if (!declared && !unsent) {
                mismatches.add(name + " = " + code + " is missing from CommandCode");
            } else if (declared && unsent) {
                mismatches.add(name + " = " + code + " is in CommandCode and in UNSENT_COMMAND_CODES");
            }
        });

        assertThat(mismatches)
                .as("command codes that differ from %s", CODES_PATH)
                .isEmpty();
        assertThat(RUST_COMMAND_NAMES.values())
                .as("Rust names in RUST_COMMAND_NAMES")
                .doesNotHaveDuplicates();
        assertThat(rust.keySet())
                .as("unsent codes that %s declares", CODES_PATH)
                .containsAll(UNSENT_COMMAND_CODES);
    }

    @Test
    void errorCodesMatchTheRustVariants() throws IOException {
        Matcher body = RUST_ERROR_ENUM.matcher(RustSources.read(ERRORS_PATH));
        assertThat(body.find()).as("pub enum IggyError in %s", ERRORS_PATH).isTrue();
        Map<String, Integer> rust = namedNumbers(RUST_ERROR_VARIANT, body.group(1));
        Map<String, String> variants =
                rust.keySet().stream().collect(Collectors.toMap(ProtocolParityTest::normalized, Function.identity()));

        List<String> mismatches = new ArrayList<>();
        Arrays.stream(IggyErrorCode.values())
                .filter(code -> code != IggyErrorCode.UNKNOWN)
                .forEach(code -> {
                    String sdk = "IggyErrorCode." + code.name() + " = " + code.getCode();
                    String variant = variants.get(normalized(code.name()));
                    if (variant == null) {
                        mismatches.add(sdk + " has no IggyError variant of that name");
                    } else if (rust.get(variant) != code.getCode()) {
                        mismatches.add(sdk + ", IggyError::" + variant + " = " + rust.get(variant));
                    }
                });
        // The SDK mirrors only some variants, so a code it does not know decodes to UNKNOWN.
        rust.forEach((variant, code) -> {
            IggyErrorCode decoded = IggyErrorCode.fromCode(code);
            if (decoded != IggyErrorCode.UNKNOWN && !normalized(decoded.name()).equals(normalized(variant))) {
                mismatches.add("IggyError::" + variant + " = " + code + " decodes to IggyErrorCode." + decoded.name());
            }
        });

        assertThat(mismatches)
                .as("error codes that differ from %s", ERRORS_PATH)
                .isEmpty();
    }

    private static List<CommandCode> sdkCommandCodes() {
        return Arrays.stream(CommandCode.class.getDeclaredClasses())
                .filter(Class::isEnum)
                .<CommandCode>flatMap(group ->
                        Arrays.stream(group.asSubclass(CommandCode.class).getEnumConstants()))
                .toList();
    }

    private static String sdkName(CommandCode code) {
        Enum<?> constant = (Enum<?>) code;
        return "CommandCode." + constant.getDeclaringClass().getSimpleName() + "." + constant.name();
    }

    private static Set<Integer> retiredCodes(String source) {
        Matcher retired = RUST_RETIRED_CODES.matcher(source);
        assertThat(retired.find()).as("RETIRED_CODES in %s", CODES_PATH).isTrue();
        return Arrays.stream(retired.group(1).split(","))
                .filter(literal -> !literal.isBlank())
                .map(ProtocolParityTest::number)
                .collect(Collectors.toSet());
    }

    private static Map<String, Integer> namedNumbers(Pattern declaration, String source) {
        Map<String, Integer> numbers = declaration
                .matcher(source)
                .results()
                .collect(Collectors.toMap(match -> match.group(1), match -> number(match.group(2))));
        assertThat(numbers).as("declarations matching %s", declaration).isNotEmpty();
        return numbers;
    }

    private static int number(String rustLiteral) {
        return Integer.parseInt(rustLiteral.strip().replace("_", ""));
    }

    private static String normalized(String name) {
        return name.replace("_", "").toLowerCase(Locale.ROOT);
    }
}
