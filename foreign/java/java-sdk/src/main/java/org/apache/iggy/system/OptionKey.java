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

package org.apache.iggy.system;

import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyValidationException;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.message.HeaderValue;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

/**
 * A catalog option key of one resource scope, with the facts {@code describeOptions} reports as flags.
 *
 * <p>The typed update records refuse a patch the server would refuse for its shape before anything is
 * sent, with the same error category the server answers with.
 */
public interface OptionKey {

    /** The longest stream or topic name the server accepts, in UTF-8 bytes. */
    int MAX_NAME_LENGTH = 255;

    /** The most reset names one update carries, the reach of its count byte. */
    int MAX_RESET_KEYS = 255;

    /** The longest option key the wire carries, in UTF-8 bytes. */
    int MAX_KEY_LENGTH = 255;

    /** The key as the server spells it. */
    String value();

    /** The {@link OptionSpec} flag bits of this key. */
    int flags();

    default boolean updatable() {
        return (flags() & OptionSpec.FLAG_UPDATABLE) != 0;
    }

    default boolean resettable() {
        return (flags() & OptionSpec.FLAG_RESETTABLE) != 0;
    }

    default boolean readOnly() {
        return (flags() & OptionSpec.FLAG_READ_ONLY) != 0;
    }

    /**
     * Refuses an update the server would refuse for its shape, in the server's admission order.
     *
     * <p>A patch with a raw set key or a raw reset name gets only the structural checks, because the server's
     * catalog may know keys or flags this build does not. The server decides the rest.
     *
     * @param setKeys the keys the update sets
     * @param reset the typed keys the update returns to their defaults
     * @param rawReset reset names this build has no key constant for
     * @param typedOnly whether every set key and reset name is a typed field of this build
     * @param catalog every key of the update's scope
     * @throws IggyValidationException carrying the server's error code for the refusal
     */
    static void requireValidUpdate(
            Set<String> setKeys,
            Collection<? extends OptionKey> reset,
            Set<String> rawReset,
            boolean typedOnly,
            Collection<? extends OptionKey> catalog) {
        if (setKeys.isEmpty() && reset.isEmpty() && rawReset.isEmpty()) {
            throw refusal(IggyErrorCode.INVALID_COMMAND, "The update sets and resets nothing", Optional.empty());
        }
        requireNone(
                reset,
                key -> setKeys.contains(key.value()),
                IggyErrorCode.INVALID_COMMAND,
                "A key is both set and reset");
        for (String name : rawReset) {
            if (setKeys.contains(name)) {
                throw refusal(IggyErrorCode.INVALID_COMMAND, "A key is both set and reset", Optional.of(name));
            }
        }
        List<String> names = resetNames(reset, rawReset);
        for (String name : names) {
            if (!textFits(name, 1, MAX_KEY_LENGTH)) {
                throw refusal(IggyErrorCode.UNSUPPORTED_OPTION_KEY, "Unsupported option key", Optional.of(name));
            }
        }
        if (names.size() > MAX_RESET_KEYS) {
            // The catalog is far smaller than the limit, so such a list always names a key outside it.
            Optional<String> unknown = names.stream()
                    .filter(name ->
                            catalog.stream().noneMatch(key -> key.value().equals(name)))
                    .findFirst();
            throw refusal(IggyErrorCode.UNSUPPORTED_OPTION_KEY, "Unsupported option key", unknown);
        }
        if (!typedOnly) {
            return;
        }
        requireNone(reset, OptionKey::readOnly, IggyErrorCode.READ_ONLY_OPTION, "Option is read-only");
        requireNone(
                reset,
                key -> !key.updatable(),
                IggyErrorCode.OPTION_NOT_UPDATABLE,
                "Option cannot be updated after creation");
        requireNone(reset, key -> !key.resettable(), IggyErrorCode.OPTION_NOT_RESETTABLE, "Option cannot be reset");
    }

    /**
     * Refuses a text value outside {@code minBytes} to {@code maxBytes} UTF-8 bytes with {@code code}.
     *
     * @throws IggyValidationException carrying {@code code}
     */
    static void requireTextLength(String key, String text, int minBytes, int maxBytes, IggyErrorCode code) {
        if (!textFits(text, minBytes, maxBytes)) {
            throw refusal(
                    code,
                    "Value must be between " + minBytes + " and " + maxBytes + " bytes when UTF-8 encoded",
                    Optional.of(key));
        }
    }

    /**
     * The text as a string value, or a bool the server refuses with the key's own error when the text does not
     * fit, so a patch the server decides still reports the server's category.
     */
    static HeaderValue textValue(String text, int minBytes, int maxBytes) {
        if (!textFits(text, minBytes, maxBytes)) {
            return HeaderValue.fromBool(false);
        }
        return new HeaderValue(HeaderKind.String, text.getBytes(StandardCharsets.UTF_8));
    }

    /** The reset names an update sends, typed and raw together in ascending order without repeats. */
    static List<String> resetNames(Collection<? extends OptionKey> reset, Set<String> rawReset) {
        Set<String> names = new TreeSet<>(rawReset);
        reset.forEach(key -> names.add(key.value()));
        return List.copyOf(names);
    }

    private static boolean textFits(String text, int minBytes, int maxBytes) {
        int length = text.getBytes(StandardCharsets.UTF_8).length;
        return length >= minBytes && length <= maxBytes;
    }

    private static void requireNone(
            Collection<? extends OptionKey> keys, Predicate<OptionKey> refused, IggyErrorCode code, String reason) {
        for (OptionKey key : keys) {
            if (refused.test(key)) {
                throw refusal(code, reason, Optional.of(key.value()));
            }
        }
    }

    private static IggyValidationException refusal(IggyErrorCode code, String reason, Optional<String> key) {
        return new IggyValidationException(code, code.getCode(), reason, key, Optional.empty());
    }
}
