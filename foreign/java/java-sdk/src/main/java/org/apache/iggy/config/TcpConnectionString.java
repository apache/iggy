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

package org.apache.iggy.config;

import org.apache.iggy.exception.IggyInvalidArgumentException;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A parsed {@code iggy://} connection string for the TCP transport.
 *
 * <p>The format is {@code iggy://<username>:<password>@<host>:<port>?<options>}, where
 * {@code iggy+tcp://} is accepted as well and the host may be an IPv6 address in brackets.
 * The supported options are {@code tls=true|false} and {@code tls_ca_file=<path>}, each at most once.
 * {@code tls_ca_file} requires {@code tls=true}.
 * Error messages never include the connection string, because it contains the password.
 */
public final class TcpConnectionString {

    private static final String DEFAULT_SCHEME = "iggy";
    private static final String SCHEME_PREFIX = "iggy+";
    private static final String TCP_TRANSPORT = "tcp";
    private static final int MAX_PORT = 65535;
    private static final String TLS_OPTION = "tls";
    private static final String TLS_CA_FILE_OPTION = "tls_ca_file";

    // Matches both "[ipv6]:port" and "host:port", where an unbracketed host may not
    // contain a colon. Multi-colon authorities such as "2001:db8::1:8090" are rejected,
    // and so are hosts with whitespace or control characters.
    private static final Pattern ADDRESS =
            Pattern.compile("^(?:\\[([^\\]\\s\\p{Cc}]+)\\]|([^:\\[\\]\\s\\p{Cc}]+)):(\\d{1,5})$");

    // A transport name is echoed in errors only when it is plain letters, because a
    // malformed scheme can contain the credentials.
    private static final Pattern TRANSPORT_NAME = Pattern.compile("^[A-Za-z]+$");

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final Boolean tls;
    private final String tlsCaFile;

    private TcpConnectionString(
            String host, int port, String username, String password, Boolean tls, String tlsCaFile) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.tls = tls;
        this.tlsCaFile = tlsCaFile;
    }

    /**
     * Returns the server host.
     *
     * @return the server host
     */
    public String host() {
        return host;
    }

    /**
     * Returns the server port.
     *
     * @return the server port
     */
    public int port() {
        return port;
    }

    /**
     * Returns the username to log in with.
     *
     * @return the username
     */
    public String username() {
        return username;
    }

    /**
     * Returns the password to log in with.
     *
     * @return the password
     */
    public String password() {
        return password;
    }

    /**
     * Returns the value of the {@code tls} option.
     *
     * @return whether TLS is enabled, or empty if the connection string does not set {@code tls}
     */
    public Optional<Boolean> tls() {
        return Optional.ofNullable(tls);
    }

    /**
     * Returns the value of the {@code tls_ca_file} option.
     *
     * @return the path of the PEM file that validates the server certificate, or empty if it is not set
     */
    public Optional<String> tlsCaFile() {
        return Optional.ofNullable(tlsCaFile);
    }

    /**
     * Parses a TCP connection string.
     *
     * @param connectionString the connection string
     * @return the parsed connection string
     * @throws IggyInvalidArgumentException if the connection string is invalid or uses an unsupported option
     */
    public static TcpConnectionString parse(String connectionString) {
        if (connectionString == null || connectionString.isEmpty()) {
            throw new IggyInvalidArgumentException("Connection string must not be empty");
        }

        String[] schemeAndRest = connectionString.split("://", -1);
        if (schemeAndRest.length != 2) {
            throw invalid("expected <scheme>://<username>:<password>@<host>:<port>");
        }
        parseScheme(schemeAndRest[0]);

        String[] credentialsAndServer = schemeAndRest[1].split("@", -1);
        if (credentialsAndServer.length != 2) {
            throw invalid("expected exactly one '@' between the credentials and the server address");
        }
        String[] credentials = parseCredentials(credentialsAndServer[0]);

        String[] addressAndOptions = credentialsAndServer[1].split("\\?", -1);
        if (addressAndOptions.length > 2) {
            throw invalid("expected at most one '?' before the options");
        }
        Address address = parseAddress(addressAndOptions[0]);
        Options options = addressAndOptions.length == 2 ? parseOptions(addressAndOptions[1]) : Options.NONE;

        return new TcpConnectionString(
                address.host(), address.port(), credentials[0], credentials[1], options.tls(), options.tlsCaFile());
    }

    private static void parseScheme(String scheme) {
        if (DEFAULT_SCHEME.equals(scheme)) {
            return;
        }
        if (!scheme.startsWith(SCHEME_PREFIX)) {
            throw invalid("scheme must be iggy:// or iggy+tcp://");
        }
        String transport = scheme.substring(SCHEME_PREFIX.length());
        if (TCP_TRANSPORT.equals(transport)) {
            return;
        }
        if (!TRANSPORT_NAME.matcher(transport).matches()) {
            throw invalid("scheme must be iggy:// or iggy+tcp://");
        }
        throw new IggyInvalidArgumentException(
                "Unsupported transport '" + transport + "' in connection string, only tcp is supported");
    }

    private static String[] parseCredentials(String credentials) {
        String[] usernameAndPassword = credentials.split(":", -1);
        if (usernameAndPassword.length == 1) {
            throw invalid("personal access tokens are not supported, use <username>:<password>");
        }
        if (usernameAndPassword.length != 2 || usernameAndPassword[0].isEmpty() || usernameAndPassword[1].isEmpty()) {
            throw invalid("credentials must be <username>:<password>");
        }
        return usernameAndPassword;
    }

    private static Address parseAddress(String serverAddress) {
        Matcher address = ADDRESS.matcher(serverAddress);
        if (!address.matches()) {
            throw invalid("server address must be <host>:<port> or [<ipv6>]:<port>");
        }
        int port = Integer.parseInt(address.group(3));
        if (port > MAX_PORT) {
            throw invalid("port must be at most " + MAX_PORT);
        }
        String host = address.group(1) != null ? address.group(1) : address.group(2);
        return new Address(host, port);
    }

    private static Options parseOptions(String options) {
        Boolean tls = null;
        String tlsCaFile = null;
        Set<String> seen = new HashSet<>();
        for (String option : options.split("&", -1)) {
            String[] nameAndValue = option.split("=", -1);
            if (nameAndValue.length != 2) {
                throw invalid("options must be <name>=<value> pairs separated by '&'");
            }
            String name = nameAndValue[0];
            String value = nameAndValue[1];
            if (TLS_OPTION.equals(name)) {
                tls = parseBoolean(name, value);
            } else if (TLS_CA_FILE_OPTION.equals(name)) {
                if (value.isBlank()) {
                    throw invalid("option 'tls_ca_file' must not be blank");
                }
                tlsCaFile = value;
            } else {
                // Rejected rather than ignored, so that an option is never silently dropped.
                throw new IggyInvalidArgumentException("Unsupported option '" + name
                        + "' in connection string, only tls and tls_ca_file are supported");
            }
            if (!seen.add(name)) {
                throw invalid("option '" + name + "' must not be set more than once");
            }
        }
        if (tlsCaFile != null && !Boolean.TRUE.equals(tls)) {
            // Without TLS the client would connect in plaintext and ignore the CA file.
            throw invalid("option 'tls_ca_file' requires tls=true");
        }
        return new Options(tls, tlsCaFile);
    }

    private static boolean parseBoolean(String name, String value) {
        if (!"true".equals(value) && !"false".equals(value)) {
            throw invalid("option '" + name + "' must be true or false");
        }
        return "true".equals(value);
    }

    /** Keeps the password out of logs. */
    @Override
    public String toString() {
        return "TcpConnectionString[host=" + host + ", port=" + port + ", username=" + username + ", password=***, tls="
                + tls + ", tlsCaFile=" + tlsCaFile + "]";
    }

    private static IggyInvalidArgumentException invalid(String reason) {
        return new IggyInvalidArgumentException("Invalid connection string: " + reason);
    }

    private record Address(String host, int port) {}

    private record Options(Boolean tls, String tlsCaFile) {
        private static final Options NONE = new Options(null, null);
    }
}
