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

package org.apache.iggy.builder;

import org.apache.iggy.exception.IggyInvalidArgumentException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A parsed {@code iggy://} connection string for the TCP transport.
 *
 * <p>Follows {@code parseConnectionString} in the Node SDK, limited to what the Java
 * TCP builders support: {@code username:password} credentials and the {@code tls} option.
 * Error messages never include the connection string, because it contains the password.
 */
record TcpConnectionString(String host, int port, String username, String password, boolean tls) {

    private static final String DEFAULT_SCHEME = "iggy";
    private static final String SCHEME_PREFIX = "iggy+";
    private static final String TCP_TRANSPORT = "tcp";
    private static final int MAX_PORT = 65535;

    // Matches both "[ipv6]:port" and "host:port", where an unbracketed host may not
    // contain a colon. Multi-colon authorities such as "2001:db8::1:8090" are rejected.
    private static final Pattern ADDRESS = Pattern.compile("^(?:\\[([^\\]]+)\\]|([^:\\[\\]]+)):(\\d{1,5})$");

    static TcpConnectionString parse(String connectionString) {
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
        Matcher address = parseAddress(addressAndOptions[0]);
        String host = address.group(1) != null ? address.group(1) : address.group(2);
        int port = Integer.parseInt(address.group(3));

        boolean tls = addressAndOptions.length == 2 && parseTlsOption(addressAndOptions[1]);
        return new TcpConnectionString(host, port, credentials[0], credentials[1], tls);
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

    private static Matcher parseAddress(String serverAddress) {
        Matcher address = ADDRESS.matcher(serverAddress);
        if (!address.matches()) {
            throw invalid("server address must be <host>:<port> or [<ipv6>]:<port>");
        }
        if (Integer.parseInt(address.group(3)) > MAX_PORT) {
            throw invalid("port must be at most " + MAX_PORT);
        }
        return address;
    }

    private static void parseScheme(String scheme) {
        if (DEFAULT_SCHEME.equals(scheme)) {
            return;
        }
        if (!scheme.startsWith(SCHEME_PREFIX)) {
            throw invalid("scheme must be iggy:// or iggy+tcp://");
        }
        String transport = scheme.substring(SCHEME_PREFIX.length());
        if (!TCP_TRANSPORT.equals(transport)) {
            throw new IggyInvalidArgumentException(
                    "Unsupported transport '" + transport + "' in connection string, only tcp is supported");
        }
    }

    private static boolean parseTlsOption(String options) {
        boolean tls = false;
        for (String option : options.split("&", -1)) {
            String[] nameAndValue = option.split("=", -1);
            if (nameAndValue.length != 2) {
                throw invalid("options must be <name>=<value> pairs separated by '&'");
            }
            String name = nameAndValue[0];
            String value = nameAndValue[1];
            if (!"tls".equals(name)) {
                // Rejected rather than ignored, so that a string that configures more
                // in the Rust SDK never silently behaves differently here.
                throw new IggyInvalidArgumentException(
                        "Unsupported option '" + name + "' in connection string, only tls is supported");
            }
            if (!"true".equals(value) && !"false".equals(value)) {
                throw invalid("option 'tls' must be true or false");
            }
            tls = "true".equals(value);
        }
        return tls;
    }

    /** Keeps the password out of logs; the generated record toString would print it. */
    @Override
    public String toString() {
        return "TcpConnectionString[host=" + host + ", port=" + port + ", username=" + username + ", password=***, tls="
                + tls + "]";
    }

    private static IggyInvalidArgumentException invalid(String reason) {
        return new IggyInvalidArgumentException("Invalid connection string: " + reason);
    }
}
