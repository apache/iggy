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

import org.apache.commons.lang3.StringUtils;
import org.apache.iggy.exception.IggyInvalidArgumentException;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Utility class for validating HTTP URLs.
 */
final class UrlValidator {

    private static final String HTTP_SCHEME = "http";
    private static final String HTTPS_SCHEME = "https";

    private UrlValidator() {}

    /**
     * Validates that the given string is a valid HTTP or HTTPS URL.
     *
     * @param url the URL string to validate
     * @throws IggyInvalidArgumentException if the URL is null, empty, malformed,
     *         or does not use http/https scheme
     */
    static void validateHttpUrl(String url) {
        if (StringUtils.isBlank(url)) {
            throw new IggyInvalidArgumentException("URL cannot be null or empty");
        }
        try {
            var parsedUrl = new URI(url).toURL();
            String protocol = parsedUrl.getProtocol();
            if (protocol == null || (!protocol.equals(HTTP_SCHEME) && !protocol.equals(HTTPS_SCHEME))) {
                throw new IggyInvalidArgumentException("URL must start with http:// or https://");
            }
        } catch (URISyntaxException | MalformedURLException e) {
            throw new IggyInvalidArgumentException("Invalid URL: " + e.getMessage());
        }
    }
}
