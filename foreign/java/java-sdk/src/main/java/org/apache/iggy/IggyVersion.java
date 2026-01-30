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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Provides version information for the Iggy Java SDK.
 *
 * <p>Version information is read from a properties file generated at build time.
 */
public final class IggyVersion {

    private static final Logger log = LoggerFactory.getLogger(IggyVersion.class);
    private static final String PROPERTIES_FILE = "/iggy-version.properties";
    private static final String UNKNOWN = "unknown";

    private static final IggyVersion INSTANCE;

    static {
        String version = UNKNOWN;
        String buildTime = UNKNOWN;
        String gitCommit = UNKNOWN;

        try (InputStream is = IggyVersion.class.getResourceAsStream(PROPERTIES_FILE)) {
            if (is != null) {
                Properties props = new Properties();
                props.load(is);
                version = props.getProperty("version", UNKNOWN);
                buildTime = props.getProperty("buildTime", UNKNOWN);
                gitCommit = props.getProperty("gitCommit", UNKNOWN);
            }
        } catch (IOException e) {
            log.warn("Failed to read version information from {}", PROPERTIES_FILE, e);
        }

        INSTANCE = new IggyVersion(version, buildTime, gitCommit);
    }

    private final String version;
    private final String buildTime;
    private final String gitCommit;

    private IggyVersion(String version, String buildTime, String gitCommit) {
        this.version = version;
        this.buildTime = buildTime;
        this.gitCommit = gitCommit;
    }

    /**
     * Gets the singleton IggyVersion instance.
     *
     * @return the version information instance
     */
    public static IggyVersion getInstance() {
        return INSTANCE;
    }

    /**
     * Gets the SDK version string.
     *
     * @return the version string (e.g., "1.0.0")
     */
    public String getVersion() {
        return version;
    }

    /**
     * Gets the build timestamp.
     *
     * @return the build time as ISO-8601 string, or "unknown" if not available
     */
    public String getBuildTime() {
        return buildTime;
    }

    /**
     * Gets the Git commit hash.
     *
     * @return the short Git commit hash, or "unknown" if not available
     */
    public String getGitCommit() {
        return gitCommit;
    }

    /**
     * Gets a User-Agent string suitable for HTTP requests.
     *
     * @return the User-Agent string (e.g., "iggy-java-sdk/1.0.0")
     */
    public String getUserAgent() {
        return "iggy-java-sdk/" + version;
    }

    /**
     * Returns a formatted version string including all available information.
     *
     * @return formatted version string
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Iggy Java SDK ").append(version);
        if (!UNKNOWN.equals(buildTime)) {
            sb.append(" (built: ").append(buildTime);
            if (!UNKNOWN.equals(gitCommit)) {
                sb.append(", commit: ").append(gitCommit);
            }
            sb.append(")");
        }
        return sb.toString();
    }
}
