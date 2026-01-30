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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IggyVersionTest {

    @Test
    void getInstanceReturnsSingleton() {
        IggyVersion instance1 = IggyVersion.getInstance();
        IggyVersion instance2 = IggyVersion.getInstance();

        assertThat(instance1).isSameAs(instance2);
    }

    @Test
    void getVersionReturnsNonNullValue() {
        IggyVersion version = IggyVersion.getInstance();

        assertThat(version.getVersion()).isNotNull();
        assertThat(version.getVersion()).isNotEmpty();
    }

    @Test
    void getBuildTimeReturnsNonNullValue() {
        IggyVersion version = IggyVersion.getInstance();

        assertThat(version.getBuildTime()).isNotNull();
        assertThat(version.getBuildTime()).isNotEmpty();
    }

    @Test
    void getGitCommitReturnsNonNullValue() {
        IggyVersion version = IggyVersion.getInstance();

        assertThat(version.getGitCommit()).isNotNull();
        assertThat(version.getGitCommit()).isNotEmpty();
    }

    @Test
    void getUserAgentHasCorrectFormat() {
        IggyVersion version = IggyVersion.getInstance();

        String userAgent = version.getUserAgent();

        assertThat(userAgent).isNotNull();
        assertThat(userAgent).startsWith("iggy-java-sdk/");
        assertThat(userAgent).isEqualTo("iggy-java-sdk/" + version.getVersion());
    }

    @Test
    void toStringContainsVersionInfo() {
        IggyVersion version = IggyVersion.getInstance();

        String str = version.toString();

        assertThat(str).isNotNull();
        assertThat(str).startsWith("Iggy Java SDK ");
        assertThat(str).contains(version.getVersion());
    }

    @Test
    void toStringContainsBuildTimeWhenAvailable() {
        IggyVersion version = IggyVersion.getInstance();

        String str = version.toString();

        if (!"unknown".equals(version.getBuildTime())) {
            assertThat(str).contains("built: " + version.getBuildTime());
        }
    }

    @Test
    void toStringContainsGitCommitWhenBuildTimeAvailable() {
        IggyVersion version = IggyVersion.getInstance();

        String str = version.toString();

        if (!"unknown".equals(version.getBuildTime()) && !"unknown".equals(version.getGitCommit())) {
            assertThat(str).contains("commit: " + version.getGitCommit());
        }
    }

    @Test
    void versionValuesArePopulatedFromPropertiesFile() {
        IggyVersion version = IggyVersion.getInstance();

        // In a proper build, all values should be populated (not "unknown")
        assertThat(version.getVersion()).isNotEqualTo("unknown");
        assertThat(version.getBuildTime()).isNotEqualTo("unknown");
        assertThat(version.getGitCommit()).isNotEqualTo("unknown");
    }
}
