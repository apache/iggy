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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Reads the Rust sources of this checkout, which own the wire contract that the SDK mirrors. */
public final class RustSources {

    private RustSources() {}

    /** Reads a file named relative to the repository root, found by walking up from the working directory. */
    public static String read(String relativePath) throws IOException {
        Path source = Path.of(relativePath);
        Path root = Path.of("").toAbsolutePath();
        while (root != null && !Files.isRegularFile(root.resolve(source))) {
            root = root.getParent();
        }
        assertThat(root).as("repository containing %s", source).isNotNull();
        return Files.readString(root.resolve(source));
    }
}
