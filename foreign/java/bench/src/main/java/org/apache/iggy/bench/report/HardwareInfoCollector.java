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

package org.apache.iggy.bench.report;

import org.apache.iggy.bench.models.report.context.BenchmarkHardware;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public final class HardwareInfoCollector {
    private static final long BYTES_PER_MEGABYTE = 1024L * 1024L;
    private static final Path CPU_INFO_PATH = Path.of("/proc/cpuinfo");
    private static final Path OS_RELEASE_PATH = Path.of("/etc/os-release");
    private static final Path KERNEL_HOSTNAME_PATH = Path.of("/proc/sys/kernel/hostname");

    public BenchmarkHardware collect() {
        return collect(Optional.empty());
    }

    public BenchmarkHardware collect(Optional<String> identifier) {
        long totalMemoryBytes = 0L;
        if (ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean bean) {
            totalMemoryBytes = bean.getTotalMemorySize();
        }

        return new BenchmarkHardware(
                identifier.filter(value -> !value.isBlank()).or(this::detectHostname),
                detectCpuName(),
                detectCpuCores(),
                totalMemoryBytes / BYTES_PER_MEGABYTE,
                detectOsName(),
                System.getProperty("os.version"));
    }

    private String detectCpuName() {
        try {
            for (String line : Files.readAllLines(CPU_INFO_PATH)) {
                if (line.startsWith("model name")) {
                    int separator = line.indexOf(':');
                    if (separator >= 0) {
                        return line.substring(separator + 1).trim();
                    }
                }
            }
        } catch (IOException ignored) {
        }

        return System.getProperty("os.arch");
    }

    private String detectOsName() {
        try {
            for (String line : Files.readAllLines(OS_RELEASE_PATH)) {
                if (line.startsWith("NAME=")) {
                    String value = line.substring("NAME=".length()).trim();
                    if (value.startsWith("\"") && value.endsWith("\"")) {
                        value = value.substring(1, value.length() - 1);
                    }
                    return value;
                }
            }
        } catch (IOException ignored) {
        }

        return System.getProperty("os.name");
    }

    private int detectCpuCores() {
        try {
            List<String> lines = Files.readAllLines(CPU_INFO_PATH);
            int cpuCount = 0;
            for (String line : lines) {
                if (line.startsWith("processor")) {
                    cpuCount++;
                }
            }
            if (cpuCount > 0) {
                return cpuCount;
            }
        } catch (IOException ignored) {
        }

        return Runtime.getRuntime().availableProcessors();
    }

    private Optional<String> detectHostname() {
        try {
            String hostname = Files.readString(KERNEL_HOSTNAME_PATH).trim();
            return hostname.isBlank() ? Optional.empty() : Optional.of(hostname);
        } catch (IOException ignored) {
            return Optional.empty();
        }
    }
}
