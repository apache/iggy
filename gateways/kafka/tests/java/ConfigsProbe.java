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

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidConfigurationException;

/**
 * Kafka 3.9 AdminClient probe for DescribeConfigs and the deprecated AlterConfigs API.
 * Prints elapsed time for 50 describes and does not fail on a latency threshold.
 */
public final class ConfigsProbe {
    private ConfigsProbe() {}

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: ConfigsProbe <bootstrap> <topic>");
        }
        String bootstrap = args[0];
        String topic = args[1];
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "20000");
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "iggy-configs-probe");

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        try (AdminClient admin = AdminClient.create(props)) {
            Config before = describe(admin, resource);
            ConfigEntry retention = before.get("retention.ms");
            ConfigEntry cleanup = before.get("cleanup.policy");
            require(retention != null && "-1".equals(retention.value()), "initial retention.ms");
            require(
                    retention.source() == ConfigEntry.ConfigSource.DEFAULT_CONFIG,
                    "initial retention source");
            require(!retention.isReadOnly(), "retention.ms is writable");
            require(cleanup != null && "delete".equals(cleanup.value()), "cleanup.policy");
            require(cleanup.isReadOnly(), "cleanup.policy is read only");
            require(
                    cleanup.source() == ConfigEntry.ConfigSource.DEFAULT_CONFIG,
                    "cleanup source");
            System.out.println(
                    "retention.ms=" + retention.value() + " source=" + retention.source());
            System.out.println(
                    "cleanup.policy="
                            + cleanup.value()
                            + " read_only="
                            + cleanup.isReadOnly()
                            + " source="
                            + cleanup.source());

            try {
                admin.alterConfigs(Map.of(resource, config("no.such", "1"))).all().get();
                throw new AssertionError("unknown key was accepted");
            } catch (ExecutionException exception) {
                if (!(exception.getCause() instanceof InvalidConfigurationException)) {
                    throw exception;
                }
                System.out.println("unknown_key_rejected=" + exception.getCause().getClass().getSimpleName());
            }

            Config stillDefault = describe(admin, resource);
            require("-1".equals(stillDefault.get("retention.ms").value()), "unknown key wrote retention");

            admin.alterConfigs(Map.of(resource, config("retention.ms", "8000")), new AlterConfigsOptions().validateOnly(true))
                    .all()
                    .get();
            Config validated = describe(admin, resource);
            require("-1".equals(validated.get("retention.ms").value()), "validate_only wrote retention");
            System.out.println("validate_only_retention.ms=" + validated.get("retention.ms").value());

            admin.alterConfigs(Map.of(resource, config("retention.ms", "8000"))).all().get();
            Config altered = describe(admin, resource);
            ConfigEntry changed = altered.get("retention.ms");
            require("8000".equals(changed.value()), "altered retention.ms");
            require(
                    changed.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG,
                    "altered retention source");
            System.out.println("altered_retention.ms=" + changed.value() + " source=" + changed.source());

            long started = System.nanoTime();
            for (int i = 0; i < 50; i++) {
                describe(admin, resource);
            }
            long elapsedMs = (System.nanoTime() - started) / 1_000_000L;
            System.out.println("describe_batch_50_elapsed_ms=" + elapsedMs);
            System.out.println("CONFIGS_PROBE_OK");
        }
    }

    private static Config describe(AdminClient admin, ConfigResource resource) throws Exception {
        return admin.describeConfigs(List.of(resource)).all().get().get(resource);
    }

    private static Config config(String name, String value) {
        return new Config(List.of(new ConfigEntry(name, value)));
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
