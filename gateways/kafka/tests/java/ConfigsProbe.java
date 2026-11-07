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
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidConfigurationException;

/**
 * Kafka 3.9 AdminClient probe for DescribeConfigs, the deprecated AlterConfigs API and
 * IncrementalAlterConfigs, the only alter API a 4.x client has.
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
            require(retention != null && "-1".equals(retention.value()), "initial retention.ms");
            require(
                    retention.source() == ConfigEntry.ConfigSource.DEFAULT_CONFIG,
                    "initial retention source");
            require(!retention.isReadOnly(), "retention.ms is writable");
            // Kafka Connect refuses an internal topic that reports any cleanup.policy other
            // than compact, and starts when the entry is missing.
            require(before.get("cleanup.policy") == null, "cleanup.policy is not reported");
            System.out.println(
                    "retention.ms=" + retention.value() + " source=" + retention.source());
            System.out.println("cleanup.policy=absent");

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

            admin.incrementalAlterConfigs(Map.of(resource, List.of(op("retention.ms", "9000", AlterConfigOp.OpType.SET))))
                    .all()
                    .get();
            Config incremented = describe(admin, resource);
            require("9000".equals(incremented.get("retention.ms").value()), "incremental set retention.ms");
            System.out.println("incremental_set_retention.ms=" + incremented.get("retention.ms").value());

            try {
                admin.incrementalAlterConfigs(Map.of(resource, List.of(op("retention.ms", "1", AlterConfigOp.OpType.APPEND))))
                        .all()
                        .get();
                throw new AssertionError("append to a non-list key was accepted");
            } catch (ExecutionException exception) {
                if (!(exception.getCause() instanceof InvalidConfigurationException)) {
                    throw exception;
                }
                System.out.println("incremental_append_rejected=" + exception.getCause().getClass().getSimpleName());
            }

            admin.incrementalAlterConfigs(Map.of(resource, List.of(op("retention.ms", "", AlterConfigOp.OpType.DELETE))))
                    .all()
                    .get();
            Config deleted = describe(admin, resource);
            require("-1".equals(deleted.get("retention.ms").value()), "incremental delete retention.ms");
            System.out.println("incremental_delete_retention.ms=" + deleted.get("retention.ms").value());

            // The Strimzi Topic Operator reads these two keys from every broker at startup, and
            // stops when either is missing.
            for (Node node : admin.describeCluster().nodes().get()) {
                ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
                Config brokerConfig = describe(admin, broker);
                ConfigEntry autoCreate = brokerConfig.get("auto.create.topics.enable");
                ConfigEntry minIsr = brokerConfig.get("min.insync.replicas");
                require(autoCreate != null && autoCreate.value() != null, "broker auto.create.topics.enable");
                require(minIsr != null && "1".equals(minIsr.value()), "broker min.insync.replicas");
                System.out.println("broker_auto.create.topics.enable=" + autoCreate.value()
                        + " broker_min.insync.replicas=" + minIsr.value());
            }

            NewTopic configured = new NewTopic(topic + "-configured", 2, (short) 1)
                    .configs(Map.of(
                            "retention.bytes", "1073741824",
                            "segment.bytes", "67108864",
                            "cleanup.policy", "delete"));
            Config created = admin.createTopics(List.of(configured)).config(configured.name()).get();
            require("1073741824".equals(created.get("retention.bytes").value()), "created retention.bytes");
            require(created.get("cleanup.policy") == null, "created cleanup.policy is not reported");
            ConfigResource configuredResource = new ConfigResource(ConfigResource.Type.TOPIC, configured.name());
            Config described = describe(admin, configuredResource);
            require("67108864".equals(described.get("segment.bytes").value()), "described segment.bytes");
            System.out.println("created_retention.bytes=" + created.get("retention.bytes").value()
                    + " source=" + created.get("retention.bytes").source());

            // Kafka Connect creates its internal topics this way. Iggy does not compact, so the
            // topic keeps every record, and the policy is not reported.
            NewTopic compacted = new NewTopic(topic + "-compacted", 1, (short) 1)
                    .configs(Map.of(
                            "cleanup.policy", "compact",
                            "min.insync.replicas", "1",
                            "compression.type", "zstd"));
            CreateTopicsResult compactedResult = admin.createTopics(List.of(compacted));
            Config compactedConfig = compactedResult.config(compacted.name()).get();
            int compactedFactor = compactedResult.replicationFactor(compacted.name()).get();
            require("-1".equals(compactedConfig.get("retention.ms").value()), "compacted retention.ms");
            require(compactedConfig.get("cleanup.policy") == null, "compacted cleanup.policy is not reported");
            require(compactedFactor == 1, "compacted replication factor");
            System.out.println("compacted_retention.ms=" + compactedConfig.get("retention.ms").value()
                    + " replication_factor=" + compactedFactor);

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

    private static AlterConfigOp op(String name, String value, AlterConfigOp.OpType type) {
        return new AlterConfigOp(new ConfigEntry(name, value), type);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
