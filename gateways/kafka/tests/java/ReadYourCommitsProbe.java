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

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Commits an offset and reads it back at once, in every partition, round after round. A read
 * that a lagging replica answers returns the previous offset, or none after the first commit.
 * Prints one line per wrong read and a summary, and exits with 1 if any read was wrong.
 *
 * <p>Arguments: bootstrap server, topic, partition count, rounds, group id.
 */
public final class ReadYourCommitsProbe {
    private ReadYourCommitsProbe() {}

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, args[4]);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        int partitions = Integer.parseInt(args[2]);
        int rounds = Integer.parseInt(args[3]);
        int wrong = 0;
        long slowestNanos = 0;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            for (int round = 1; round <= rounds; round++) {
                for (int partition = 0; partition < partitions; partition++) {
                    TopicPartition topicPartition = new TopicPartition(args[1], partition);
                    long started = System.nanoTime();
                    consumer.commitSync(Map.of(topicPartition, new OffsetAndMetadata(round)));
                    OffsetAndMetadata read =
                            consumer.committed(Set.of(topicPartition), Duration.ofSeconds(30))
                                    .get(topicPartition);
                    slowestNanos = Math.max(slowestNanos, System.nanoTime() - started);
                    if (read == null || read.offset() != round) {
                        wrong++;
                        System.out.println("partition=" + partition + " committed=" + round
                                + " read=" + (read == null ? "none" : read.offset()));
                    }
                }
            }
        }
        System.out.println("read_your_commits rounds=" + rounds + " partitions=" + partitions
                + " wrong=" + wrong + " slowest_ms=" + slowestNanos / 1_000_000);
        System.exit(wrong == 0 ? 0 : 1);
    }
}
