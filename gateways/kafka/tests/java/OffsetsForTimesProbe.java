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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Kafka 3.9 consumer probe for {@code KafkaConsumer.offsetsForTimes}, which plain consumers and
 * Kafka Streams seek by time with. Flink, Spark and {@code --by-duration} resets go through
 * {@code AdminClient.listOffsets} instead. Prints one {@code time=<ms> offset=<offset|null>} line
 * per time.
 *
 * <p>Arguments: bootstrap server, topic, partition, then one or more times in milliseconds.
 */
public final class OffsetsForTimesProbe {
    private OffsetsForTimesProbe() {}

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10_000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        TopicPartition partition = new TopicPartition(args[1], Integer.parseInt(args[2]));
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            for (int i = 3; i < args.length; i++) {
                long time = Long.parseLong(args[i]);
                Map<TopicPartition, OffsetAndTimestamp> found =
                        consumer.offsetsForTimes(Map.of(partition, time), Duration.ofSeconds(10));
                OffsetAndTimestamp entry = found.get(partition);
                System.out.println(
                        "time=" + time + " offset=" + (entry == null ? "null" : entry.offset()));
            }
        }
    }
}
