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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Client matrix probe for the Java client (3.9 and 4.x), at its defaults: an idempotent producer
 * with {@code acks=all}, and a classic consumer group. Speaks the probe contract in
 * {@code ../README.md}: ACK, NACK, REC, COMMITTED and DONE lines on stdout.
 *
 * <p>Environment: {@code BOOTSTRAP}, {@code TOPIC}, {@code GROUP}, {@code COUNT}, {@code
 * DEADLINE_S}.
 */
public final class ClientMatrixProbe {
    private static final String BOOTSTRAP = System.getenv("BOOTSTRAP");
    private static final String TOPIC = System.getenv("TOPIC");
    private static final String GROUP = System.getenv("GROUP");
    private static final int COUNT = envInt("COUNT", 12);
    private static final Duration DEADLINE = Duration.ofSeconds(envInt("DEADLINE_S", 45));

    private ClientMatrixProbe() {}

    private static int envInt(String name, int fallback) {
        String value = System.getenv(name);
        return value == null ? fallback : Integer.parseInt(value);
    }

    private static synchronized void out(String line) {
        System.out.println(line);
        System.out.flush();
    }

    private static String word(Throwable error) {
        return error.getClass().getSimpleName();
    }

    private static void produce(String phase, int batch) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "matrix-java");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) DEADLINE.toMillis());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            int first = (batch - 1) * COUNT;
            for (int index = first; index < first + COUNT; index++) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(
                                TOPIC, String.format("k%03d", index), String.format("v%03d", index));
                record.headers().add("idx", Integer.toString(index).getBytes(StandardCharsets.UTF_8));
                record.headers().add("src", "matrix".getBytes(StandardCharsets.UTF_8));
                producer.send(
                        record,
                        (metadata, error) -> {
                            if (error != null) {
                                out("NACK " + phase + " " + word(error));
                            } else {
                                out("ACK " + phase + " " + metadata.partition() + " " + metadata.offset());
                            }
                        });
            }
            producer.flush();
        }
    }

    private static KafkaConsumer<String, String> consumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "matrix-java");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    private static String text(String value) {
        return value == null ? "-" : value;
    }

    private static void consume(String phase) {
        try (KafkaConsumer<String, String> consumer = consumer()) {
            consumer.subscribe(List.of(TOPIC));
            int seen = 0;
            long deadline = System.nanoTime() + DEADLINE.toNanos();
            while (seen < COUNT && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    List<String> headers = new ArrayList<>();
                    for (Header header : record.headers()) {
                        headers.add(
                                header.key()
                                        + "="
                                        + (header.value() == null
                                                ? "-"
                                                : new String(header.value(), StandardCharsets.UTF_8)));
                    }
                    out(
                            "REC "
                                    + phase
                                    + " "
                                    + record.partition()
                                    + " "
                                    + record.offset()
                                    + " "
                                    + text(record.key())
                                    + " "
                                    + text(record.value())
                                    + " "
                                    + (headers.isEmpty() ? "-" : String.join(",", headers)));
                    seen++;
                }
            }
            try {
                consumer.commitSync(DEADLINE);
            } catch (RuntimeException error) {
                out("NACK " + phase + ".commit " + word(error));
            }
            Set<TopicPartition> partitions = new HashSet<>();
            for (PartitionInfo info : consumer.partitionsFor(TOPIC, DEADLINE)) {
                partitions.add(new TopicPartition(TOPIC, info.partition()));
            }
            Map<Integer, Long> committed = new TreeMap<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry :
                    consumer.committed(partitions, DEADLINE).entrySet()) {
                committed.put(
                        entry.getKey().partition(),
                        entry.getValue() == null ? -1L : entry.getValue().offset());
            }
            committed.forEach((partition, offset) -> out("COMMITTED " + phase + " " + partition + " " + offset));
        }
    }

    public static void main(String[] args) throws InterruptedException {
        produce("produce", 1);
        consume("consume");
        produce("produce2", 2);
        consume("resume");
        out("DONE");
    }
}
