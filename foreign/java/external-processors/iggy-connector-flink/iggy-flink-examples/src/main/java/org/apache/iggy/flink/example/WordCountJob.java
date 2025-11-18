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

package org.apache.iggy.flink.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.iggy.connector.config.IggyConnectionConfig;
import org.apache.iggy.connector.flink.sink.IggySink;
import org.apache.iggy.connector.flink.source.IggySource;
import org.apache.iggy.connector.serialization.JsonSerializationSchema;
import org.apache.iggy.connector.serialization.StringDeserializationSchema;
import org.apache.iggy.flink.example.model.WordCount;

import java.time.Duration;

/**
 * Classic word count example demonstrating Iggy connector usage.
 *
 * <p>This job:
 * <ul>
 *   <li>Reads text lines from Iggy stream "text-input" topic "lines"</li>
 *   <li>Tokenizes lines into words</li>
 *   <li>Counts word occurrences in 1-minute tumbling windows</li>
 *   <li>Writes word counts to Iggy stream "word-counts" topic "results"</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 * # Run locally
 * ./gradlew :iggy-flink-examples:run -PmainClass=org.apache.iggy.flink.example.WordCountJob
 *
 * # Submit to Flink cluster
 * flink run -c org.apache.iggy.flink.example.WordCountJob build/libs/flink-iggy-examples.jar
 *
 * # Send sample data (requires iggy CLI)
 * echo "hello world hello flink" | iggy stream send text-input lines
 * echo "apache flink connector for iggy" | iggy stream send text-input lines
 * </pre>
 */
public final class WordCountJob {

    private static final String IGGY_SERVER = getEnv("IGGY_SERVER", "localhost:8090");
    private static final String IGGY_USERNAME = getEnv("IGGY_USERNAME", "iggy");
    private static final String IGGY_PASSWORD = getEnv("IGGY_PASSWORD", "iggy");

    // Use stream/topic names (async TCP client requires names, not IDs)
    private static final String INPUT_STREAM = "text-input";
    private static final String INPUT_TOPIC = "lines";
    private static final String OUTPUT_STREAM = "word-counts";
    private static final String OUTPUT_TOPIC = "results";

    private WordCountJob() {}

    @SuppressWarnings("checkstyle:IllegalThrows")
    public static void main(String[] args) throws Exception {
        // Set up Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(30000); // Checkpoint every 30 seconds

        // Create Iggy connection configuration
        IggyConnectionConfig connectionConfig = IggyConnectionConfig.builder()
                .serverAddress(IGGY_SERVER)
                .username(IGGY_USERNAME)
                .password(IGGY_PASSWORD)
                .connectionTimeout(Duration.ofSeconds(30))
                .build();

        // Read text lines from Iggy
        DataStream<String> textLines = env.fromSource(
                        IggySource.<String>builder()
                                .setConnectionConfig(connectionConfig)
                                .setStreamId(INPUT_STREAM)
                                .setTopicId(INPUT_TOPIC)
                                .setConsumerGroup("flink-word-counter")
                                .setDeserializer(new StringDeserializationSchema())
                                .setPollBatchSize(100)
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "Iggy Text Source")
                .returns(String.class);

        // Tokenize, count, and aggregate words
        DataStream<WordCount> wordCounts = textLines
                .flatMap(new Tokenizer())
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.LONG))
                .name("Tokenize")
                .keyBy(word -> word.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofMinutes(1)))
                .sum(1)
                .name("Count Words")
                .map(tuple -> new WordCount(tuple.f0, tuple.f1))
                .name("Create WordCount");

        // Write word counts back to Iggy
        wordCounts
                .sinkTo(IggySink.<WordCount>builder()
                        .setConnectionConfig(connectionConfig)
                        .setStreamId(OUTPUT_STREAM)
                        .setTopicId(OUTPUT_TOPIC)
                        .setSerializer(new JsonSerializationSchema<>())
                        .setBatchSize(50)
                        .setFlushInterval(Duration.ofSeconds(5))
                        .withBalancedPartitioning() // Use balanced partitioning instead
                        .build())
                .name("Iggy WordCount Sink");

        // Execute the job
        env.execute("Word Count with Iggy");
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    /**
     * Tokenizer that splits lines into words.
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Long>> {

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Long>> out) {
            if (line == null || line.trim().isEmpty()) {
                return;
            }

            // Split line into words and emit each word with count 1
            String[] words = line.toLowerCase()
                    .replaceAll("[^a-z0-9\\s]", "") // Remove punctuation
                    .trim()
                    .split("\\s+");

            for (String word : words) {
                if (!word.isEmpty()) {
                    out.collect(new Tuple2<>(word, 1L));
                }
            }
        }
    }
}
