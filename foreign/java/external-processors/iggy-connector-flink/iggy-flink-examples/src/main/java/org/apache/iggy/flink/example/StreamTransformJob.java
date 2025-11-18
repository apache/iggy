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

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.iggy.connector.config.IggyConnectionConfig;
import org.apache.iggy.connector.flink.sink.IggySink;
import org.apache.iggy.connector.flink.source.IggySource;
import org.apache.iggy.connector.serialization.JsonDeserializationSchema;
import org.apache.iggy.connector.serialization.JsonSerializationSchema;
import org.apache.iggy.flink.example.model.Alert;
import org.apache.iggy.flink.example.model.SensorReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.function.Function;

/**
 * Example Flink job demonstrating stream-to-stream transformation using Iggy connector.
 *
 * <p>This job:
 * <ul>
 *   <li>Reads sensor readings from Iggy stream "sensors" topic "readings"</li>
 *   <li>Detects anomalies (temperature > 30°C or humidity > 80%)</li>
 *   <li>Generates alerts for critical conditions</li>
 *   <li>Writes alerts back to Iggy stream "alerts" topic "critical"</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 * # Run locally
 * ./gradlew :iggy-flink-examples:run
 *
 * # Submit to Flink cluster
 * flink run build/libs/flink-iggy-examples.jar
 * </pre>
 */
public final class StreamTransformJob {

    private static final String IGGY_SERVER = getEnv("IGGY_SERVER", "localhost:8090");
    private static final String IGGY_USERNAME = getEnv("IGGY_USERNAME", "iggy");
    private static final String IGGY_PASSWORD = getEnv("IGGY_PASSWORD", "iggy");

    // Stream and topic IDs (numeric)
    private static final String INPUT_STREAM = "1"; // sensors
    private static final String INPUT_TOPIC = "1"; // readings
    private static final String OUTPUT_STREAM = "2"; // alerts
    private static final String OUTPUT_TOPIC = "1"; // critical

    private static final double TEMP_THRESHOLD = 30.0;
    private static final double HUMIDITY_THRESHOLD = 80.0;

    private StreamTransformJob() {}

    @SuppressWarnings("checkstyle:IllegalThrows")
    public static void main(String[] args) throws Exception {
        // Set up Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(60000); // Checkpoint every 60 seconds

        // Create Iggy connection configuration
        IggyConnectionConfig connectionConfig = IggyConnectionConfig.builder()
                .serverAddress(IGGY_SERVER)
                .username(IGGY_USERNAME)
                .password(IGGY_PASSWORD)
                .connectionTimeout(Duration.ofSeconds(30))
                .build();

        // Read sensor readings from Iggy
        DataStream<SensorReading> sensorReadings = env.fromSource(
                IggySource.<SensorReading>builder()
                        .setConnectionConfig(connectionConfig)
                        .setStreamId(INPUT_STREAM)
                        .setTopicId(INPUT_TOPIC)
                        .setConsumerGroup("flink-anomaly-detector")
                        .setDeserializer(new JsonDeserializationSchema<>(SensorReading.class))
                        .setPollBatchSize(100)
                        .build(),
                WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new TimestampExtractor()),
                "Iggy Sensor Source",
                TypeInformation.of(SensorReading.class));

        // Process sensor readings and generate alerts
        DataStream<Alert> alerts = sensorReadings
                .filter(new AnomalyFilter())
                .map(new AlertMapper())
                .name("Generate Alerts");

        // Optional: Add windowed aggregation to count alerts per sensor
        DataStream<Alert> windowedAlerts = alerts.keyBy(Alert::getSensorId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
                .reduce(new AlertReducer())
                .name("Window Alerts");

        // Write alerts back to Iggy
        windowedAlerts
                .sinkTo(IggySink.<Alert>builder()
                        .setConnectionConfig(connectionConfig)
                        .setStreamId(OUTPUT_STREAM)
                        .setTopicId(OUTPUT_TOPIC)
                        .setSerializer(new JsonSerializationSchema<>())
                        .setBatchSize(50)
                        .setFlushInterval(Duration.ofSeconds(5))
                        .withBalancedPartitioning() // Use balanced partitioning instead
                        .build())
                .name("Iggy Alert Sink");

        // Execute the job
        env.execute("Sensor Anomaly Detection with Iggy");
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    // Static serializable function classes
    private static final class AnomalyFilter implements FilterFunction<SensorReading> {
        private static final Logger log = LoggerFactory.getLogger(AnomalyFilter.class);

        @Override
        public boolean filter(SensorReading reading) {
            boolean isAnomaly = reading.getTemperature() > TEMP_THRESHOLD || reading.getHumidity() > HUMIDITY_THRESHOLD;
            log.info(
                    "AnomalyFilter: reading={}, temp={}, humidity={}, isAnomaly={}",
                    reading.getSensorId(),
                    reading.getTemperature(),
                    reading.getHumidity(),
                    isAnomaly);
            return isAnomaly;
        }
    }

    private static final class AlertMapper implements MapFunction<SensorReading, Alert> {
        private static final Logger log = LoggerFactory.getLogger(AlertMapper.class);

        @Override
        public Alert map(SensorReading reading) {
            String alertType;
            double value;
            String message;

            if (reading.getTemperature() > TEMP_THRESHOLD) {
                alertType = "HIGH_TEMPERATURE";
                value = reading.getTemperature();
                message = String.format(
                        "Temperature %.1f°C exceeds threshold %.1f°C", reading.getTemperature(), TEMP_THRESHOLD);
            } else {
                alertType = "HIGH_HUMIDITY";
                value = reading.getHumidity();
                message = String.format(
                        "Humidity %.1f%% exceeds threshold %.1f%%", reading.getHumidity(), HUMIDITY_THRESHOLD);
            }

            Alert alert = new Alert(reading.getSensorId(), alertType, value, message, reading.getTimestamp());
            log.info("AlertMapper: created alert={}, type={}, sensor={}", alert, alertType, reading.getSensorId());
            return alert;
        }
    }

    private static final class AlertReducer implements ReduceFunction<Alert> {
        private static final Logger log = LoggerFactory.getLogger(AlertReducer.class);

        @Override
        public Alert reduce(Alert alert1, Alert alert2) {
            // Keep the latest alert in the window
            Alert result = alert1.getTimestamp().isAfter(alert2.getTimestamp()) ? alert1 : alert2;
            log.info(
                    "AlertReducer: reducing alert1={} and alert2={}, result={}",
                    alert1.getSensorId(),
                    alert2.getSensorId(),
                    result.getSensorId());
            return result;
        }
    }

    private static final class TimestampExtractor implements SerializableTimestampAssigner<SensorReading> {
        @Override
        public long extractTimestamp(SensorReading reading, long recordTimestamp) {
            return reading.getTimestamp().toEpochMilli();
        }
    }

    private static final class PartitionKeyExtractor implements Function<Alert, Integer>, Serializable {
        @Override
        public Integer apply(Alert alert) {
            return alert.getSensorId().hashCode();
        }
    }
}
