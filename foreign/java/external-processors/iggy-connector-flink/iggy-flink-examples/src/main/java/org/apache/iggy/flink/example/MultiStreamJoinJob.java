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
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iggy.connector.config.IggyConnectionConfig;
import org.apache.iggy.connector.flink.sink.IggySink;
import org.apache.iggy.connector.flink.source.IggySource;
import org.apache.iggy.connector.serialization.JsonDeserializationSchema;
import org.apache.iggy.connector.serialization.JsonSerializationSchema;
import org.apache.iggy.flink.example.model.EnrichedActivity;
import org.apache.iggy.flink.example.model.UserActivity;
import org.apache.iggy.flink.example.model.UserProfile;

import java.io.Serializable;
import java.time.Duration;
import java.util.function.Function;

/**
 * Example demonstrating multi-stream join using Iggy connector.
 *
 * <p>This job:
 * <ul>
 *   <li>Reads user activities from Iggy stream "user-activities" topic "events"</li>
 *   <li>Reads user profiles from Iggy stream "user-profiles" topic "updates"</li>
 *   <li>Enriches activities with profile information using stateful join</li>
 *   <li>Writes enriched activities to Iggy stream "enriched-activities" topic "results"</li>
 * </ul>
 *
 * <p>The join maintains user profile state and enriches incoming activities with the
 * latest profile information. If a profile is not yet available for a user, the activity
 * is buffered until the profile arrives.
 *
 * <p>Usage:
 * <pre>
 * # Run locally
 * ./gradlew :iggy-flink-examples:run -PmainClass=org.apache.iggy.flink.example.MultiStreamJoinJob
 *
 * # Submit to Flink cluster
 * flink run -c org.apache.iggy.flink.example.MultiStreamJoinJob build/libs/flink-iggy-examples.jar
 *
 * # Send sample data (requires iggy CLI)
 * # Send user profile
 * echo '{"userId":"user123","userName":"Alice","userTier":"premium","country":"US"}' \
 *   | iggy stream send user-profiles updates
 *
 * # Send user activity
 * echo '{"userId":"user123","activityType":"VIEW","resourceId":"article-456","timestamp":"2025-10-22T10:30:00Z"}' \
 *   | iggy stream send user-activities events
 * </pre>
 */
public final class MultiStreamJoinJob {

    private static final String IGGY_SERVER = getEnv("IGGY_SERVER", "localhost:8090");
    private static final String IGGY_USERNAME = getEnv("IGGY_USERNAME", "iggy");
    private static final String IGGY_PASSWORD = getEnv("IGGY_PASSWORD", "iggy");

    // Use numeric IDs to match docker-compose.yml setup
    private static final String ACTIVITY_STREAM = "5"; // user-activities
    private static final String ACTIVITY_TOPIC = "1"; // events
    private static final String PROFILE_STREAM = "6"; // user-profiles
    private static final String PROFILE_TOPIC = "1"; // updates
    private static final String OUTPUT_STREAM = "7"; // enriched-activities
    private static final String OUTPUT_TOPIC = "1"; // results

    private MultiStreamJoinJob() {}

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

        // Read user activities from Iggy
        DataStream<UserActivity> activities = env.fromSource(
                        IggySource.<UserActivity>builder()
                                .setConnectionConfig(connectionConfig)
                                .setStreamId(ACTIVITY_STREAM)
                                .setTopicId(ACTIVITY_TOPIC)
                                .setConsumerGroup("flink-activity-enricher")
                                .setDeserializer(new JsonDeserializationSchema<>(UserActivity.class))
                                .setPollBatchSize(100)
                                .build(),
                        WatermarkStrategy.<UserActivity>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((activity, timestamp) ->
                                        activity.getTimestamp().toEpochMilli()),
                        "Iggy Activity Source")
                .returns(UserActivity.class);

        // Read user profiles from Iggy
        DataStream<UserProfile> profiles = env.fromSource(
                        IggySource.<UserProfile>builder()
                                .setConnectionConfig(connectionConfig)
                                .setStreamId(PROFILE_STREAM)
                                .setTopicId(PROFILE_TOPIC)
                                .setConsumerGroup("flink-profile-reader")
                                .setDeserializer(new JsonDeserializationSchema<>(UserProfile.class))
                                .setPollBatchSize(50)
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "Iggy Profile Source")
                .returns(UserProfile.class);

        // Enrich activities with profile information using stateful join
        DataStream<EnrichedActivity> enrichedActivities = activities
                .keyBy(UserActivity::getUserId)
                .connect(profiles.keyBy(UserProfile::getUserId))
                .process(new ProfileEnrichmentFunction())
                .name("Enrich Activities");

        // Write enriched activities back to Iggy
        enrichedActivities
                .sinkTo(IggySink.<EnrichedActivity>builder()
                        .setConnectionConfig(connectionConfig)
                        .setStreamId(OUTPUT_STREAM)
                        .setTopicId(OUTPUT_TOPIC)
                        .setSerializer(new JsonSerializationSchema<>())
                        .setBatchSize(50)
                        .setFlushInterval(Duration.ofSeconds(5))
                        .withBalancedPartitioning() // Use balanced partitioning instead
                        .build())
                .name("Iggy Enriched Activity Sink");

        // Execute the job
        env.execute("Multi-Stream Join with Iggy");
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    /**
     * Co-process function that enriches activities with profile information.
     * Maintains profile state and buffers activities if profile is not yet available.
     */
    public static class ProfileEnrichmentFunction
            extends KeyedCoProcessFunction<String, UserActivity, UserProfile, EnrichedActivity> {

        private transient ValueState<UserProfile> profileState;
        private transient ValueState<UserActivity> pendingActivityState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);

            // State to store the latest user profile
            ValueStateDescriptor<UserProfile> profileDescriptor =
                    new ValueStateDescriptor<>("user-profile", UserProfile.class);
            profileState = getRuntimeContext().getState(profileDescriptor);

            // State to buffer activities waiting for profile
            ValueStateDescriptor<UserActivity> activityDescriptor =
                    new ValueStateDescriptor<>("pending-activity", UserActivity.class);
            pendingActivityState = getRuntimeContext().getState(activityDescriptor);
        }

        @Override
        public void processElement1(UserActivity activity, Context context, Collector<EnrichedActivity> out)
                throws Exception {
            // Process activity event
            System.out.println(">>> processElement1 - Activity: userId=" + activity.getUserId() + ", type="
                    + activity.getActivityType());
            UserProfile profile = profileState.value();

            if (profile != null) {
                // Profile is available, enrich and emit
                System.out.println(
                        ">>> MATCH! Profile found for userId=" + activity.getUserId() + ", emitting enriched activity");
                out.collect(EnrichedActivity.from(activity, profile));
            } else {
                // Profile not yet available, buffer the activity
                System.out.println(">>> NO MATCH - Buffering activity for userId=" + activity.getUserId());
                pendingActivityState.update(activity);
            }
        }

        @Override
        public void processElement2(UserProfile profile, Context context, Collector<EnrichedActivity> out)
                throws Exception {
            // Process profile update
            System.out.println(
                    ">>> processElement2 - Profile: userId=" + profile.getUserId() + ", name=" + profile.getUserName());
            profileState.update(profile);

            // Check if there's a pending activity for this user
            UserActivity pendingActivity = pendingActivityState.value();
            if (pendingActivity != null) {
                // Enrich and emit the pending activity
                System.out.println(">>> FOUND PENDING! Emitting buffered activity for userId=" + profile.getUserId());
                out.collect(EnrichedActivity.from(pendingActivity, profile));
                // Clear the pending activity
                pendingActivityState.clear();
            } else {
                System.out.println(">>> No pending activity for userId=" + profile.getUserId());
            }
        }
    }

    /**
     * Serializable partition key extractor for enriched activities.
     */
    private static final class EnrichedActivityPartitionKeyExtractor
            implements Function<EnrichedActivity, Integer>, Serializable {
        @Override
        public Integer apply(EnrichedActivity enriched) {
            return enriched.getUserId().hashCode();
        }
    }
}
