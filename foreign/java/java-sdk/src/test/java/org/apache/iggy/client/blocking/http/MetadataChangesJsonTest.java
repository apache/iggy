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

package org.apache.iggy.client.blocking.http;

import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.serde.BytesDeserializer;
import org.apache.iggy.system.MetadataChangeBody;
import org.apache.iggy.system.MetadataChangeKind;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

import static org.apache.iggy.MetadataChangesFixture.assertAnswer;
import static org.apache.iggy.MetadataChangesFixture.assertItem;
import static org.apache.iggy.MetadataChangesFixture.assertRecord;
import static org.apache.iggy.MetadataChangesFixture.buffer;
import static org.apache.iggy.MetadataChangesFixture.parse;
import static org.apache.iggy.MetadataChangesFixture.section;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The shared metadata changes vectors through the HTTP JSON reader, which must agree with the binary decoder. */
class MetadataChangesJsonTest {

    private static final String EMPTY_PAGE = """
            {"status":"ok","phase":"changes","log_id":null,"oldest_op":0,"retained_since":0,"head_op":0,
             "next":null,"has_more":false,"records":[],"items":[],"current_page_token":null}
            """;

    @Test
    void givenSharedRecordsWhenReadingJsonShouldMatchTheBinaryDecoder() {
        for (JsonNode vector : section("records")) {
            var record = MetadataChangesJson.record(vector.get("expected"));
            var binary = BytesDeserializer.readMetadataChangeRecord(
                    buffer(vector.get("record_hex").asString()));

            assertRecord(vector.get("expected"), record);
            assertThat(record).as(vector.get("case").asString()).isEqualTo(binary);
        }
    }

    @Test
    void givenSharedItemsWhenReadingJsonShouldMatchTheBinaryDecoder() {
        for (JsonNode vector : section("items")) {
            var item = MetadataChangesJson.item(vector.get("expected"));
            var binary = BytesDeserializer.readMetadataCurrentItem(
                    buffer(vector.get("item_hex").asString()));

            assertItem(vector.get("expected"), item);
            assertThat(item).as(vector.get("case").asString()).isEqualTo(binary);
        }
    }

    @Test
    void givenSharedResponsesWhenReadingJsonShouldReachTheServerJson() {
        for (JsonNode vector : section("responses")) {
            assertAnswer(vector.get("expected"), MetadataChangesJson.read(vector.get("expected")));
        }
    }

    @Test
    void givenRecordOfAKindThisBuildDoesNotKnowWhenReadingJsonShouldKeepItWhole() {
        JsonNode unknown = parse("""
                {"op":9,"ordinal":0,"checksum":"0000000000000000000000000000000a","timestamp":1,
                 "group":"streams","kind":"stream_archived","archive":"cold"}
                """);

        var record = MetadataChangesJson.record(unknown);

        assertThat(record.kind()).isEmpty();
        assertThat(record.group()).isEmpty();
        var body = (MetadataChangeBody.UnknownJson) record.body();
        assertThat(body.group()).isEqualTo("streams");
        assertThat(body.kind()).contains("stream_archived");
        assertThat(parse(body.json())).isEqualTo(unknown);
    }

    @Test
    void givenKnownKindUnderAnotherGroupWhenReadingJsonShouldRefuseIt() {
        ObjectNode record =
                (ObjectNode) section("records").get(0).get("expected").deepCopy();
        record.put("group", "topics");

        assertThat(record.get("kind").asString()).isEqualTo(MetadataChangeKind.STREAM_CREATED.value());
        assertThatThrownBy(() -> MetadataChangesJson.record(record)).isInstanceOf(IggyMalformedResponseException.class);
    }

    @Test
    void givenValueOfTheWrongTypeWhenReadingJsonShouldRefuseTheAnswer() {
        ObjectNode answer = (ObjectNode) parse(EMPTY_PAGE);
        answer.put("oldest_op", "1");

        assertThatThrownBy(() -> MetadataChangesJson.read(answer)).isInstanceOf(IggyMalformedResponseException.class);
        assertThat(MetadataChangesJson.read(parse(EMPTY_PAGE)).records()).isEmpty();
    }
}
