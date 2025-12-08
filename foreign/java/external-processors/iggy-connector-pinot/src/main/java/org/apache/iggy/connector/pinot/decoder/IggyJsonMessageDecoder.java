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

package org.apache.iggy.connector.pinot.decoder;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageDecoder;

import java.util.Map;
import java.util.Set;

/**
 * JSON message decoder for Iggy streams.
 * Decodes JSON-formatted messages from Iggy into Pinot GenericRow format.
 *
 * <p>Configuration in Pinot table config:
 * <pre>{@code
 * "streamConfigs": {
 *   "stream.iggy.decoder.class.name": "org.apache.iggy.connector.pinot.decoder.IggyJsonMessageDecoder"
 * }
 * }</pre>
 */
public class IggyJsonMessageDecoder implements StreamMessageDecoder<byte[]> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Initializes the decoder with configuration.
     * Can be used to set up custom deserialization if needed.
     *
     * @param props decoder properties from streamConfigs
     * @param fieldsToRead set of fields to read from messages
     * @param topicName topic name
     * @throws Exception if initialization fails
     */
    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName) throws Exception {
        // No special initialization needed for basic JSON decoding
    }

    /**
     * Decodes a JSON message payload into a GenericRow.
     *
     * @param payload raw byte array containing JSON
     * @return GenericRow with decoded fields
     */
    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> jsonMap = OBJECT_MAPPER.readValue(payload, Map.class);

            for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
                destination.putValue(entry.getKey(), entry.getValue());
            }

            return destination;

        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to decode JSON message", e);
        }
    }

    /**
     * Decodes a JSON message and returns the specified field values.
     *
     * @param payload raw byte array containing JSON
     * @param offset offset in the payload to start decoding
     * @param length length of the message to decode
     * @param destination destination GenericRow to populate
     * @return GenericRow with requested fields
     */
    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
        // Create a new byte array for the specified range
        byte[] messageBytes = new byte[length];
        System.arraycopy(payload, offset, messageBytes, 0, length);
        return decode(messageBytes, destination);
    }
}
