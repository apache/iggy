package org.apache.iggy.connector.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;

/**
 * Serialization schema for JSON using Jackson.
 * Supports Java 8 time types and optional partition key extraction.
 *
 * @param <T> the type to serialize from
 */
public class JsonSerializationSchema<T> implements SerializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final transient ObjectMapper objectMapper;
    private final Function<T, Integer> partitionKeyExtractor;

    /**
     * Creates a new JSON serializer.
     */
    public JsonSerializationSchema() {
        this(createDefaultObjectMapper(), null);
    }

    /**
     * Creates a new JSON serializer with partition key extractor.
     *
     * @param partitionKeyExtractor function to extract partition key from elements
     */
    public JsonSerializationSchema(Function<T, Integer> partitionKeyExtractor) {
        this(createDefaultObjectMapper(), partitionKeyExtractor);
    }

    /**
     * Creates a new JSON serializer with partition key extractor.
     * Static factory method to avoid constructor ambiguity.
     *
     * @param partitionKeyExtractor function to extract partition key from elements
     * @param <T> the type to serialize
     * @return new JsonSerializationSchema instance
     */
    public static <T> JsonSerializationSchema<T> withPartitionKeyExtractor(
            Function<T, Integer> partitionKeyExtractor) {
        return new JsonSerializationSchema<>(partitionKeyExtractor);
    }

    /**
     * Creates a new JSON serializer with a custom ObjectMapper.
     *
     * @param objectMapper the Jackson ObjectMapper to use
     */
    public JsonSerializationSchema(ObjectMapper objectMapper) {
        this(objectMapper, null);
    }

    /**
     * Creates a new JSON serializer with custom ObjectMapper and partition key extractor.
     *
     * @param objectMapper the Jackson ObjectMapper to use
     * @param partitionKeyExtractor function to extract partition key from elements
     */
    public JsonSerializationSchema(
            ObjectMapper objectMapper,
            Function<T, Integer> partitionKeyExtractor) {
        if (objectMapper == null) {
            throw new IllegalArgumentException("objectMapper cannot be null");
        }
        this.objectMapper = objectMapper;
        this.partitionKeyExtractor = partitionKeyExtractor;
    }

    @Override
    public byte[] serialize(T element) throws IOException {
        if (element == null) {
            return null;
        }

        try {
            return getObjectMapper().writeValueAsBytes(element);
        } catch (IOException e) {
            throw new IOException(
                    "Failed to serialize object of type " + element.getClass().getName(), e);
        }
    }

    @Override
    public Optional<Integer> extractPartitionKey(T element) {
        if (partitionKeyExtractor != null && element != null) {
            try {
                Integer key = partitionKeyExtractor.apply(element);
                return Optional.ofNullable(key);
            } catch (Exception e) {
                // Log and return empty if extraction fails
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    /**
     * Gets the ObjectMapper, creating a default one if needed (for deserialization).
     *
     * @return the ObjectMapper instance
     */
    private ObjectMapper getObjectMapper() {
        if (objectMapper != null) {
            return objectMapper;
        }
        // Fallback for deserialized instances
        return createDefaultObjectMapper();
    }

    /**
     * Creates a default ObjectMapper configured for common use cases.
     *
     * @return configured ObjectMapper
     */
    private static ObjectMapper createDefaultObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // Register Java 8 time module for LocalDateTime, Instant, etc.
        mapper.registerModule(new JavaTimeModule());

        // Don't fail on unknown properties
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // Write dates as ISO-8601 strings, not timestamps
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        return mapper;
    }
}
