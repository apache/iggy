package org.apache.iggy.connector.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;

/**
 * Deserialization schema for JSON using Jackson.
 * Supports Java 8 time types and handles unknown properties gracefully.
 *
 * @param <T> the type to deserialize to
 */
public class JsonDeserializationSchema<T> implements DeserializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> typeClass;
    private final transient ObjectMapper objectMapper;

    /**
     * Creates a new JSON deserializer for the specified type.
     *
     * @param typeClass the class to deserialize to
     */
    public JsonDeserializationSchema(Class<T> typeClass) {
        this(typeClass, createDefaultObjectMapper());
    }

    /**
     * Creates a new JSON deserializer with a custom ObjectMapper.
     *
     * @param typeClass the class to deserialize to
     * @param objectMapper the Jackson ObjectMapper to use
     */
    public JsonDeserializationSchema(Class<T> typeClass, ObjectMapper objectMapper) {
        if (typeClass == null) {
            throw new IllegalArgumentException("typeClass cannot be null");
        }
        if (objectMapper == null) {
            throw new IllegalArgumentException("objectMapper cannot be null");
        }
        this.typeClass = typeClass;
        this.objectMapper = objectMapper;
    }

    @Override
    public T deserialize(byte[] data, RecordMetadata metadata) throws IOException {
        if (data == null || data.length == 0) {
            return null;
        }

        try {
            return getObjectMapper().readValue(data, typeClass);
        } catch (IOException e) {
            throw new IOException(
                    "Failed to deserialize JSON for type " + typeClass.getName(), e);
        }
    }

    @Override
    public TypeDescriptor<T> getProducedType() {
        return TypeDescriptor.of(typeClass);
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

    /**
     * Gets the target type class.
     *
     * @return the type class
     */
    public Class<T> getTypeClass() {
        return typeClass;
    }
}
