package rs.iggy.clients.blocking.http;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

final class ObjectMapperFactory {

    private static final ObjectMapper INSTANCE = JsonMapper.builder()
            .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES, true)
            .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
            .build()
            .registerModule(new Jdk8Module())
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    private ObjectMapperFactory() {
    }

    static ObjectMapper getInstance() {
        return INSTANCE;
    }

}
