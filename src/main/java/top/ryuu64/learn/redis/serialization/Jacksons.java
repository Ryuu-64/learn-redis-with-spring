package top.ryuu64.learn.redis.serialization;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.ryuu.functional.Func;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimeZone;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Jacksons {
    private static final Logger LOGGER = LoggerFactory.getLogger(Jacksons.class);
    private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper();

    static {
        DEFAULT_OBJECT_MAPPER.setTimeZone(TimeZone.getDefault());
        SimpleModule module = new SimpleModule();
        DEFAULT_OBJECT_MAPPER.registerModule(module);
    }

    public static String writeValueAsString(Object value) throws JsonProcessingException {
        return DEFAULT_OBJECT_MAPPER.writeValueAsString(value);
    }

    public static <T> T readValueOrDefault(String content, TypeReference<T> valueTypeRef, Func<T> getDefaultValue) {
        try {
            return readValue(content, valueTypeRef);
        } catch (Exception e) {
            LOGGER.error("failed to deserialize json", e);
            return getDefaultValue.invoke();
        }
    }

    public static <T> T readValue(String content, TypeReference<T> valueTypeRef) throws JsonProcessingException {
        return DEFAULT_OBJECT_MAPPER.readValue(content, valueTypeRef);
    }

    public static ObjectNode createObjectNode() {
        return DEFAULT_OBJECT_MAPPER.createObjectNode();
    }

    public static <T extends JsonNode> T valueToTree(Object fromValue) throws IllegalArgumentException {
        return DEFAULT_OBJECT_MAPPER.valueToTree(fromValue);
    }

    public static JsonNode readTree(String content) {
        try {
            return DEFAULT_OBJECT_MAPPER.readTree(content);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
