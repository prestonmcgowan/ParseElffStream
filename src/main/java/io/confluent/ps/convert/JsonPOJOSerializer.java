package io.confluent.ps.convert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * JSON Serializer for Kafka.
 * @param <T> - TypeToMap
 */
public class JsonPOJOSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka.
     */
    public JsonPOJOSerializer() {
        // Nothing to do here
    }

    /**
     * Configure this class.
     */
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        // Nothing to do here
    }

    /**
     * Serialize the bytes provided.
     */
    @Override
    public byte[] serialize(String topic, T data) {
        byte[] emptyArray = {};
        if (data == null) {
            return emptyArray;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    /**
     * All done, close this class.
     */
    @Override
    public void close() {
        // Nothing to do here
    }

}
