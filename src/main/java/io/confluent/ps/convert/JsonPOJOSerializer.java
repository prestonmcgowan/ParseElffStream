package io.confluent.ps.convert;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonPOJOSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka
     */
    public JsonPOJOSerializer() {
        // Nothing to do here
    }

    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        // Nothing to do here
    }

    @Override
    public byte[] serialize(String topic, T data) {
        byte[] emptyArray = {};
        if (data == null)
            return emptyArray;

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
        // Nothing to do here
    }

}
