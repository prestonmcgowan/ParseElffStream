package io.confluent.ps.convert;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * JSON Deserializer for Kafka.
 * @param <T> - TypeToMap
 */
public class JsonPOJODeserializer<T> implements Deserializer<T> {
    private ObjectMapper objectMapper = new ObjectMapper();

    private Class<T> tClass;

    /**
     * Default constructor needed by Kafka.
     */
    public JsonPOJODeserializer() {
        // Nothing to do here
    }

    /**
     * Configure this class.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        tClass = (Class<T>) props.get("JsonPOJOClass");
    }

    /**
     * Deserialize the bytes provided.
     */
    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        T data;
        try {
            data = objectMapper.readValue(bytes, tClass);
        } catch (IOException e) {
            throw new SerializationException(e);
        }


        return data;
    }

    /**
     * All done, close this class.
     */
    @Override
    public void close() {
        // Nothing to do here
    }
}
