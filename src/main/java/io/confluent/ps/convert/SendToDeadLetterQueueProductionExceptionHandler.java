package io.confluent.ps.convert;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.streams.StreamsConfig;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;


public class SendToDeadLetterQueueProductionExceptionHandler implements ProductionExceptionHandler {
    private final static Logger log = LoggerFactory.getLogger(SendToDeadLetterQueueDeserialicationExceptionHandler.class);
    KafkaProducer<byte[], byte[]> dlqProducer;
    String dlqTopic;

    @Override
    public ProductionExceptionHandlerResponse handle(final ProducerRecord<byte[], byte[]> record,
                                                     final Exception exception) {

        log.warn("Exception caught during Deserialization, sending to the dead queue topic; " +
                  "topic: {}, partition: {}",
                  record.topic(), record.partition(), exception);

        dlqProducer.send(new ProducerRecord<>(dlqTopic, null, record.timestamp(), record.key(), record.value()));

        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        Properties props = new Properties();
        props.put("bootstrap.servers", configs.get("bootstrap.servers"));
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, configs.get("security.protocol"));
        props.put(SaslConfigs.SASL_MECHANISM, configs.get("sasl.mechanism"));
        props.put(SaslConfigs.SASL_JAAS_CONFIG, configs.get("sasl.jaas.config"));

        dlqProducer = new KafkaProducer<byte[], byte[]>(props);
        dlqTopic = configs.get("error.topic.name").toString();
        log.info("Configured DLQ as topic: " + dlqTopic);
    }
}