package io.confluent.ps.convert;

import com.github.jcustenborder.parsers.elf.ElfParser;
import com.github.jcustenborder.parsers.elf.ElfParserBuilder;
import com.github.jcustenborder.parsers.elf.LogEntry;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.gist.github.yfnick.Gzip.decompress;
import static com.gist.github.yfnick.Gzip.isGZipped;
import static org.apache.kafka.common.serialization.Serdes.String;

/**
* Parse ELFF Stream with a KStream.
*/
public final class ParseElffStream {
    private final Logger log = LoggerFactory.getLogger(ParseElffStream.class);

    /**
    * Parse ELFF Stream Constructor.
    */
    private ParseElffStream() {
    }

    /**
    * Setup the Streams Processors we will be using from the passed in configuration.properties.
    * @param envProps Environment Properties file
    * @return Properties Object ready for KafkaStreams Topology Builder
    */
    protected Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.putAll(envProps);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, String().getClass());

        // Broken negative timestamp
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());

        props.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
            "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

        props.put(StreamsConfig.MAIN_CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
            "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");

        return props;
    }

    /**
    * Build the topology from the loaded configuration.
    * @param streamProps - Properties built by the buildStreamsProperties
    * @return The build topology
    */
    @SuppressWarnings("checkstyle:linelength")
    protected Topology buildTopology(Properties streamProps) {
        log.debug("Starting buildTopology");
        final String inputTopics     = streamProps.getProperty("input.topics");
        final String outputTopicName = streamProps.getProperty("output.topic.name");

        final StreamsBuilder builder = new StreamsBuilder();

        // Build the json Serialiser for log Entry
        //example from https://github.com/apache/kafka/blob/1.0/streams/examples/src/main/java/org/apache/kafka/streams/examples/pageview/PageViewTypedDemo.java

        Map<String, Object> serdeProps = (Map) streamProps;
        serdeProps.put("JsonPOJOClass", LogEntry.class);
        final Serializer<LogEntry> logEntrySerializer = new JsonPOJOSerializer<>();
        logEntrySerializer.configure(serdeProps, false);
        final Deserializer<LogEntry> logEntryDeserializer = new JsonPOJODeserializer<>();
        logEntryDeserializer.configure(serdeProps, false);

        final Serde<LogEntry> logEntrySerde = Serdes.serdeFrom(logEntrySerializer, logEntryDeserializer);

        final Pattern inputTopicPattern = Pattern.compile(inputTopics);

        // topic contains byte data
        final KStream<String, Bytes> elffStream =
            builder.stream(inputTopicPattern, Consumed.with(Serdes.String(), Serdes.Bytes()));

        // decompress gzip data into a string
        elffStream
            .flatMap((key, elffData) -> {
                List<KeyValue<String, LogEntry>> messages = new LinkedList<>();

                // Grab the ELFF String or compressed ELFF Message
                String elffString = "";
                log.debug("Decoded data!");
                if (isGZipped(elffData.get())) {
                    try {
                        elffString = decompress(elffData.get());
                    } catch (IOException e) {
                        // TODO: Poison Pill Time
                        log.info("Decode error!");
                        e.printStackTrace();
                    }
                    log.debug("Decoded: {}", elffString);
                } else {
                    // Data is already decoded
                    elffString = elffData.toString();
                    log.debug("Data is already decoded: {}", elffString);
                }

                // Now that we have the ELFF String, parse it
                try {

                    log.info("---------- ELFF String Before ----------");
                    log.info(elffString);

                    // Remove any escaped or encoded new lines with actual new lines
                    elffString = elffString.replace("\\r\\n", "\n");
                    elffString = elffString.replace("\\x5Cr\\x5Cn", "\n");
                    elffString = elffString.replace("\\x0A", "\n");
                    elffString = elffString.replace("\\x0D", "");
                    // Remove any escaped quotes
                    elffString = elffString.replace("\\\"", "\"");
                    elffString = elffString.replace("\\x5C\"", "\"");

                    log.info("---------- ELFF String After ----------");
                    log.info(elffString);
                    log.info("---------------------------------------");

                    // Now convert the string into a Reader so the Parser can do its thing
                    Reader targetReader = new StringReader(elffString);
                    ElfParser parser = ElfParserBuilder.of().build(targetReader);

                    // Copy the messages to our output
                    LogEntry entry;
                    while (null != (entry = parser.next())) {
                        messages.add(KeyValue.pair(UUID.randomUUID().toString(), entry));
                    }
                    log.debug("Parsed out {} messages", messages.size());
                } catch (IOException e) {
                    log.info("Parsing Exception");
                    e.printStackTrace();
                } catch (IllegalStateException e) {
                    log.info("Empty Bluecoat File");
                    e.printStackTrace();
                }

                return messages;
            })
            .to(outputTopicName, Produced.with(Serdes.String(), logEntrySerde));


        return builder.build();
    }

    /**
    * Load in the Environment Properties that were passed in from the CLI.
    * @param fileName
    * @return
    * @throws IOException
    */
    protected Properties loadEnvProperties(String fileName) throws IOException {
        Properties envProps = new Properties();

        try (
            FileInputStream input = new FileInputStream(fileName);
        ) {
            envProps.load(input);
        }
        return envProps;
    }

    /**
     * CountDownLatch.
     */
    @SuppressWarnings("checkstyle:declarationorder")
    private final Integer countDownLatch = 1;

    /**
     * Shutdown Delay for the Streams Close method.
     */
    @SuppressWarnings("checkstyle:declarationorder")
    private final Long shutdownDelay = 5L;

    /**
    * Main function that handles the life cycle of the Kafka Streams app.
    * @param configPath
    * @throws IOException
    */
    private void run(String configPath) throws IOException {

        Properties envProps = this.loadEnvProperties(configPath);
        Properties streamProps = this.buildStreamsProperties(envProps);

        Topology topology = this.buildTopology(streamProps);

        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(countDownLatch);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(shutdownDelay));
                latch.countDown();
            }
        });

        try {
            streams.cleanUp();
            streams.start();
            latch.await();
        } catch (IllegalStateException e) {
            System.exit(1);
        } catch (InterruptedException e) {
            System.exit(1);
        } catch (StreamsException e) {
            System.exit(1);
        }
        System.exit(0);
    }

    /**
     * Example Properties when a properties file is not provided.
     */
    @SuppressWarnings("checkstyle:linelength")
    private static void exampleProperties() {
        System.out.println("Please create a configuration properties file and pass it on the command line as an argument");
        System.out.println("Sample env.properties:");
        System.out.println("----------------------------------------------------------------");
        System.out.println("application.id=parse-elff-stream");
        System.out.println("bootstrap.servers=boot-strap.fqdn:9093");
        System.out.println("schema.registry.url=http://schema-registry.fqdn:8081");
        System.out.println("input.topic.name=elff-input");
        System.out.println("output.topic.name=elff-output");
        System.out.println("error.topic.name=elff-error");
        System.out.println("security.protocol=SASL_PLAINTEXT");
        System.out.println("sasl.mechanism=PLAIN");
        System.out.println("sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"password\";");
        System.out.println("----------------------------------------------------------------");
    }

    /**
    *  Run this with an arg for the properties file.
    * @param args
    * @throws IOException
    */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            exampleProperties();
            throw new IllegalArgumentException(
                "This program takes one argument: the path to an environment configuration file."
            );
        }

        new ParseElffStream().run(args[0]);
    }
}
