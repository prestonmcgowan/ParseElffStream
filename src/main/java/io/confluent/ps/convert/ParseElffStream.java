package io.confluent.ps.convert;

import static com.gist.github.yfnick.Gzip.decompress;
import static com.gist.github.yfnick.Gzip.isGZipped;
import static org.apache.kafka.common.serialization.Serdes.String;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.jcustenborder.parsers.elf.ElfParser;
import com.github.jcustenborder.parsers.elf.ElfParserBuilder;
import com.github.jcustenborder.parsers.elf.LogEntry;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.glassfish.jersey.internal.util.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, String().getClass());
        /**props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, envProps.getProperty("security.protocol"));
        props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, envProps.getProperty("security.protocol"));
        props.put(SaslConfigs.SASL_MECHANISM, envProps.getProperty("sasl.mechanism"));
        props.put(SaslConfigs.SASL_JAAS_CONFIG, envProps.getProperty("sasl.jaas.config"));
**/
        log.debug("SASL Config------");
        log.debug("bootstrap.servers={}", envProps.getProperty("bootstrap.servers"));
        log.debug("security.protocol={}", envProps.getProperty("security.protocol"));
        log.debug("sasl.mechanism={}", envProps.getProperty("sasl.mechanism"));
        log.debug("sasl.jaas.config={}", envProps.getProperty("sasl.jaas.config"));
        log.debug("-----------------");

        props.put("error.topic.name", envProps.getProperty("error.topic.name"));

        // Broken negative timestamp
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
            WallclockTimestampExtractor.class.getName());

        return props;
    }

    /**
     * Build the topology from the loaded configuration
     * @param Properties built by the buildStreamsProperties
     * @return The build topology
     */
    protected Topology buildTopology(Properties envProps) {
        log.debug("Starting buildTopology");
        final String inputTopicName = envProps.getProperty("input.topic.name");
        final String outputTopicName = envProps.getProperty("output.topic.name");

        final StreamsBuilder builder = new StreamsBuilder();

        // topic contains byte data
        final KStream<Integer, Bytes> elffStream =
            builder.stream(inputTopicName, Consumed.with(Serdes.Integer(), Serdes.Bytes()));
        ;

        // decompress gzip data into a string

        elffStream.mapValues( elffData-> {
            //System.out.println(elffData.toString().replace("\\x0A", "\\r\\n"));
            //System.out.println("------------------Done printing elffString --------------------------");

            List<LogEntry> messages = new ArrayList<>();

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
                String elffreplace = elffString.replace("\\x0A", "\n");
                Reader targetReader = new StringReader(elffreplace);
                System.out.println("------------------processing elffString --------------------------");
                System.out.println(elffreplace);
                System.out.print(targetReader);
                ElfParser parser = ElfParserBuilder.of().build(targetReader);
                System.out.println("------------------Done processing elffString --------------------------");
                 // Copy the messages to our output
                LogEntry entry;
                while (null != (entry = parser.next())) {
                    messages.add(entry);
                }

            } catch (IOException e) {
                log.info("Parsing Exception");
                e.printStackTrace();
            }

            System.out.println(messages);

            return messages;

        }).map((key, value) -> {
            return
            KeyValue.pair(
                    UUID.randomUUID().toString(),
                    value.toString()
            );
                }
        )
        .to(outputTopicName);//to(outputTopicName, Produced<Integer, LogEntry>); // , Produced.valueSerde(Serdes.String()));

        //TODO: Implement this so it works, it should be before the .to(...)
        // .mapValues( message -> {
        //     log.info(message);
        //     //TODO: Push into JSON using gson library
        //     //TODO: each message individually to the output topic (one to many)
        // })
        //elffStream.to(outputTopicName);

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
       * Main function that handles the life cycle of the Kafka Streams app.
       * @param configPath
       * @throws IOException
       */
      private void run(String configPath) throws IOException {

        Properties envProps = this.loadEnvProperties(configPath);
        Properties streamProps = this.buildStreamsProperties(envProps);

        Topology topology = this.buildTopology(envProps);

        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
          @Override
          public void run() {
            streams.close(Duration.ofSeconds(5));
            latch.countDown();
          }
        });

        try {
          streams.cleanUp();
          streams.start();
          latch.await();
        } catch (Throwable e) {
          System.exit(1);
        }
        System.exit(0);
      }

      /**
       *  Run this with an arg for the properties file
       * @param args
       * @throws IOException
       */
      public static void main(String[] args) throws IOException {
        if (args.length < 1) {
          throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }

        new ParseElffStream().run(args[0]);
      }
}
