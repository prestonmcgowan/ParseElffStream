# ParseElffStream
Parse BlueCoat (ELFF) Data Streams

### Build and Execution Environment
* Java 8
* Confluent Platform 5.5.x or newer
* jcustenborder extended log format parser

```
git clone https://github.com/jcustenborder/extended-log-format-parser.git
cd extended-log-format-parser
mvn clean install
 ```

## Build
Use Maven to build the KStream Application.

```
mvn clean package
```

A successful build will create a target directory with the following two jar files:
* parse-elff-stream-0.1.jar
* parse-elff-stream-0.1-jar-with-dependencies.jar

The `parse-elff-stream-0.1-jar-with-dependencies.jar` file contains all the dependencies needed to run the application. Therefore, this dependencies jar file should be the file executed when running the KStream Application.

## Execution Configuration
The KStream Application requires a configuration properties file.

Example:
```
application.id=parse-elff-stream
bootstrap.servers=big-host-1.datadisorder.dev:9092
schema.registry.url=http://big-host-1.datadisorder.dev:8081

input.topic.name=elff-input

output.topic.name=elff-output

error.topic.name=elff-error

security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin";

confluent.metrics.reporter.bootstrap.servers=big-host-1.datadisorder.dev:9091
confluent.metrics.reporter.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";
confluent.metrics.reporter.sasl.mechanism=PLAIN
confluent.metrics.reporter.security.protocol=SASL_PLAINTEXT
```

With the above configuration, the KStreams application will connect to the Kafka Brokers identified by the `bootstrap.servers` cluster making use of the `security.protocol` and `sasl.` configuration. The KStreams Application will use a consumer group with the `application.id` and read its input from `input.topic.name` and write out the parsed events to `output.topic.name`. If any configured exceptions are caught with the `input.topic.name` deserialization or parsing, the event will not be written to `output.topic.name`, and will instead be written to `error.topic.name`. To horizontally scale the KStream, make sure the `input.topic.name` has multiple partitions and start another jvm with the same configuration properties file.

## Execution
Run the `parse-elff-stream-0.1-jar-with-dependencies.jar` with Java 8.

```
java -jar parse-elff-stream-0.1-jar-with-dependencies.jar lab.properties
```


## Test
Create input/output/error topics
```
$ kafka-topics --bootstrap-server big-host-1.datadisorder.dev:9092 --create --topic elff-input --replication-factor 1 --partitions 3 --command-config configuration/dev.properties

$ kafka-topics --bootstrap-server big-host-1.datadisorder.dev:9092 --create --topic elff-output --replication-factor 1 --partitions 3 --command-config configuration/dev.properties

$ kafka-topics --bootstrap-server big-host-1.datadisorder.dev:9092 --create --topic elff-error --replication-factor 1 --partitions 3 --command-config configuration/dev.properties
```

Assign ACLs if needed
```
TODO
```

Start the Kafka Stream (see Execution Configuration and Execution)

GZip sample ELFF messages
```
cd test/resources
for x in `ls *.elff`; do echo $x; gzip -k $x; done
```

Push the sample ELFF messages
```
kcat -F configuration/kafkacat.properties -b big-host-1.datadisorder.dev:9092 -P -t elff-input -k test-message-key test/resources/formatted.elff

kcat -F configuration/kafkacat.properties -b big-host-1.datadisorder.dev:9092 -P -t elff-input -k test-message-key test/resources/formatted.elff.gz
```

Validate the messages were parsed using the Confluent Control Center or with kcat

```
kcat -F configuration/kafkacat.properties -b  big-host-1.datadisorder.dev:9092 -C -t elff-output
```