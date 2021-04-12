# ParseElffStream
Parse BlueCoat (ELFF) Data Streams

## Test
Create input/output/error topics
```
$ kafka-topics --bootstrap-server big-host-2.datadisorder.dev:9093 --create --topic elff-input --replication-factor 1 --partitions 3 --command-config configuration/dev.properties

$ kafka-topics --bootstrap-server big-host-2.datadisorder.dev:9093 --create --topic elff-output --replication-factor 1 --partitions 3 --command-config configuration/dev.properties

$ kafka-topics --bootstrap-server big-host-2.datadisorder.dev:9093 --create --topic elff-error --replication-factor 1 --partitions 3 --command-config configuration/dev.properties
```

Assign ACLs if needed
```
TODO
```

Start the Kafka Stream
```

```

GZip sample ELFF messages
```
cd test/resources
for x in `ls *.elff`; do echo $x; gzip -k $x; done
```

Push the sample ELFF messages
```
kafkacat -F configuration/kafkacat.properties -b big-host-2.datadisorder.dev:9093 -P -t elff-input -k test-message-key test/resources/formatted.elff
```