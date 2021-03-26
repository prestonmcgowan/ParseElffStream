# ParseElffStream
Parse BlueCoat (ELFF) Data Streams
1. clone elff parser from https://github.com/jcustenborder/extended-log-format-parser
2. build the package mvn clean package dependency:copy-dependencies -DskipTests -Dresume=false -Dcheckstyle.skip -Dmaven.javadoc.skip=true

3. create a message in topic
kafkacat -b localhost:9092 -t elff.input -P configuration/sample.txt
Elffparser is just testing to see how the data will look after parsing 
