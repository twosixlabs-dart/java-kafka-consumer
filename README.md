# Kafka Consumer Example using Java

To understand this project, read through `com.worldmodelers.kafka.processors.java.ExampleConsumer`. 
This class is where the stream processor is constructed; comments are provided to explain how it works.

For more information on using Kafka, you can find documentation here:
https://kafka.apache.org/documentation/

## Configuration

See the `.properties` files in the resource directories to see what properties need to be passed to
kafka (the kafka-specific properties are prefixed by `kafka.*`, which prefix is stripped when they
are passed to the streams builder).

## SASL / SSL Configuration
See the `compose-secure.properties` which includes kafka configuration that enables 
SASL (Simple Authentication And Security layer) authentication with SSL encryption.  

For kafka client to be able to connect to kafka topic via SSL and successfully perform hostname verification 
the `kafka.bootstrap.servers` hostname has to match the hostname that is set in the `Common Name (CN)` field 
or `dnsName` in `subjectAltName` field of the SSL certificate presented by the kafka broker.  
Wildcard can be used in the CN / dnsName (*.example.com).  

In case when kafka broker responds with self signed certificate the truststore location and password can be defined
using `kafka` properties.
```
kafka.ssl.truststore.location=client.truststore.jks // full path to the trustore which includes the self signed certificate
kafka.ssl.truststore.password=trustStorePassword
``` 

## Running the Example

To run the example use the `java.yml` docker-compose files in the 
[kafka-examples-docker](https://github.com/twosixlabs-dart/kafka-examples-docker) repository in
this github account.

Copy that file to a local directory, and from that directory run:

```$xslt
docker-compose -f java.yml pull
docker-compose -f java.yml up -d
```

It is configured to print its output to a `data/java-kafka-consumer` directory
relative to the directory where you run the containers. Once the kafka servers are up and running, 
It should print a text file to that directory once every two seconds. To stop the process, run:

```$xslt
docker-compose -f java.yml down
```
