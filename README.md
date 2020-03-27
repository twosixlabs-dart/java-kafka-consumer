# Kafka Consumer Example using Java

To understand this project, read through `com.worldmodelers.kafka.processors.java.ExampleConsumer`. 
This class is where the stream processor is constructed; comments are provided to explain how it works.

For more information on using Kafka, you can find the slides from our original presentation here:
https://drive.google.com/open?id=1h1tC4oOEQfhwehvIxfY92VeMmkF5EUb_

## Configuration

See the `.properties` files in the resource directories to see what properties need to be passed to
kafka (the kafka-specific properties are prefixed by `kafka.*`, which prefix is stripped when they
are passed to the streams builder).

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
