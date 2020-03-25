package com.worldmodelers.kafka.processors.java;

import scala.collection.immutable.Stream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ConsumerApp {

    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();
        InputStream propsStream = Stream.Cons.class.getClassLoader().getResourceAsStream( "app.properties" );
        properties.load( propsStream );

        String topicFrom = properties.getProperty( "topic.from" );
        String persistDir = properties.getProperty( "consumer.persist.dir" );

        ExampleConsumer consumer = new ExampleConsumer( topicFrom, persistDir, properties );

        consumer.run();

    }

}
