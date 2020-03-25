package com.worldmodelers.kafka.processors.java;

import scala.collection.immutable.Stream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ConsumerApp {

    public static void main(String[] args) throws IOException {

        String configName;

        if (args.length > 0) configName = args[0];
        else configName = "default";

        String configResource = configName.trim() + ".properties";

        Properties properties = new Properties();
        InputStream propsStream = Stream.Cons.class.getClassLoader().getResourceAsStream( configResource );
        properties.load( propsStream );

        String topicFrom = properties.getProperty( "topic.from" );
        String persistDir = properties.getProperty( "consumer.persist.dir" );

        ExampleConsumer consumer = new ExampleConsumer( topicFrom, persistDir, properties );

        consumer.run();

    }

}
