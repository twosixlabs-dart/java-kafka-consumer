package com.worldmodelers.kafka.consumer.java;

import kafka.server.KafkaConfig$;
import net.mguenther.kafka.junit.*;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class ExampleConsumerTest {

    private EmbeddedKafkaConfig kafkaConfig = EmbeddedKafkaConfig
            .create()
            .with( KafkaConfig$.MODULE$.PortProp(), 6308 )
            .build();

    private EmbeddedKafkaClusterConfig kafkaClusterConfig = EmbeddedKafkaClusterConfig
            .create()
            .provisionWith( kafkaConfig )
            .build();

    private Properties properties = new Properties();

    public ExampleConsumerTest() throws IOException {
        InputStream propStream = getClass().getClassLoader().getResourceAsStream( "test.properties" );
        properties.load( propStream );
    }

    @Rule
    public EmbeddedKafkaCluster cluster = EmbeddedKafkaCluster.provisionWith( kafkaClusterConfig );

    @Test
    public void ExampleConsumerShouldReadAMessageFromATopic() throws IOException, InterruptedException {
        String topic = properties.getProperty( "topic.from" );
        String persistDir = properties.getProperty( "consumer.persist.dir" );
        ExampleConsumer consumer = new ExampleConsumer( topic, persistDir, properties );

        new Thread( consumer::run ).start();
        Thread.sleep( 2000 );

        List<KeyValue<String, String>> records = new ArrayList<>();

        String id = UUID.randomUUID().toString();
        String message = "my test message";

        records.add( new KeyValue<>( id, message ) );

        SendKeyValues<String, String> sendRequest = SendKeyValues.to( topic, records ).useDefaults();

        cluster.send( sendRequest );

        Thread.sleep( 2000 );

        String content = new String( Files.readAllBytes( Paths.get( persistDir + "/" + id + ".txt" ) ) );

        assertEquals( content, message );
    }
}