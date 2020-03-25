package com.worldmodelers.kafka.processors.java;

import com.worldmodelers.kafka.messages.ExampleConsumerMessage;
import com.worldmodelers.kafka.messages.ExampleConsumerMessageJsonFormat;
import kafka.server.KafkaConfig$;
import net.mguenther.kafka.junit.*;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class ExampleConsumerTest extends ExampleConsumerMessageJsonFormat {

    private final Logger LOG = LoggerFactory.getLogger( ExampleConsumerTest.class );

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

        ArrayList<String> breadcrumbs = new ArrayList<String>();
        breadcrumbs.add( "java-kafka-streams" );

        new Thread( consumer::run ).start();
        Thread.sleep( 2000 );

        ExampleConsumerMessage message = new ExampleConsumerMessage( "id1", breadcrumbs );
        String messageJson = marshalMessage( message );

        List<KeyValue<String, String>> records = new ArrayList<>();

        records.add( new KeyValue<>( message.getId(), messageJson ) );

        SendKeyValues<String, String> sendRequest = SendKeyValues.to( topic, records ).useDefaults();

        cluster.send( sendRequest );

        Thread.sleep( 2000 );

        String content = new String ( Files.readAllBytes( Paths.get( persistDir + "/" + message.getId() + ".txt" ) ) );

        assertEquals( "id1\njava-kafka-streams, java-kafka-consumer", content );
    }
}