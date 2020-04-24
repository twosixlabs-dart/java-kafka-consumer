package com.worldmodelers.kafka.consumer.java;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class ExampleConsumer {

    private final Logger LOG = LoggerFactory.getLogger( ExampleConsumer.class );
    private Properties kafkaProps = new Properties();
    private String topic;
    private String persistDir;

    protected KafkaConsumer<String, String> consumer;

    public ExampleConsumer( String topicIn, String persistDirIn, Properties properties ) {
        properties.forEach( ( key, val ) -> {
            if ( key.toString().startsWith( "kafka" ) ) {
                kafkaProps.put( key.toString().substring( 6 ), val );
            }
        } );

        topic = topicIn;
        persistDir = persistDirIn;
        consumer = new KafkaConsumer<>( kafkaProps );

        ArrayList<String> topics = new ArrayList<>();
        topics.add( topic );
        consumer.subscribe( topics );
    }

    // This is the business logic of the consumer: it acts on the consumed message (in
    // this case it just adds itself to the breadcrumbs and writes it to the filesystem)
    private void persist( String key, String value ) throws IOException {
        String fileName = persistDir + "/" + key + ".txt";

        FileWriter writer = new FileWriter( new File( fileName ) );
        writer.write( value );
        writer.close();
    }

    // The only public method apart from the constructor: starts the consumer, and
    // ensures the consumer closes if it is forced to stop for any reason
    public void run() {
        LOG.info( "starting kafka consumer" );
        Integer timeout = Integer.parseInt( kafkaProps.getProperty( "poll.timeout.millis" ) );

        try {
            while ( true ) {
                ConsumerRecords<String, String> records = consumer.poll( Duration.ofMillis( timeout ) );
                records.forEach( record -> {
                    try {
                        persist( record.key(), record.value() );
                    } catch ( IOException e ) {
                        LOG.error( e.getMessage() );
                        e.printStackTrace();
                    }
                } );
            }
        } finally {
            stop();
        }
    }

    private void stop() {
        consumer.close();
    }

}
