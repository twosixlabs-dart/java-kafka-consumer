package com.worldmodelers.kafka.consumer.java;

import com.worldmodelers.kafka.messages.ExampleConsumerMessage;
import com.worldmodelers.kafka.messages.serde.ExampleStreamMessageSerde;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ExampleConsumer {

    private final Logger LOG = LoggerFactory.getLogger( ExampleConsumer.class );
    private Properties kafkaProps = new Properties();
    private String topic;
    private String persistDir;

    protected KafkaConsumer<String, ExampleConsumerMessage> consumer;

    public ExampleConsumer( String topicIn, String persistDirIn, Properties properties ) {
        properties.forEach( ( key, val ) -> {
            if ( key.toString().startsWith( "kafka" ) ) {
                kafkaProps.put( key.toString().substring( 6 ), val );
            }
        } );

        topic = topicIn;
        persistDir = persistDirIn;
        consumer = new KafkaConsumer< String, ExampleConsumerMessage>( kafkaProps );

        ArrayList<String> topics = new ArrayList<>(  );
        topics.add( topic );
        consumer.subscribe( topics );
    }

    // Serdes are objects that handle serializing and deserializing kafka messages.
    // One is needed to deserialize the key (simple string serde) and another to
    // deserialize the message itself (in this case the custom ExampleStreamMessageSerde
    // defined in the messages package
    private Serde<String> stringSerdes = Serdes.String();
    private Serde<ExampleConsumerMessage> streamMessageSerdes = new ExampleStreamMessageSerde();

    // This is the business logic of the consumer: it acts on the consumed message (in
    // this case it just adds itself to the breadcrumbs and writes it to the filesystem)
    private void persist( ExampleConsumerMessage message ) throws IOException {
        message.getBreadcrumbs().add( "java-kafka-consumer" );
        String fileName = persistDir + "/" + message.getId() + ".txt";

        File file = new File( fileName );
        file.createNewFile();

        FileWriter fw = new FileWriter( fileName );
        fw.write( message.getId() );
        fw.append( "\n" + String.join(", ", message.getBreadcrumbs()) );
        fw.close();
    }

    // The only public method apart from the constructor: starts the consumer, and
    // ensures the consumer closes if it is forced to stop for any reason
    public void run( ) {
        LOG.info( "starting kafka consumer" );
        Integer timeout = Integer.parseInt( kafkaProps.getProperty( "poll.timeout.millis" ) );

        try {
            while ( true ) {
                ConsumerRecords<String, ExampleConsumerMessage> records = consumer.poll( Duration.ofMillis( timeout ) );
                records.forEach( record -> {
                    try {
                        persist( record.value() );
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

    private void stop( ) {
        consumer.close();
    }

}
