package com.worldmodelers.kafka.consumer;

import com.worldmodelers.kafka.messages.ExampleStreamMessage;
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
import java.util.Properties;
import java.util.function.Consumer;

public class ExampleConsumer {

    private final Logger LOG = LoggerFactory.getLogger( ExampleConsumer.class );
    private Properties kafkaProps = new Properties();
    private String topic;
    private String persistDir;

    protected KafkaConsumer<String, ExampleStreamMessage> consumer;

    public ExampleConsumer( String topicIn, String persistDirIn, Properties properties ) {
        properties.forEach( ( key, val ) -> {
            if ( key.toString().startsWith( "kafka" ) ) {
                kafkaProps.put( key.toString().substring( 6 ), val );
            }
        } );

        topic = topicIn;
        persistDir = persistDirIn;
        consumer = new KafkaConsumer< String, ExampleStreamMessage >( kafkaProps );

        ArrayList<String> topics = new ArrayList<>(  );
        topics.add( topic );
        consumer.subscribe( topics );
    }

    // Serdes are objects that handle serializing and deserializing kafka messages
    // one is needed to deserialize the key (simple string serde) and another to
    // deserialize the message itself (in this case the custom ExampleStreamMessageSerde
    // defined in the messages package
    private Serde<String> stringSerdes = Serdes.String();
    private Serde<ExampleStreamMessage> streamMessageSerdes = new ExampleStreamMessageSerde();

    // This is the business logic of the consumer: it acts on the consumed message (in
    // this case it just adds itself to the breadcrumbs and writes it to the filesystem)
    private void persist( ExampleStreamMessage message ) throws IOException {
        ArrayList<String> breadcrumbs = message.breadcrumbs;
        breadcrumbs.add( "java-kafka-consumer" );
        String id = message.id;
        String fileName = persistDir + "/" + message.id + ".txt";

        LOG.info( "Persisting " + fileName );

        File file = new File( fileName );
        file.createNewFile();

        FileWriter fw = new FileWriter( fileName );
        fw.write( id );
        fw.append( "\n" + String.join(", ", breadcrumbs) );
        fw.close();
    }

    // The only public method apart from the constructor: starts the consumer, and
    // ensures the consumer closes if it is forced to stop for any reason
    public void run( ) {
        LOG.info( "starting kafka consumer" );
        Integer timeout = Integer.parseInt( kafkaProps.getProperty( "poll.timeout.millis" ) );

        try {
            while ( true ) {
                ConsumerRecords<String, ExampleStreamMessage> records = consumer.poll( Duration.ofMillis( timeout ) );
                records.forEach( record -> {
                    try {
                        persist( record.value() );
                    } catch ( IOException e ) {
                        LOG.error( "FAILED TO PERSIST RECORD" );
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
