package com.worldmodelers.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ExampleConsumer {

    private final Logger LOG = LoggerFactory.getLogger( ExampleConsumer.class );
    private Properties kafkaProps = new Properties(  );

    public ExampleConsumer( String topicIn, String persistDirIn, Properties properties ) {
        properties.forEach( ( key, val ) -> {
            if ( key.toString().startsWith( "kafka" ) ) {
                kafkaProps.put( key.toString().substring( 6 ), val );
            }
        } );

        LOG.info("FIXED PROPERTIES!:");
        kafkaProps.forEach( (k,v) -> {
            LOG.info(k.toString());
        } );
    }
}
