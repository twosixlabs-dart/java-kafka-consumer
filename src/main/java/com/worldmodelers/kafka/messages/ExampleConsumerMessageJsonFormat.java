package com.worldmodelers.kafka.messages;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleConsumerMessageJsonFormat {
    private Logger LOG = LoggerFactory.getLogger( ExampleConsumerMessageJsonFormat.class );

    ObjectMapper mapper = new ObjectMapper();

    public ExampleConsumerMessage unmarshalMessage( String json ) throws JsonProcessingException {
        return mapper.readValue( json, ExampleConsumerMessage.class );
    }

    public String marshalMessage( ExampleConsumerMessage message ) throws JsonProcessingException {
        return mapper.writeValueAsString( message );
    }
}
