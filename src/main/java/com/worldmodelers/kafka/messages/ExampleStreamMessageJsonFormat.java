package com.worldmodelers.kafka.messages;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleStreamMessageJsonFormat {
    private Logger LOG = LoggerFactory.getLogger( ExampleStreamMessageJsonFormat.class );

    ObjectMapper mapper = new ObjectMapper();

    public ExampleStreamMessage unmarshalMessage( String json ) throws JsonProcessingException {
        return mapper.readValue( json, ExampleStreamMessage.class );
    }

    public String marshalMessage( ExampleStreamMessage message ) throws JsonProcessingException {
        return mapper.writeValueAsString( message );
    }
}
