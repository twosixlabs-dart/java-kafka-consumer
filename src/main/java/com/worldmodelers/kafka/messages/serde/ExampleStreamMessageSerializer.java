package com.worldmodelers.kafka.messages.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.worldmodelers.kafka.messages.ExampleStreamMessage;
import com.worldmodelers.kafka.messages.ExampleStreamMessageJsonFormat;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ExampleStreamMessageSerializer extends ExampleStreamMessageJsonFormat implements Serializer<ExampleStreamMessage> {
    public void configure( Map<String, ?> configs, Boolean isKey ) { }

    public byte[] serialize( String topic, ExampleStreamMessage data ) {
        byte[] dataOut = null;

        try {
            dataOut = marshalMessage( data ).getBytes();
        } catch ( JsonProcessingException e ) {
            e.printStackTrace();
        }

        return dataOut;
    }

    public void close() { }
}