package com.worldmodelers.kafka.messages.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.worldmodelers.kafka.messages.ExampleConsumerMessage;
import com.worldmodelers.kafka.messages.ExampleConsumerMessageJsonFormat;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ExampleConsumerMessageSerializer extends ExampleConsumerMessageJsonFormat implements Serializer<ExampleConsumerMessage> {
    public void configure( Map<String, ?> configs, Boolean isKey ) { }

    public byte[] serialize( String topic, ExampleConsumerMessage data ) {
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