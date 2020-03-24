package com.worldmodelers.kafka.messages.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.worldmodelers.kafka.messages.ExampleStreamMessage;
import com.worldmodelers.kafka.messages.ExampleStreamMessageJsonFormat;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ExampleStreamMessageDeserializer extends ExampleStreamMessageJsonFormat implements Deserializer<ExampleStreamMessage> {
    public void configure( Map<String, ?> configs, Boolean isKey ) {}

    public ExampleStreamMessage deserialize( String topic, byte[] data ) {
        ExampleStreamMessage dataOut = null;
        try {
            dataOut = unmarshalMessage( new String( data, StandardCharsets.UTF_8 ) );
        } catch ( JsonProcessingException e ) {
            e.printStackTrace();
        }

        return dataOut;
    }

    public void close( ) {}
}
