package com.worldmodelers.kafka.messages.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.worldmodelers.kafka.messages.ExampleConsumerMessage;
import com.worldmodelers.kafka.messages.ExampleConsumerMessageJsonFormat;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ExampleConsumerMessageDeserializer extends ExampleConsumerMessageJsonFormat implements Deserializer<ExampleConsumerMessage> {
    public void configure( Map<String, ?> configs, Boolean isKey ) {}

    public ExampleConsumerMessage deserialize( String topic, byte[] data ) {
        ExampleConsumerMessage dataOut = null;
        try {
            dataOut = unmarshalMessage( new String( data, StandardCharsets.UTF_8 ) );
        } catch ( JsonProcessingException e ) {
            e.printStackTrace();
        }

        return dataOut;
    }

    public void close( ) {}
}
