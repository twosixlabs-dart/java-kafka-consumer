package com.worldmodelers.kafka.messages.serde;

import com.worldmodelers.kafka.messages.ExampleConsumerMessage;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;

public class ExampleStreamMessageSerde extends WrapperSerde<ExampleConsumerMessage> {

    public ExampleStreamMessageSerde() {
        super( new ExampleConsumerMessageSerializer(), new ExampleConsumerMessageDeserializer() );
    }
}

