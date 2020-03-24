package com.worldmodelers.kafka.messages.serde;

import com.worldmodelers.kafka.messages.ExampleStreamMessage;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;

public class ExampleStreamMessageSerde extends WrapperSerde<ExampleStreamMessage> {

    public ExampleStreamMessageSerde() {
        super( new ExampleStreamMessageSerializer(), new ExampleStreamMessageDeserializer() );
    }
}

