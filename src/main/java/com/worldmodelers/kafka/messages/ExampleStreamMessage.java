package com.worldmodelers.kafka.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;

public class ExampleStreamMessage {
    @JsonProperty( "id" )
    public String id;

    @JsonProperty( "breadcrumbs" )
    public ArrayList<String> breadcrumbs;

    @JsonCreator
    public ExampleStreamMessage( @JsonProperty( "id" ) String idIn, @JsonProperty( "breadcrumbs" ) ArrayList<String> breadcrumbsIn ) {
        id = idIn;
        breadcrumbs = breadcrumbsIn;
    }
}
