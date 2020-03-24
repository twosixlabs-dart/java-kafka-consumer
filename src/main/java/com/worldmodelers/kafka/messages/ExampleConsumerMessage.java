package com.worldmodelers.kafka.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Objects;

public class ExampleConsumerMessage {
    @JsonProperty( "id" )
    private String id;

    @JsonProperty( "breadcrumbs" )
    private ArrayList<String> breadcrumbs = new ArrayList<>();

    @JsonCreator
    public ExampleConsumerMessage( @JsonProperty( "id" ) String idIn, @JsonProperty( "breadcrumbs" ) ArrayList<String> breadcrumbsIn ) {
        id = idIn;
        breadcrumbs = breadcrumbsIn;
    }

    public void setId( String idIn ) { id = idIn; }

    public String getId() { return id; }

    public void setBreadcrumbs( ArrayList<String> breadcrumbsIn ) { breadcrumbs = breadcrumbsIn; }

    public ArrayList<String> getBreadcrumbs() { return breadcrumbs; }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        ExampleConsumerMessage message = (ExampleConsumerMessage) o;
        return Objects.equals( id, message.id ) &&
                Objects.equals( breadcrumbs, message.breadcrumbs );
    }

    @Override
    public int hashCode() {
        return Objects.hash( id, breadcrumbs );
    }
}
