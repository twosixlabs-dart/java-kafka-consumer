import com.worldmodelers.kafka.consumer.java.ExampleConsumer;
import scala.collection.immutable.Stream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {

    public static void main( String[] args ) throws IOException {

        Properties properties = new Properties();
        InputStream propsStream = Main.class.getClassLoader().getResourceAsStream( String.format( "%s.properties", args[ 0 ] ) );
        properties.load( propsStream );

        String topicFrom = properties.getProperty( "topic.from" );
        String persistDir = properties.getProperty( "consumer.persist.dir" );

        ExampleConsumer consumer = new ExampleConsumer( topicFrom, persistDir, properties );

        consumer.run();

    }

}
