package querying;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/* THIS CLASS IS NOT YET OPERATIONAL */
public class CotQuerying {
    public static final String DEFAULT_METRIC_ID = "airquality.no2::number";
    public static final String APP_NAME = "cot-aq-querying";
    public static final String KBROKERS = "10.10.139.32:9092";
    public static final int DEFAULT_GH_PRECISION = 6;

    public static void main(String[] args) {
        boolean cleanup = false;

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KBROKERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption( "cl", "cleanup", false, "Should a cleanup be performed before staring. Defaults to false" );

        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );
            if( line.hasOption( "cleanup" ) ) {
                cleanup = true;
            }
        }
        catch( Exception exp ) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("CotIngestStream", exp.getMessage(), options,null, true);
        }

        final StreamsBuilder builder = new StreamsBuilder();

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            if(cleanup) {
                streams.cleanUp();
            }
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }

        ReadOnlyKeyValueStore<String,String> keyValueStore =
                streams.store("raw-airquality.no2.number", QueryableStoreTypes.keyValueStore());

        final KeyValueIterator<String,String> range = keyValueStore.all();

        while(range.hasNext()){
            KeyValue<String,String> next = range.next();
            System.out.println(String.format("key: %s | value: %s", next.key,next.value));

        }

        System.exit(0);
    }
}
